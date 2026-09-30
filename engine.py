"""Deterministic paper ledger. No credentials or live trading routes."""
import contextlib
import hashlib
import json
import math
import os
import sqlite3
import threading
import time
from pathlib import Path

DB = os.getenv('DB_PATH', '/data/hermes_pm_v2.sqlite3' if Path('/data').exists() else 'hermes_pm_v2.sqlite3')
LOCK = threading.RLock()
START = float(os.getenv('START_CASH', '1000'))
POLICIES = ('reference', 'filtered', 'adaptive', 'ranked')
LIMITS = dict(trade=25., wallet=200., market=75., event=125., total=600., positions=20,
              spread=.06, relative=.05, absolute=.02, max_age=180, daily_loss=.05)
EPS = 1e-8

@contextlib.contextmanager
def database():
    conn = sqlite3.connect(DB, timeout=25)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA busy_timeout=25000')
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def note(db, action, detail, now=None):
    db.execute('INSERT INTO pm_audit(ts,action,detail) VALUES(?,?,?)', (now or int(time.time()), action, detail[:2000]))


def init():
    Path(DB).parent.mkdir(parents=True, exist_ok=True)
    # SQLite backup before the first migration; original V1 tables are retained.
    if Path(DB).exists():
        with database() as db:
            present = db.execute("SELECT 1 FROM sqlite_master WHERE name='pm_portfolios'").fetchone()
            if not present:
                with sqlite3.connect(DB+'.pre-v2.bak') as target: db.backup(target)
    with database() as db:
        db.execute('PRAGMA journal_mode=WAL')
        db.executescript('''
        CREATE TABLE IF NOT EXISTS pm_meta(k TEXT PRIMARY KEY,v TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS pm_portfolios(id TEXT PRIMARY KEY,initial REAL,cash REAL,realized REAL DEFAULT 0,fees REAL DEFAULT 0,peak REAL,drawdown REAL DEFAULT 0,created INTEGER);
        CREATE TABLE IF NOT EXISTS pm_wallets(address TEXT PRIMARY KEY,label TEXT,enabled INTEGER DEFAULT 1,auto INTEGER DEFAULT 0,origin TEXT DEFAULT 'manual',added INTEGER,cursor INTEGER DEFAULT 0,ready INTEGER DEFAULT 0,error TEXT DEFAULT '',last_poll INTEGER DEFAULT 0,last_reconcile INTEGER DEFAULT 0,buy_after INTEGER DEFAULT 0,blocked INTEGER DEFAULT 0,weight REAL DEFAULT 1,weight_review INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS pm_source(address TEXT,asset TEXT,qty REAL,PRIMARY KEY(address,asset));
        CREATE TABLE IF NOT EXISTS pm_positions(portfolio TEXT,address TEXT,asset TEXT,market TEXT,event TEXT,title TEXT,outcome TEXT,qty REAL,cost REAL,PRIMARY KEY(portfolio,address,asset));
        CREATE TABLE IF NOT EXISTS pm_signals(id TEXT PRIMARY KEY,address TEXT,asset TEXT,market TEXT,event TEXT,title TEXT,outcome TEXT,side TEXT,qty REAL,price REAL,ts INTEGER,detected INTEGER,source_before REAL,reason TEXT);
        CREATE TABLE IF NOT EXISTS pm_orders(id TEXT PRIMARY KEY,signal TEXT,portfolio TEXT,address TEXT,asset TEXT,market TEXT,event TEXT,title TEXT,outcome TEXT,side TEXT,requested REAL,remaining REAL,source_price REAL,source_ts INTEGER,created INTEGER,expires INTEGER,state TEXT,reason TEXT,manual INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS pm_fills(id INTEGER PRIMARY KEY AUTOINCREMENT,order_id TEXT,portfolio TEXT,address TEXT,asset TEXT,market TEXT,event TEXT,title TEXT,outcome TEXT,side TEXT,qty REAL,price REAL,fee REAL,cash_delta REAL,realized REAL,ts INTEGER,source_ts INTEGER,quote_ts INTEGER,source_price REAL,model TEXT);
        CREATE TABLE IF NOT EXISTS pm_quotes(asset TEXT PRIMARY KEY,ts INTEGER,payload TEXT,error TEXT);
        CREATE TABLE IF NOT EXISTS pm_equity(ts INTEGER,portfolio TEXT,cash REAL,equity REAL,realized REAL,unrealized REAL,complete INTEGER,PRIMARY KEY(ts,portfolio));
        CREATE TABLE IF NOT EXISTS pm_candidates(address TEXT PRIMARY KEY,label TEXT,pnl REAL,volume REAL,score REAL,sample_count INTEGER,span_days REAL,concentration REAL,eligible INTEGER,reason TEXT,updated INTEGER);
        CREATE TABLE IF NOT EXISTS pm_audit(id INTEGER PRIMARY KEY AUTOINCREMENT,ts INTEGER,action TEXT,detail TEXT);
        CREATE INDEX IF NOT EXISTS pm_orders_pending ON pm_orders(state,created);
        CREATE INDEX IF NOT EXISTS pm_fills_wallet ON pm_fills(address,portfolio,ts);
        CREATE INDEX IF NOT EXISTS pm_signals_time ON pm_signals(detected);
        ''')
        db.execute('CREATE TABLE IF NOT EXISTS pm_runs(version TEXT PRIMARY KEY,started INTEGER,config TEXT)')
        db.execute('INSERT OR IGNORE INTO pm_runs VALUES(?,?,?)',('3.1.0',int(time.time()),json.dumps(dict(limits=LIMITS,poll_seconds=max(5,int(os.getenv('POLL_SECONDS','5'))),wallet_fraction=.10),sort_keys=True)))
        if not db.execute("SELECT 1 FROM pm_meta WHERE k='version'").fetchone():
            now = int(time.time())
            for p in POLICIES:
                db.execute('INSERT INTO pm_portfolios(id,initial,cash,peak,created) VALUES(?,?,?,?,?)',(p,START,START,START,now))
            legacy = db.execute("SELECT 1 FROM sqlite_master WHERE name='wallets'").fetchone()
            if legacy:
                for w in db.execute('SELECT * FROM wallets').fetchall():
                    db.execute('INSERT OR IGNORE INTO pm_wallets(address,label,enabled,auto,added,buy_after) VALUES(?,?,?,?,?,?)', (w['address'],w['label'],w['enabled'],w['auto'],now,now))
                # Historical V1 cash/holdings live only in filtered; benchmarks start fresh.
                old_fills = db.execute('SELECT * FROM fills ORDER BY id').fetchall()
                for f in old_fills:
                    q=float(f['shares']); cost=-float(f['cash_delta'])
                    db.execute('INSERT INTO pm_positions VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(portfolio,address,asset) DO UPDATE SET qty=qty+excluded.qty,cost=cost+excluded.cost',('filtered',f['address'],f['asset'],f['asset'],f['asset'],f['title'],f['outcome'],q,cost))
                old_cash=db.execute("SELECT value FROM settings WHERE key='cash'").fetchone()
                if old_cash:
                    cash=float(old_cash[0]); initial=cash+sum(-float(f['cash_delta']) for f in old_fills)
                    db.execute("UPDATE pm_portfolios SET cash=?,initial=?,peak=? WHERE id='filtered'",(cash,initial,initial))
                if old_fills: note(db,'migration','V1 positions imported into filtered; benchmark starting dates differ')
            db.execute("INSERT INTO pm_meta VALUES('version','2.0.0')")
            db.execute("INSERT INTO pm_meta VALUES('automation','1')")
            note(db,'migration','V2 initialized. Legacy tables retained; forward-only paper ledgers.')

        # Add the new ledger once, preserving all existing balances and history.
        now=int(time.time())
        db.execute('INSERT OR IGNORE INTO pm_portfolios(id,initial,cash,peak,created) VALUES(?,?,?,?,?)',('ranked',START,START,START,now))


def ranked_wallets(db,now):
    return [dict(r) for r in db.execute("""SELECT w.address,w.label,c.score,c.updated FROM pm_wallets w
        JOIN pm_candidates c ON c.address=w.address
        WHERE w.enabled=1 AND w.auto=1 AND w.ready=1 AND w.blocked=0 AND w.error=''
        AND c.sample_count>0 AND c.updated>=?
        ORDER BY c.score DESC,w.address ASC LIMIT 3""",(now-7*86400,))]


def add_wallet(db,address,label,auto=False,origin='manual',now=None):
    now=now or int(time.time())
    db.execute('INSERT INTO pm_wallets(address,label,enabled,auto,origin,added,buy_after) VALUES(?,?,1,?,?,?,?) ON CONFLICT(address) DO UPDATE SET label=excluded.label,enabled=1', (address,label[:60],int(auto),origin,now,now))
    note(db,'wallet_added',json.dumps({'address':address,'origin':origin,'paper':auto}),now)


def position(db,p,address,asset):
    r=db.execute('SELECT * FROM pm_positions WHERE portfolio=? AND address=? AND asset=?',(p,address,asset)).fetchone()
    return dict(r) if r else {'qty':0.,'cost':0.}


def source_qty(db,address,asset):
    r=db.execute('SELECT qty FROM pm_source WHERE address=? AND asset=?',(address,asset)).fetchone()
    return float(r[0]) if r else 0.


def set_source(db,address,asset,qty):
    db.execute('INSERT INTO pm_source VALUES(?,?,?) ON CONFLICT(address,asset) DO UPDATE SET qty=excluded.qty',(address,asset,max(0.,qty)))


def signal(db,w,t,now):
    """Register once. Source inventory changes once, across all three portfolios."""
    address=w['address']; asset=str(t.get('asset','')); side=str(t.get('side','')).upper()
    qty=float(t.get('size') or 0); price=float(t.get('price') or 0); ts=int(t.get('timestamp') or 0)
    if not (asset.isdigit() and side in ('BUY','SELL') and math.isfinite(qty) and qty>0 and 0<price<1 and 0<ts<=now+5): return
    sid=hashlib.sha256(json.dumps([address,asset,side,qty,price,ts,t.get('transactionHash')],separators=(',',':')).encode()).hexdigest()
    if db.execute('SELECT 1 FROM pm_signals WHERE id=?',(sid,)).fetchone(): return
    before=source_qty(db,address,asset)
    market=str(t.get('conditionId') or asset); event=str(t.get('eventSlug') or market)
    title=str(t.get('title',''))[:240]; outcome=str(t.get('outcome',''))[:80]
    reason=t.get('_complex_reason','')
    if w.get('strategy_blocked'):reason='inventario alterado por conversión/split/merge; requiere nueva línea base'
    if not w['ready'] or ts<=w['buy_after']: reason='antes del inicio de seguimiento; solo observación'
    db.execute('INSERT INTO pm_signals VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(sid,address,asset,market,event,title,outcome,side,qty,price,ts,now,before,reason))
    # Initial baseline is already a snapshot of all prior positions.
    if ts<=w['buy_after'] or not w['ready']: return
    set_source(db,address,asset,before+qty if side=='BUY' else before-qty)
    if reason:return
    for p in POLICIES:
        own=position(db,p,address,asset)
        if side=='BUY':
            if not w['auto'] or not w['enabled'] or w['blocked']: continue
            if now-ts>LIMITS['max_age']: continue
            if p=='ranked':
                started=db.execute("SELECT created FROM pm_portfolios WHERE id='ranked'").fetchone()[0]
                if ts<=started or address not in {x['address'] for x in ranked_wallets(db,now)}:continue
            # Scale shares, not arbitrary equal dollar bets. Each wallet uses a 1% base ratio.
            scale=.01*(w['weight'] if p=='adaptive' else 1.)
            target=min(qty*scale,LIMITS['trade']/price)
            if p in ('filtered','adaptive'):
                probe=dict(portfolio=p,address=address,market=market,event=event)
                if room(db,probe)<1:
                    continue  # Signal retained; avoid redundant unfillable orders.
        else:
            if own['qty']<=EPS: continue
            if before+EPS<qty or before<=EPS:
                db.execute('UPDATE pm_wallets SET blocked=1,error=? WHERE address=?',('inventario de origen insuficiente; revisar reconciliación',address))
                w['blocked']=1
                note(db,'inventory_gap',address+' '+asset,now)
                continue
            # Reserve outstanding sell quantities so consecutive sales cannot double-sell.
            reserved=db.execute("SELECT COALESCE(SUM(remaining),0) FROM pm_orders WHERE portfolio=? AND address=? AND asset=? AND side='SELL' AND state='pending'",(p,address,asset)).fetchone()[0]
            target=max(0,own['qty']-reserved)*min(1,qty/before)
        if target<=EPS: continue
        oid=sid+':'+p
        db.execute('INSERT INTO pm_orders VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(oid,sid,p,address,asset,market,event,title,outcome,side,target,target,price,ts,now,ts+LIMITS['max_age'] if side=='BUY' else 0,'pending','',0))


def room(db,o):
    """Cost based cumulative risk ceilings. All outcomes of one event are grouped."""
    p=o['portfolio']; totals={}
    for key,col,val in [('wallet','address',o['address']),('market','market',o['market']),('event','event',o['event'])]:
        totals[key]=db.execute('SELECT COALESCE(SUM(cost),0) FROM pm_positions WHERE portfolio=? AND '+col+'=? AND qty>?',(p,val,EPS)).fetchone()[0]
    totals['total']=db.execute('SELECT COALESCE(SUM(cost),0) FROM pm_positions WHERE portfolio=? AND qty>?',(p,EPS)).fetchone()[0]
    cash=db.execute('SELECT cash FROM pm_portfolios WHERE id=?',(p,)).fetchone()[0]
    caps=dict(LIMITS)
    if p in ('filtered','adaptive'):
        initial=db.execute('SELECT initial FROM pm_portfolios WHERE id=?',(p,)).fetchone()[0]
        caps['wallet']=min(caps['wallet'],initial*.10)
    return max(0.,min([cash]+[caps[k]-v for k,v in totals.items()]))


def consume(levels,side,wanted,budget,rate,exponent,limit_price):
    """Walk order-book levels; budget includes fee. Return exact marginal fees."""
    qty=notional=fee=0.
    for price,depth in sorted(levels,key=lambda l:l[0],reverse=side=='SELL'):
        if not (0<price<1 and depth>0 and math.isfinite(depth)): continue
        if side=='BUY' and price>limit_price+EPS: break
        if side=='SELL' and price<limit_price-EPS: break
        unit_fee=rate*(price*(1-price))**exponent
        take=min(wanted-qty,depth)
        if side=='BUY': take=min(take,max(0,budget-notional-fee)/(price+unit_fee))
        if take<=EPS: break
        qty+=take; notional+=take*price; fee+=take*unit_fee
        if qty>=wanted-EPS: break
    return qty,notional,fee


def apply_fill(db,o,qty,notional,fee,now,quote_ts,model):
    if qty<=EPS: return
    p=o['portfolio']; own=position(db,p,o['address'],o['asset']); side=o['side']; price=notional/qty
    if side=='BUY':
        delta=-notional-fee; realized=0.; new_qty=own['qty']+qty; new_cost=own['cost']+notional+fee
    else:
        if qty>own['qty']+EPS: raise ValueError('oversell prevented')
        released=own['cost']*qty/own['qty']
        delta=notional-fee; realized=delta-released
        new_qty=max(0.,own['qty']-qty); new_cost=max(0.,own['cost']-released) if new_qty>EPS else 0.
    db.execute('INSERT INTO pm_positions VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(portfolio,address,asset) DO UPDATE SET qty=excluded.qty,cost=excluded.cost', (p,o['address'],o['asset'],o['market'],o['event'],o['title'],o['outcome'],new_qty,new_cost))
    db.execute('UPDATE pm_portfolios SET cash=cash+?,realized=realized+?,fees=fees+? WHERE id=?',(delta,realized,fee,p))
    db.execute('INSERT INTO pm_fills(order_id,portfolio,address,asset,market,event,title,outcome,side,qty,price,fee,cash_delta,realized,ts,source_ts,quote_ts,source_price,model) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(o['id'],p,o['address'],o['asset'],o['market'],o['event'],o['title'],o['outcome'],side,qty,price,fee,delta,realized,now,o['source_ts'],quote_ts,o['source_price'],model))
    remaining=max(0.,o['remaining']-qty)
    db.execute('UPDATE pm_orders SET remaining=?,state=?,reason=? WHERE id=?',(remaining,'filled' if remaining<EPS else 'pending','completa' if remaining<EPS else 'ejecución parcial; saldo pendiente',o['id']))


def execute(db,o,quote,now):
    """A quote is consumed by pending orders within each independent portfolio."""
    if o['side']=='BUY' and now>o['expires']:
        db.execute("UPDATE pm_orders SET state='expired',reason='señal vencida' WHERE id=?",(o['id'],)); return
    if not quote or now-quote['ts']>20:
        db.execute("UPDATE pm_orders SET reason='sin cotización reciente' WHERE id=?",(o['id'],)); return
    w=db.execute('SELECT * FROM pm_wallets WHERE address=?',(o['address'],)).fetchone()
    if o['side']=='BUY' and (not w or not w['auto'] or not w['enabled'] or w['blocked']):
        db.execute("UPDATE pm_orders SET state='cancelled',reason='entradas pausadas' WHERE id=?",(o['id'],)); return
    if o['side']=='BUY' and o['portfolio']=='ranked' and o['address'] not in {x['address'] for x in ranked_wallets(db,now)}:
        db.execute("UPDATE pm_orders SET state='cancelled',reason='wallet fuera del Top 3 vigente' WHERE id=?",(o['id'],));return
    if quote.get('settlement') is not None: return
    if quote.get('fee_rate') is None:
        db.execute("UPDATE pm_orders SET reason='comisión no verificada; sin ejecución' WHERE id=?",(o['id'],)); return
    if o['side']=='BUY' and w['error']:
        db.execute("UPDATE pm_orders SET reason='lectura de wallet incompleta; esperando validación' WHERE id=?",(o['id'],));return
    asks=quote.get('asks',[]); bids=quote.get('bids',[])
    side=o['side']; filtered=o['portfolio'] in ('filtered','adaptive')
    if side=='BUY':
        if not asks or not bids: return
        if quote.get('complex'):
            db.execute("UPDATE pm_orders SET state='rejected',reason=? WHERE id=?",(quote.get('risk_reason') or 'estructura compleja no soportada',o['id'])); return
        spread=min(p for p,q in asks)-max(p for p,q in bids)
        if filtered and spread>LIMITS['spread']:
            db.execute("UPDATE pm_orders SET reason='spread excesivo' WHERE id=?",(o['id'],)); return
        # Pause new exposure when any current position cannot be valued.
        if o['portfolio'] in ('filtered','adaptive') and not next(p for p in portfolios(db,now) if p['id']==o['portfolio'])['complete']:
            db.execute("UPDATE pm_orders SET reason='valoración incompleta; entradas pausadas' WHERE id=?",(o['id'],));return
        # Portfolio-level daily stop pauses entries, never exits.
        day=now-now%86400
        first=db.execute('SELECT equity FROM pm_equity WHERE portfolio=? AND ts>=? AND complete=1 ORDER BY ts LIMIT 1',(o['portfolio'],day)).fetchone()
        latest=db.execute('SELECT equity FROM pm_equity WHERE portfolio=? AND complete=1 ORDER BY ts DESC LIMIT 1',(o['portfolio'],)).fetchone()
        if first and latest and latest[0]<first[0]*(1-LIMITS['daily_loss']):
            db.execute("UPDATE pm_orders SET state='rejected',reason='límite de pérdida diaria' WHERE id=?",(o['id'],)); return
        count=db.execute('SELECT COUNT(*) FROM pm_positions WHERE portfolio=? AND qty>?',(o['portfolio'],EPS)).fetchone()[0]
        if count>=LIMITS['positions'] and position(db,o['portfolio'],o['address'],o['asset'])['qty']<=EPS: return
        spent=db.execute('SELECT COALESCE(SUM(-cash_delta),0) FROM pm_fills WHERE order_id=?',(o['id'],)).fetchone()[0]
        budget=min(room(db,o),max(0.,LIMITS['trade']-spent))
        if budget<.01:
            db.execute("UPDATE pm_orders SET state='rejected',reason='límite acumulado de exposición / efectivo' WHERE id=?",(o['id'],)); return
        cap=min(.9999,o['source_price']+min(LIMITS['absolute'],o['source_price']*LIMITS['relative'])) if filtered else .9999
        wanted=o['remaining']
    else:
        budget=float('inf'); cap=0.000001; wanted=min(o['remaining'],position(db,o['portfolio'],o['address'],o['asset'])['qty'])
        if wanted<=EPS:
            db.execute("UPDATE pm_orders SET state='cancelled',reason='posición ya cerrada' WHERE id=?",(o['id'],));return
    if wanted+EPS<quote.get('min_order_size',0):
        db.execute("UPDATE pm_orders SET reason='cantidad inferior al mínimo de mercado' WHERE id=?",(o['id'],));return
    levels=asks if side=='BUY' else bids
    qty,notional,fee=consume(levels,side,wanted,budget,quote['fee_rate'],quote.get('fee_exponent',1),cap)
    if side=='BUY' and notional<1:
        db.execute("UPDATE pm_orders SET reason='compra inferior a 1 USDC' WHERE id=?",(o['id'],));return
    if qty<=EPS:
        db.execute("UPDATE pm_orders SET reason='sin profundidad dentro del precio permitido' WHERE id=?",(o['id'],)); return
    apply_fill(db,o,qty,notional,fee,now,quote['ts'],'HermesPM.V2/3.0.0; depth; fee cash-equivalent; no queue simulation')
    # Subtract simulated liquidity consumed by earlier orders in this portfolio/cycle.
    left=qty
    for level in sorted(levels,key=lambda l:l[0],reverse=side=='SELL'):
        if left<=EPS: break
        used=min(left,level[1]);level[1]-=used;left-=used


def close_position(db,p,address,asset,now):
    own=db.execute('SELECT * FROM pm_positions WHERE portfolio=? AND address=? AND asset=? AND qty>?',(p,address,asset,EPS)).fetchone()
    if not own: raise ValueError('posición inexistente')
    db.execute("UPDATE pm_orders SET state='cancelled',reason='sustituida por cierre manual' WHERE portfolio=? AND address=? AND asset=? AND state='pending'",(p,address,asset))
    oid='manual:'+str(time.time_ns())
    db.execute('INSERT INTO pm_orders VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(oid,oid,p,address,asset,own['market'],own['event'],own['title'],own['outcome'],'SELL',own['qty'],own['qty'],0.,now,now,0,'pending','cierre manual solicitado',1))
    note(db,'manual_exit',oid,now)


def settle(db,asset,payout,now):
    if payout not in (0.,.5,1.): return
    for row in db.execute('SELECT * FROM pm_positions WHERE asset=? AND qty>?',(asset,EPS)).fetchall():
        o=dict(row);o.update(id='settle:'+row['portfolio']+':'+row['address']+':'+asset,side='SELL',remaining=row['qty'],source_ts=now,source_price=payout)
        apply_fill(db,o,row['qty'],row['qty']*payout,0.,now,now,'official resolved market payout')
    db.execute("UPDATE pm_orders SET state='cancelled',reason='mercado liquidado' WHERE asset=? AND state='pending'",(asset,))


def portfolios(db,now,snapshot=False):
    out=[]
    for r in db.execute('SELECT * FROM pm_portfolios').fetchall():
        p=dict(r); positions=[]; value=cost=0.;complete=True; remaining_books={};issues={}
        for row in db.execute('SELECT * FROM pm_positions WHERE portfolio=? AND qty>?',(p['id'],EPS)).fetchall():
            item=dict(row);q=db.execute('SELECT * FROM pm_quotes WHERE asset=?',(item['asset'],)).fetchone()
            quote=json.loads(q['payload']) if q else {}; fresh=bool(q and now-q['ts']<=120 and not q['error'])
            bids=remaining_books.setdefault(item['asset'],[list(l) for l in quote.get('bids',[])]); mark=max((l[0] for l in bids),default=None) if fresh else None
            fee_rate=quote.get('fee_rate'); available=sum(l[1] for l in bids)
            # Valuation requires enough visible depth for this position; stale/missing -> unknown.
            qty,n,f=consume(bids,'SELL',item['qty'],float('inf'),fee_rate or 0,quote.get('fee_exponent',1),0)
            good=fresh and fee_rate is not None and qty>=item['qty']-EPS
            if good:reason=None
            elif q and q['error'] and 'stale orderbook timestamp' in q['error']:reason='Libro de órdenes sin actualización'
            elif q and q['error']:reason='Fallo al actualizar la cotización'
            elif not q:reason='Sin cotización registrada'
            elif not fresh:reason='Cotización antigua'
            elif quote.get('market_closed'):reason='Mercado cerrado; resolución pendiente'
            elif fee_rate is None:reason='Comisión sin verificar'
            elif not bids:reason='Sin ofertas de compra'
            else:reason='Profundidad de compra insuficiente'
            if reason:issues[reason]=issues.get(reason,0)+1
            item.update(scheduled_end=quote.get('scheduled_end'),resolution_status=quote.get('resolution_status',''),market_closed=quote.get('market_closed',False),mark=mark,value=n-f if good else None,unrealized=n-f-item['cost'] if good else None,quote_ts=q['ts'] if q else None,liquidatable_shares=qty if fresh else 0,valuation_reason=reason)
            if good:
                value+=n-f
                left=qty
                for level in sorted(bids,key=lambda l:l[0],reverse=True):
                    used=min(left,level[1]);level[1]-=used;left-=used
                    if left<=EPS:break
            else: complete=False
            cost+=item['cost'];positions.append(item)
        equity=p['cash']+value if complete else None
        if complete:
            peak=max(p['peak'],equity);dd=max(p['drawdown'],(peak-equity)/peak if peak else 0)
            if snapshot: db.execute('UPDATE pm_portfolios SET peak=?,drawdown=? WHERE id=?',(peak,dd,p['id']))
            p['peak']=peak;p['drawdown']=dd
        last=db.execute('SELECT ts,equity FROM pm_equity WHERE portfolio=? AND complete=1 ORDER BY ts DESC LIMIT 1',(p['id'],)).fetchone() if not complete else None
        p.update(equity=equity,total_pnl=equity-p['initial'] if complete else None,unrealized=value-cost if complete else None,complete=complete,positions=positions,valuation_issues=issues,last_complete_equity=last['equity'] if last else None,last_complete_ts=last['ts'] if last else None)
        if snapshot:
            stamp=now-now%60
            db.execute('INSERT OR REPLACE INTO pm_equity VALUES(?,?,?,?,?,?,?)',(stamp,p['id'],p['cash'],equity,p['realized'],value-cost if complete else None,int(complete)))
        out.append(p)
    return out


def closed_events(db,portfolio,address):
    """One observation per fully exited event, including all partial exits and costs."""
    rows=db.execute("SELECT event,SUM(realized) pnl,SUM(CASE WHEN side='BUY' THEN -cash_delta ELSE 0 END) basis FROM pm_fills WHERE portfolio=? AND address=? GROUP BY event",(portfolio,address)).fetchall()
    result=[]
    for row in rows:
        open_qty=db.execute('SELECT COALESCE(SUM(qty),0) FROM pm_positions WHERE portfolio=? AND address=? AND event=?',(portfolio,address,row['event'])).fetchone()[0]
        pending=db.execute("SELECT 1 FROM pm_orders WHERE portfolio=? AND address=? AND event=? AND state='pending'",(portfolio,address,row['event'])).fetchone()
        if row['basis']>0 and open_qty<=EPS and not pending:
            result.append(dict(event=row['event'],pnl=row['pnl'],roi=row['pnl']/row['basis']))
    return result
