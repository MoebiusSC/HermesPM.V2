"""Autonomous discovery, concurrent polling, reconciliation, valuation and maintenance."""
import copy, json, os, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import engine as e
import provider as api
POLL=max(5,int(os.getenv('POLL_SECONDS','5')))
STARTED=time.time()
CYCLE_LOCK=threading.Lock()
CYCLES=[]
WAKE=threading.Event()
STATE={'last_poll':None,'last_error':None,'last_discovery':None,'research_error':None,'cycle_seconds':0,'mode':'paper-only','version':'3.1.1','project':'HermesPM.V2','poll_seconds':POLL,'effective_interval':POLL,'cycle_overruns':0,'worker_heartbeat':None}


def collect(w,now):
    if not w['ready']:
        holdings=api.positions(w['address'])
        # Barrier after snapshot prevents backfilling historical trades into paper.
        return {'baseline':holdings,'barrier':int(time.time())}
    activity=api.complex_activity(w['address'],w['cursor'],now)
    return {'trades':api.trades(w['address'],w['cursor'],now),'activity':activity}


def cycle():
    started=time.time();now=int(started);errors=[]
    with e.LOCK,e.database() as db:
        wallets=[dict(w) for w in db.execute('SELECT * FROM pm_wallets WHERE enabled=1 OR address IN (SELECT address FROM pm_positions WHERE qty>?)',(e.EPS,))]
    with ThreadPoolExecutor(max_workers=8) as pool:
        jobs={pool.submit(collect,w,now):w for w in wallets}
        for future in as_completed(jobs):
            old=jobs[future]
            try:
                result=future.result()
                with e.LOCK,e.database() as db:
                    w=dict(db.execute('SELECT * FROM pm_wallets WHERE address=?',(old['address'],)).fetchone())
                    if 'baseline' in result:
                        for asset,qty in result['baseline'].items():e.set_source(db,w['address'],asset,qty)
                        db.execute('UPDATE pm_wallets SET ready=1,cursor=?,buy_after=?,last_poll=?,last_reconcile=?,error=? WHERE address=?',(result['barrier'],result['barrier'],now,now,'',w['address']))
                        e.note(db,'baseline',w['address']+' '+str(len(result['baseline']))+' positions',now)
                    else:
                        complex_rows=[r for r in result.get('activity',[]) if int(r.get('timestamp') or 0)>w['buy_after']]
                        if complex_rows:
                            marker='strategy_block:'+w['address']
                            detail=','.join(sorted({r['type'] for r in complex_rows}))
                            if not db.execute('SELECT 1 FROM pm_meta WHERE k=?',(marker,)).fetchone():
                                db.execute('INSERT INTO pm_meta VALUES(?,?)',(marker,detail))
                                e.note(db,'complex_activity',w['address']+' '+detail,now)
                            db.execute('UPDATE pm_wallets SET blocked=1,error=? WHERE address=?',('conversión/split/merge detectado; nueva línea base requerida',w['address']))
                            db.execute("UPDATE pm_orders SET state='cancelled',reason='inventario afectado por operación compleja' WHERE address=? AND side='BUY' AND state='pending'",(w['address'],))
                            w['blocked']=1
                        w['strategy_blocked']=bool(db.execute('SELECT 1 FROM pm_meta WHERE k=?',('strategy_block:'+w['address'],)).fetchone())
                        for t in result['trades']:e.signal(db,w,t,int(time.time()))
                        db.execute('UPDATE pm_wallets SET cursor=?,last_poll=? WHERE address=?',(now,now,w['address']))
                        if not w['blocked']:db.execute("UPDATE pm_wallets SET error='' WHERE address=?",(w['address'],))
            except Exception as exc:
                errors.append(old['label']+': '+str(exc)[:120])
                with e.LOCK,e.database() as db:
                    db.execute('UPDATE pm_wallets SET error=? WHERE address=?',(str(exc)[:200],old['address']))
                    # Failed collection doesn't move watermark; retry safely next cycle.
    with e.LOCK,e.database() as db:
        assets={r['asset']:r['market'] for r in db.execute("SELECT asset,market FROM pm_orders WHERE state='pending' UNION SELECT asset,market FROM pm_positions WHERE qty>?",(e.EPS,))}
    quotes={};quote_errors={}
    with ThreadPoolExecutor(max_workers=8) as pool:
        jobs={pool.submit(api.quote,a,c):a for a,c in assets.items()}
        for f in as_completed(jobs):
            asset=jobs[f]
            try:quotes[asset]=f.result()
            except Exception as exc:
                quote_errors[asset]=str(exc)[:160]
                errors.append('quote '+asset[:8]+': '+str(exc)[:100])
    now=int(time.time())
    with e.LOCK,e.database() as db:
        for asset in assets:
            if asset in quotes:
                q=quotes[asset]
                db.execute('INSERT INTO pm_quotes VALUES(?,?,?,?) ON CONFLICT(asset) DO UPDATE SET ts=excluded.ts,payload=excluded.payload,error=excluded.error',(asset,q['ts'],json.dumps(q),''))
                # Canonical event from Gamma prevents concentration across different outcomes/markets.
                db.execute('UPDATE pm_positions SET event=? WHERE asset=?',(q['event'],asset))
                db.execute('UPDATE pm_orders SET event=? WHERE asset=?',(q['event'],asset))
                if q['settlement'] is not None:e.settle(db,asset,q['settlement'],now)
            else:
                db.execute("INSERT INTO pm_quotes(asset,ts,payload,error) VALUES(?,?,?,?) ON CONFLICT(asset) DO UPDATE SET error=excluded.error",(asset,0,'{}',quote_errors.get(asset,'quote refresh failed')))
        for p in e.POLICIES:
            available=copy.deepcopy(quotes)
            orders=db.execute("SELECT * FROM pm_orders WHERE portfolio=? AND state='pending' ORDER BY CASE side WHEN 'SELL' THEN 0 ELSE 1 END,created,id",(p,)).fetchall()
            for o in orders:e.execute(db,dict(o),available.get(o['asset']),now)
        e.portfolios(db,now,snapshot=True)
    CYCLES.append(round(time.time()-started,3))
    del CYCLES[:-500]
    if CYCLES[-1]>POLL:STATE['cycle_overruns']+=1
    STATE.update(last_poll=now,last_error='; '.join(errors)[:1500] or None,cycle_seconds=round(time.time()-started,2))


def reconcile():
    with e.LOCK,e.database() as db:
        wallets=[dict(w) for w in db.execute('SELECT * FROM pm_wallets WHERE ready=1 AND enabled=1 AND (last_reconcile<? OR (blocked=1 AND last_reconcile<?))',(int(time.time())-900,int(time.time())-300))]
    for w in wallets:
        try:
            observed=api.positions(w['address'])
            with e.LOCK,e.database() as db:
                expected={r['asset']:r['qty'] for r in db.execute('SELECT * FROM pm_source WHERE address=?',(w['address'],)) if r['qty']>e.EPS}
            missing={a for a,qty in expected.items() if qty>e.EPS and observed.get(a,0)<=e.EPS}
            resolved=set()
            for asset in missing:
                try:
                    if api.resolved_asset(asset):resolved.add(asset)
                except Exception:
                    # Leave uncertain inventory blocked; retry on the next reconciliation.
                    pass
            with e.LOCK,e.database() as db:
                for asset in resolved:
                    e.set_source(db,w['address'],asset,0)
                    e.note(db,'source_resolved',w['address']+' '+asset+'; official final resolution, public position absent')
                expected={r['asset']:r['qty'] for r in db.execute('SELECT * FROM pm_source WHERE address=?',(w['address'],)) if r['qty']>e.EPS}
                diff={a:(expected.get(a,0),observed.get(a,0)) for a in expected.keys()|observed.keys() if abs(expected.get(a,0)-observed.get(a,0))>max(.01,observed.get(a,0)*.001)}
                if diff:
                    db.execute('UPDATE pm_wallets SET blocked=1,error=? WHERE address=?',('diferencia de inventario; entradas pausadas hasta reconciliar',w['address']))
                    e.note(db,'reconcile_difference',json.dumps({'address':w['address'],'differences':dict(list(diff.items())[:10])}))
                elif not db.execute('SELECT 1 FROM pm_meta WHERE k=?',('strategy_block:'+w['address'],)).fetchone():
                    db.execute("UPDATE pm_wallets SET blocked=0,error='' WHERE address=?",(w['address'],))
                db.execute('UPDATE pm_wallets SET last_reconcile=? WHERE address=?',(int(time.time()),w['address']))
        except Exception as exc:
            with e.LOCK,e.database() as db:e.note(db,'reconcile_error',w['address']+' '+str(exc)[:150])


def discover():
    candidates=api.discovery();now=int(time.time())
    with e.LOCK,e.database() as db:
        for w in candidates:
            a=str(w['proxyWallet']).lower()
            db.execute('INSERT INTO pm_candidates VALUES(?,?,?,?,0,0,0,1,0,?,?) ON CONFLICT(address) DO UPDATE SET pnl=excluded.pnl,volume=excluded.volume,label=excluded.label', (a,str(w.get('userName') or a[:10])[:60],float(w.get('pnl') or 0),float(w.get('vol') or 0),'pendiente de evaluación; ranking mensual',now))
        # Rotate evaluations so every discovered candidate is eventually reviewed.
        addresses={r[0] for r in db.execute("SELECT c.address FROM pm_candidates c LEFT JOIN pm_wallets w ON w.address=c.address ORDER BY CASE WHEN w.enabled=1 THEN 0 ELSE 1 END,c.updated ASC LIMIT 10")}
        addresses.update(r[0] for r in db.execute("SELECT address FROM pm_candidates WHERE address NOT IN (SELECT address FROM pm_wallets WHERE enabled=1) ORDER BY updated ASC LIMIT 5"))
        selected=[dict(proxyWallet=r['address'],userName=r['label'],pnl=r['pnl'],vol=r['volume']) for r in db.execute('SELECT * FROM pm_candidates') if r['address'] in addresses]
    with ThreadPoolExecutor(max_workers=2) as pool:
        jobs=[pool.submit(api.evaluate,w) for w in selected]
        for f in as_completed(jobs):
            try:
                x=f.result()
                with e.LOCK,e.database() as db:db.execute('INSERT OR REPLACE INTO pm_candidates VALUES(?,?,?,?,?,?,?,?,?,?,?)',tuple(x[k] for k in ('address','label','pnl','volume','score','sample_count','span_days','concentration','eligible','reason','updated')))
            except Exception as exc:
                with e.LOCK,e.database() as db:e.note(db,'candidate_error',str(exc)[:200])
    with e.LOCK,e.database() as db:
        auto=db.execute("SELECT v FROM pm_meta WHERE k='automation'").fetchone()[0]=='1'
        if auto:
            count=db.execute('SELECT COUNT(*) FROM pm_wallets WHERE enabled=1').fetchone()[0]
            for w in db.execute('SELECT * FROM pm_candidates WHERE eligible=1 AND updated>? ORDER BY score DESC',(now-86400,)).fetchall():
                if count>=10:break
                # Do not re-enable a user-paused wallet.
                if not db.execute('SELECT 1 FROM pm_wallets WHERE address=?',(w['address'],)).fetchone():
                    e.add_wallet(db,w['address'],w['label'],auto=True,origin='discovery',now=now);count+=1
        e.note(db,'discovery',str(len(candidates))+' candidates; '+str(len(selected))+' evaluated; auto='+str(auto))
        db.execute("INSERT INTO pm_meta VALUES('last_discovery',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",(str(now),))
    STATE['last_discovery']=now


def review_weights():
    now=int(time.time())
    with e.LOCK,e.database() as db:
        reference=next(p for p in e.portfolios(db,now) if p['id']=='reference')
        for w in db.execute('SELECT * FROM pm_wallets WHERE weight_review<?',(now-86400,)).fetchall():
            # Use prior reference results to avoid rewarding the adaptive portfolio's own allocation.
            rows=db.execute("SELECT realized,cash_delta,ts,market FROM pm_fills WHERE portfolio='reference' AND address=? AND side='SELL' ORDER BY ts",(w['address'],)).fetchall()
            age=(now-w['added'])/86400
            pnl=sum(x['realized'] for x in rows);basis=sum(x['cash_delta']-x['realized'] for x in rows)
            closed=e.closed_events(db,'reference',w['address'])
            distinct=len(closed)
            weight=w['weight']
            open_positions=[p for p in reference['positions'] if p['address']==w['address']]
            reliable=all(p['unrealized'] is not None for p in open_positions)
            net=pnl+sum(p['unrealized'] or 0 for p in open_positions)
            if age>=14 and distinct>=20 and basis>0 and reliable:
                returns=[x['roi'] for x in closed]
                mean=sum(returns)/len(returns)
                variance=sum((v-mean)**2 for v in returns)/max(1,len(returns)-1)
                lower=mean-1.96*(variance/len(returns))**.5
                target=.5 if net<0 else (1.25 if lower>0 else min(1.,weight))
                weight=max(.5,min(1.25,w['weight']+max(-.25,min(.25,target-w['weight']))))
            db.execute('UPDATE pm_wallets SET weight=?,weight_review=? WHERE address=?',(weight,now,w['address']))
            if abs(weight-w['weight'])>1e-8:e.note(db,'allocation_review',json.dumps({'wallet':w['address'],'old':w['weight'],'new':weight,'realized':pnl,'closed_markets':distinct}))


def maintenance():
    now=int(time.time())
    with e.LOCK,e.database() as db:
        db.execute('DELETE FROM pm_equity WHERE ts<? AND ts%3600!=0',(now-7*86400,))
        db.execute('DELETE FROM pm_audit WHERE ts<?',(now-90*86400,))
        db.execute("DELETE FROM pm_quotes WHERE asset NOT IN (SELECT asset FROM pm_positions WHERE qty>?) AND ts<?",(e.EPS,now-86400))
        # Consistent daily backup; on-volume redundancy, not external disaster backup.
        stamp=time.strftime('%Y-%m-%d',time.gmtime(now))
        db.execute("INSERT INTO pm_meta VALUES('maintenance',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",(stamp,))
    import sqlite3
    from pathlib import Path
    folder=Path(e.DB).parent/'backups';folder.mkdir(exist_ok=True)
    target=folder/('hermes-'+stamp+'.sqlite3')
    with e.LOCK,e.database() as db:
        with sqlite3.connect(target) as out:db.backup(out)
    for file in sorted(folder.glob('hermes-*.sqlite3'))[:-3]:file.unlink()


def health():
    now=time.time();last=STATE['last_poll']
    ok=(last is not None and now-last<max(60,POLL*4)) or (last is None and now-STARTED<60)
    return dict(ok=ok,version=STATE['version'],project='HermesPM.V2',mode='paper-only',last_poll=last,poll_seconds=POLL)


def worker():
    failures=0
    while True:
        start=time.monotonic();STATE['worker_heartbeat']=int(time.time())
        try:
            with CYCLE_LOCK:cycle()
            failures=min(4,failures+1) if STATE['last_error'] else 0
        except Exception as exc:
            STATE['last_error']=str(exc)[:500];failures=min(4,failures+1)
        interval=min(60,POLL*2**failures)
        STATE['effective_interval']=interval
        WAKE.wait(max(.1,interval-(time.monotonic()-start)));WAKE.clear()


def research_worker():
    last_maintenance=0
    while True:
        try:
            with e.LOCK,e.database() as db:
                row=db.execute("SELECT v FROM pm_meta WHERE k='last_discovery'").fetchone()
                last=int(row[0]) if row else 0
            STATE['last_discovery']=last or None
            if time.time()-last>3600:discover()
            reconcile();review_weights()
            if time.time()-last_maintenance>86400:maintenance();last_maintenance=time.time()
            STATE['research_error']=None
        except Exception as exc:
            STATE['research_error']=str(exc)[:300]
        time.sleep(60)
