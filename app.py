"""Hermes PM V2 HTTP dashboard. Public data in, paper ledger out."""
import hmac, json, os, re, sqlite3, tempfile, threading, time, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import engine as e
import service
WALLET=re.compile(r'^0x[a-fA-F0-9]{40}$')


def state():
    now=int(time.time())
    with e.LOCK,e.database() as db:
        portfolios=e.portfolios(db,now)
        stats={p:dict(db.execute('SELECT COUNT(*) fills,COALESCE(SUM(CASE side WHEN \'SELL\' THEN 1 ELSE 0 END),0) exits,AVG(ts-source_ts) latency FROM pm_fills WHERE portfolio=?',(p,)).fetchone()) for p in e.POLICIES}
        for p in portfolios:p.update(stats[p['id']])
        return dict(ranked_selection=e.ranked_wallets(db,now),status=dict(service.STATE),telemetry=telemetry(db,now),portfolios=portfolios,
            wallets=[dict(x) for x in db.execute('SELECT * FROM pm_wallets ORDER BY added DESC')],
            candidates=[dict(x) for x in db.execute('SELECT * FROM pm_candidates ORDER BY eligible DESC,score DESC,pnl DESC LIMIT 200')],
            orders=[dict(x) for x in db.execute('SELECT * FROM pm_orders ORDER BY created DESC LIMIT 200')],
            fills=[dict(x) for x in db.execute('SELECT * FROM pm_fills ORDER BY id DESC LIMIT 200')],
            signals=[dict(x) for x in db.execute('SELECT * FROM pm_signals ORDER BY detected DESC LIMIT 100')],
            audit=[dict(x) for x in db.execute('SELECT * FROM pm_audit ORDER BY id DESC LIMIT 60')],
            equity=[dict(x) for x in db.execute('SELECT * FROM (SELECT * FROM pm_equity ORDER BY ts DESC LIMIT 3000) ORDER BY ts')],
            totals=dict(db.execute('SELECT (SELECT COUNT(*) FROM pm_candidates) candidates,(SELECT COUNT(*) FROM pm_signals) signals,(SELECT COUNT(*) FROM pm_fills) fills').fetchone()),
            limits=e.LIMITS,automation=db.execute("SELECT v FROM pm_meta WHERE k='automation'").fetchone()[0]=='1',
            storage={'persistent_mount':Path('/data').is_mount(),'db_path':e.DB,'size_mb':round(Path(e.DB).stat().st_size/1048576,2)},
            assumptions=['Copia proporcional base: 1% de shares, con límites acumulados.',
            'Simulación con profundidad visible y comisiones equivalentes en efectivo; no replica colas ni impacto futuro.',
            'Neg-risk estándar permitido con evento padre verificado. Conversiones y transacciones con varios activos no se copian.',
            'Candidatos: hasta 300 cierres, semanas netas y P&L abierto; muestra parcial, score exploratorio.',
            'Valoración incompleta si faltan cotizaciones recientes o profundidad para liquidar.',
            'Adaptativa: espera 14 días y 20 eventos totalmente cerrados en referencia; ajustes diarios limitados; sin evidencia conserva peso previo.',
            'Copias de respaldo diarias en el mismo volumen, últimas 3. Descarga externa disponible.'])


def percentile(values,q):
    values=sorted(values)
    return round(values[min(len(values)-1,int((len(values)-1)*q))],2) if values else None

def telemetry(db,now):
    signals=db.execute('SELECT ts,detected FROM pm_signals ORDER BY detected DESC LIMIT 1000').fetchall()
    fills=db.execute("SELECT f.ts,f.source_ts,s.detected FROM pm_fills f LEFT JOIN pm_orders o ON o.id=f.order_id LEFT JOIN pm_signals s ON s.id=o.signal WHERE f.portfolio='reference' AND s.id IS NOT NULL ORDER BY f.id DESC LIMIT 1000").fetchall()
    detection=[max(0,r['detected']-r['ts']) for r in signals]
    execution=[max(0,r['ts']-r['detected']) for r in fills]
    return dict(detection_p50=percentile(detection,.5),detection_p95=percentile(detection,.95),execution_p95=percentile(execution,.95),cycle_p95=percentile(service.CYCLES,.95),api=dict(service.api.METRICS),sample_signals=len(signals),sample_fills=len(fills),window='últimas 1000 señales/ejecuciones; ciclos del proceso actual',runs=[dict(r) for r in db.execute('SELECT * FROM pm_runs')])


class Handler(BaseHTTPRequestHandler):
    def output(self,code,body,kind='application/json; charset=utf-8',extra=None):
        if not isinstance(body,bytes):body=json.dumps(body,ensure_ascii=False,allow_nan=False).encode()
        self.send_response(code);self.send_header('Content-Type',kind);self.send_header('Cache-Control','no-store')
        self.send_header('X-Content-Type-Options','nosniff');self.send_header('X-Frame-Options','DENY')
        self.send_header('Content-Length',str(len(body)))
        for k,v in (extra or {}).items():self.send_header(k,v)
        self.end_headers();self.wfile.write(body)

    def auth(self):
        key=os.getenv('HERMES_PM_KEY','')
        return bool(key) and hmac.compare_digest(self.headers.get('X-Hermes-Key',''),key)

    def do_GET(self):
        path=urllib.parse.urlparse(self.path).path
        if path=='/health':
            health=service.health();return self.output(200 if health['ok'] else 503,health)
        if path=='/':return self.output(200,Path(__file__).with_name('static').joinpath('index.html').read_bytes(),'text/html; charset=utf-8')
        if not self.auth():return self.output(401,{'error':'Clave requerida'})
        try:
            if path=='/api/state':return self.output(200,state())
            if path=='/api/backup':
                with tempfile.TemporaryDirectory() as temp:
                    file=Path(temp)/'hermes-pm.sqlite3'
                    with e.LOCK,e.database() as db:
                        with sqlite3.connect(file) as out:db.backup(out)
                    return self.output(200,file.read_bytes(),'application/octet-stream',{'Content-Disposition':'attachment; filename="hermes-pm-backup.sqlite3"'})
            return self.output(404,{'error':'Ruta desconocida'})
        except Exception as exc:return self.output(500,{'error':str(exc)[:250]})

    def do_POST(self):
        if not self.auth():return self.output(401,{'error':'Clave requerida'})
        try:
            size=int(self.headers.get('Content-Length','0'))
            if not 0<=size<=4096:return self.output(413,{'error':'Solicitud demasiado grande'})
            data=json.loads(self.rfile.read(size) or b'{}');path=urllib.parse.urlparse(self.path).path
            if not isinstance(data,dict):raise ValueError('Objeto JSON requerido')
            address=str(data.get('address','')).lower();now=int(time.time())
            with e.LOCK,e.database() as db:
                if path in ('/api/wallets','/api/wallet-mode','/api/remove-wallet','/api/rebaseline'):
                    if not WALLET.fullmatch(address):raise ValueError('Dirección EVM inválida')
                    if path=='/api/wallets':
                        count=db.execute('SELECT COUNT(*) FROM pm_wallets WHERE enabled=1').fetchone()[0]
                        if count>=20 and not db.execute('SELECT 1 FROM pm_wallets WHERE address=?',(address,)).fetchone():raise ValueError('Máximo 20 wallets vigiladas en V2')
                        e.add_wallet(db,address,str(data.get('label') or address[:10]),False)
                    else:
                        if not db.execute('SELECT 1 FROM pm_wallets WHERE address=?',(address,)).fetchone():raise ValueError('Wallet inexistente')
                        if path=='/api/wallet-mode':
                            auto=int(data.get('auto') is True)
                            db.execute('UPDATE pm_wallets SET auto=?,enabled=1 WHERE address=?',(auto,address))
                            e.note(db,'wallet_mode',address+' auto='+str(auto),now)
                        elif path=='/api/remove-wallet':
                            db.execute('UPDATE pm_wallets SET enabled=0,auto=0 WHERE address=?',(address,))
                            e.note(db,'wallet_paused',address+' exits remain active',now)
                        else:
                            db.execute('DELETE FROM pm_source WHERE address=?',(address,))
                            db.execute('DELETE FROM pm_meta WHERE k=?',('strategy_block:'+address,))
                            db.execute("UPDATE pm_wallets SET ready=0,blocked=0,error='' WHERE address=?",(address,))
                            db.execute("UPDATE pm_orders SET state='cancelled',reason='nueva línea base' WHERE address=? AND side='BUY' AND state='pending'",(address,))
                            e.note(db,'rebaseline_requested',address,now)
                elif path=='/api/close':
                    p=data.get('portfolio');asset=str(data.get('asset',''))
                    if p not in e.POLICIES or not WALLET.fullmatch(address) or not asset.isdigit():raise ValueError('Posición inválida')
                    e.close_position(db,p,address,asset,now)
                elif path=='/api/automation':
                    db.execute("UPDATE pm_meta SET v=? WHERE k='automation'",('1' if data.get('enabled') is True else '0',))
                    e.note(db,'automation',str(data.get('enabled') is True),now)
                else:return self.output(404,{'error':'Ruta desconocida'})
            service.WAKE.set()
            return self.output(200,{'ok':True})
        except (ValueError,TypeError,KeyError) as exc:return self.output(400,{'error':str(exc)[:200]})
        except Exception as exc:return self.output(500,{'error':str(exc)[:200]})


if __name__=='__main__':
    if os.getenv('REQUIRE_PERSISTENT_VOLUME')=='1' and not Path('/data').is_mount():
        raise RuntimeError('Persistent /data mount required; refusing ephemeral ledger')
    e.init()
    seed=os.getenv('SEED_WALLETS_JSON','')
    if seed:
        with e.LOCK,e.database() as db:
            if not db.execute("SELECT 1 FROM pm_meta WHERE k='cohort_seeded'").fetchone():
                for w in json.loads(seed):e.add_wallet(db,w['address'],w['label'],True,'original-cohort')
                db.execute("INSERT INTO pm_meta VALUES('cohort_seeded','1')")
    threading.Thread(target=service.worker,daemon=True).start()
    threading.Thread(target=service.research_worker,daemon=True).start()
    ThreadingHTTPServer(('0.0.0.0',int(os.getenv('PORT','8000'))),Handler).serve_forever()
