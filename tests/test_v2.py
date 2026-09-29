import json,tempfile,time,unittest
from pathlib import Path
from unittest.mock import patch
import engine as e, provider as api, service, app

class V2Test(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.old=e.DB;e.DB=str(Path(self.tmp.name)/'db');e.init()
        self.a='0x'+'c'*40
    def tearDown(self):e.DB=self.old;self.tmp.cleanup()
    def test_negative_net_weeks_do_not_qualify(self):
        rows=[]
        for week in range(8):
            for value in (10,10,10,-40):rows.append(dict(timestamp=1700000000+week*604800,realizedPnl=value))
        rows.append(dict(timestamp=1700000000+9*604800,realizedPnl=100))
        with patch.object(api,'paged',side_effect=[(rows,True),([],True)]):
            result=api.evaluate(dict(proxyWallet=self.a))
        self.assertFalse(result['eligible']);self.assertIn('1 semanas netas',result['reason'])
    def test_open_losses_exclude_candidate(self):
        rows=[dict(timestamp=1700000000+i*86400,realizedPnl=10) for i in range(50)]
        with patch.object(api,'paged',side_effect=[(rows,True),([dict(cashPnl=-1000)],True)]):
            self.assertFalse(api.evaluate(dict(proxyWallet=self.a))['eligible'])
    def test_inadequate_evidence_does_not_restore_weight(self):
        with e.database() as db:
            e.add_wallet(db,self.a,'w',True);db.execute('UPDATE pm_wallets SET weight=.5')
        service.review_weights()
        with e.database() as db:self.assertEqual(db.execute('SELECT weight FROM pm_wallets').fetchone()[0],.5)
    def test_partial_exit_not_closed_event(self):
        now=int(time.time())
        with e.database() as db:
            e.add_wallet(db,self.a,'w',True)
            o=dict(id='buy',portfolio='reference',address=self.a,asset='123',market='m',event='event:1',title='T',outcome='yes',side='BUY',remaining=10,source_ts=now,source_price=.5)
            e.apply_fill(db,o,10,5,0,now,now,'test')
            o.update(id='sell',side='SELL',remaining=10)
            e.apply_fill(db,o,5,3,0,now,now,'test')
            self.assertEqual(e.closed_events(db,'reference',self.a),[])
            e.apply_fill(db,o,5,3,0,now,now,'test')
            closed=e.closed_events(db,'reference',self.a)
            self.assertEqual(len(closed),1);self.assertAlmostEqual(closed[0]['roi'],.2)
    def test_wallet_budget_only_optimized_portfolios(self):
        with e.database() as db:
            base=dict(address=self.a,market='m',event='e')
            self.assertEqual(e.room(db,dict(base,portfolio='filtered')),75)
            db.execute('INSERT INTO pm_positions VALUES(?,?,?,?,?,?,?,?,?)',('filtered',self.a,'123','other','other','t','yes',100,90))
            self.assertEqual(e.room(db,dict(base,portfolio='filtered')),10)
            self.assertEqual(e.room(db,dict(base,portfolio='reference')),75)
    def test_stalled_worker_health(self):
        with patch.dict(service.STATE,last_poll=time.time()-1000):self.assertFalse(service.health()['ok'])
        with patch.dict(service.STATE,last_poll=time.time()):self.assertTrue(service.health()['ok'])
    def test_429_cooldown_prevents_next_request(self):
        import urllib.error
        api._requests.clear();api._cooldowns.clear()
        error=urllib.error.HTTPError('url',429,'rate',{'Retry-After':'10'},None)
        with patch.object(api.urllib.request,'urlopen',side_effect=error) as request:
            with self.assertRaises(urllib.error.HTTPError):api.get(api.DATA,'/test')
            with self.assertRaises(RuntimeError):api.get(api.DATA,'/test')
            self.assertEqual(request.call_count,1)
        api._cooldowns.clear();api._requests.clear()
    def test_telemetry_separates_detection(self):
        with e.database() as db:
            db.execute('INSERT INTO pm_signals VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',('s',self.a,'1','m','e','t','yes','BUY',10,.5,100,107,0,''))
            metrics=app.telemetry(db,110)
            self.assertEqual(metrics['detection_p50'],7);self.assertIsNone(metrics['execution_p95'])
