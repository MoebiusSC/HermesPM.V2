import copy, json, os, sqlite3, sys, tempfile, time, unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[1]))
import engine as e

class PaperTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();e.DB=str(Path(self.tmp.name)/'paper.sqlite3');e.init()
        self.now=int(time.time());self.address='0x'+'a'*40;self.n=0
        with e.database() as db:
            e.add_wallet(db,self.address,'Alpha',True,now=self.now-1000)
            db.execute('UPDATE pm_wallets SET ready=1,buy_after=?',(self.now-1000,))
    def tearDown(self):self.tmp.cleanup()
    def trade(self,side='BUY',qty=1000,asset='123',price=.4,ts=None,market='m1',event='e1'):
        self.n+=1
        return dict(side=side,size=qty,asset=asset,price=price,timestamp=ts or self.now-2,transactionHash=str(self.n),conditionId=market,eventSlug=event,title='Market '+asset,outcome='YES')
    def quote(self,asks=None,bids=None,rate=0):
        return dict(ts=self.now,asks=asks or [[.4,1000]],bids=bids or [[.39,1000]],fee_rate=rate,fee_exponent=1,settlement=None,complex=False,event='e1')
    def register(self,db,t,address=None):
        w=dict(db.execute('SELECT * FROM pm_wallets WHERE address=?',(address or self.address,)).fetchone());e.signal(db,w,t,self.now)
    def execute_all(self,db,quote=None,p='filtered'):
        q=copy.deepcopy(quote or self.quote())
        for row in db.execute("SELECT * FROM pm_orders WHERE portfolio=? AND state='pending' ORDER BY created,id",(p,)).fetchall():e.execute(db,dict(row),q,self.now)
    def test_minimum_order_size_blocks_dust(self):
        with e.database() as db:
            self.register(db,self.trade(qty=100));q=self.quote();q['min_order_size']=5
            self.execute_all(db,q)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM pm_fills').fetchone()[0],0)
    def test_standard_neg_risk_paper_execution(self):
        with e.database() as db:
            self.register(db,self.trade());q=self.quote();q.update(neg_risk=True,complex=False,event='event:42')
            self.execute_all(db,q)
            self.assertAlmostEqual(e.position(db,'filtered',self.address,'123')['qty'],10)
    def test_multileg_signal_observed_not_copied(self):
        with e.database() as db:
            t=self.trade();t['_complex_reason']='multiple assets';self.register(db,t)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM pm_orders').fetchone()[0],0)
            self.assertEqual(e.source_qty(db,self.address,'123'),1000)
    def test_conversion_guard_blocks_source_dependent_orders(self):
        with e.database() as db:
            w=dict(db.execute('SELECT * FROM pm_wallets').fetchone());w['strategy_blocked']=True
            e.signal(db,w,self.trade(),self.now)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM pm_orders').fetchone()[0],0)
    def test_duplicate_and_fee_accounting(self):
        with e.database() as db:
            t=self.trade();self.register(db,t);self.register(db,t);self.execute_all(db,self.quote(rate=.07))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM pm_signals').fetchone()[0],1)
            pos=e.position(db,'filtered',self.address,'123');p=db.execute("SELECT * FROM pm_portfolios WHERE id='filtered'").fetchone()
            self.assertAlmostEqual(pos['qty'],10)
            self.assertAlmostEqual(pos['cost'],4+.168)
            self.assertAlmostEqual(p['cash']+pos['cost'],1000)
            self.assertAlmostEqual(p['fees'],.168)
    def test_proportional_sell_and_pause(self):
        with e.database() as db:
            self.register(db,self.trade());self.execute_all(db)
            db.execute('UPDATE pm_wallets SET auto=0')
            self.register(db,self.trade('SELL',300));self.execute_all(db,self.quote(bids=[[.5,1000]],asks=[[.51,1000]],rate=.04))
            pos=e.position(db,'filtered',self.address,'123')
            self.assertAlmostEqual(pos['qty'],7)
            self.assertAlmostEqual(pos['cost'],2.8)
            p=db.execute("SELECT * FROM pm_portfolios WHERE id='filtered'").fetchone()
            self.assertAlmostEqual(p['realized'],.27)
            self.assertAlmostEqual(p['cash']+pos['cost'],1000.27)
    def test_wallet_inventory_isolation(self):
        b='0x'+'b'*40
        with e.database() as db:
            e.add_wallet(db,b,'Beta',True,now=self.now-1000);db.execute('UPDATE pm_wallets SET ready=1,buy_after=?',(self.now-1000,))
            self.register(db,self.trade());self.register(db,self.trade(qty=500),b);self.execute_all(db)
            self.register(db,self.trade('SELL',1000));self.execute_all(db)
            self.assertAlmostEqual(e.position(db,'filtered',self.address,'123')['qty'],0)
            self.assertAlmostEqual(e.position(db,'filtered',b,'123')['qty'],5)
    def test_exit_partial_reservations_and_retries(self):
        with e.database() as db:
            self.register(db,self.trade());self.execute_all(db)
            self.register(db,self.trade('SELL',500));self.register(db,self.trade('SELL',500))
            self.execute_all(db,self.quote(bids=[[.39,3]]))
            self.assertAlmostEqual(e.position(db,'filtered',self.address,'123')['qty'],7)
            self.execute_all(db,self.quote(bids=[[.39,100]]))
            self.assertAlmostEqual(e.position(db,'filtered',self.address,'123')['qty'],0)
            self.assertGreaterEqual(db.execute("SELECT cash FROM pm_portfolios WHERE id='filtered'").fetchone()[0],0)
    def test_market_limit_accumulates(self):
        with e.database() as db:
            for _ in range(8):
                self.register(db,self.trade(qty=100000));self.execute_all(db)
            self.assertLessEqual(e.position(db,'filtered',self.address,'123')['cost'],e.LIMITS['market']+1e-7)
    def test_event_limit_across_markets(self):
        with e.database() as db:
            for i in range(10):
                self.register(db,self.trade(qty=100000,asset=str(i+200),market='m'+str(i)));self.execute_all(db)
            cost=db.execute("SELECT SUM(cost) FROM pm_positions WHERE portfolio='filtered'").fetchone()[0]
            self.assertLessEqual(cost,e.LIMITS['event']+1e-7)
    def test_relative_filter_and_reference(self):
        with e.database() as db:
            self.register(db,self.trade(price=.1));q=self.quote(asks=[[.11,1000]],bids=[[.1,1000]])
            self.execute_all(db,q);self.execute_all(db,q,p='reference')
            self.assertEqual(e.position(db,'filtered',self.address,'123')['qty'],0)
            self.assertGreater(e.position(db,'reference',self.address,'123')['qty'],0)
    def test_stale_and_prebaseline_never_copy(self):
        with e.database() as db:
            self.register(db,self.trade(ts=self.now-500));self.register(db,self.trade(ts=self.now-1100));self.execute_all(db)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM pm_fills').fetchone()[0],0)
    def test_unknown_fee_and_stale_quote_block(self):
        with e.database() as db:
            self.register(db,self.trade());q=self.quote();q['fee_rate']=None;self.execute_all(db,q)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM pm_fills').fetchone()[0],0)
            q=self.quote();q['ts']=self.now-121;self.execute_all(db,q)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM pm_fills').fetchone()[0],0)
    def test_settlement_once(self):
        with e.database() as db:
            self.register(db,self.trade());self.execute_all(db);e.settle(db,'123',1.,self.now);e.settle(db,'123',1.,self.now)
            p=db.execute("SELECT * FROM pm_portfolios WHERE id='filtered'").fetchone()
            self.assertAlmostEqual(p['cash'],1006);self.assertAlmostEqual(p['realized'],6)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM pm_fills WHERE side='SELL'").fetchone()[0],1)
    def test_unknown_valuation_not_zero_profit(self):
        with e.database() as db:
            self.register(db,self.trade());self.execute_all(db)
            p=next(p for p in e.portfolios(db,self.now) if p['id']=='filtered')
            self.assertIsNone(p['equity']);self.assertFalse(p['complete'])
    def test_restart_keeps_cash_and_positions(self):
        with e.database() as db:self.register(db,self.trade());self.execute_all(db)
        e.init()
        with e.database() as db:
            self.assertAlmostEqual(db.execute("SELECT cash FROM pm_portfolios WHERE id='filtered'").fetchone()[0],996)
            self.assertAlmostEqual(e.position(db,'filtered',self.address,'123')['qty'],10)
    def test_manual_close_no_oversell(self):
        with e.database() as db:
            self.register(db,self.trade());self.execute_all(db);self.register(db,self.trade('SELL',500))
            e.close_position(db,'filtered',self.address,'123',self.now);self.execute_all(db)
            self.assertAlmostEqual(e.position(db,'filtered',self.address,'123')['qty'],0)
    def test_daily_drawdown_blocks_new_buys(self):
        with e.database() as db:
            db.execute('INSERT INTO pm_equity VALUES(?,?,?,?,?,?,?)',(self.now-10,'filtered',1000,1000,0,0,1))
            db.execute('INSERT INTO pm_equity VALUES(?,?,?,?,?,?,?)',(self.now,'filtered',900,900,0,0,1))
            self.register(db,self.trade());self.execute_all(db)
            self.assertEqual(e.position(db,'filtered',self.address,'123')['qty'],0)

class MigrationTest(unittest.TestCase):
    def test_legacy_import_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            e.DB=str(Path(tmp)/'v1.db')
            with sqlite3.connect(e.DB) as db:
                db.executescript("CREATE TABLE wallets(address TEXT,label TEXT,enabled INTEGER,auto INTEGER); CREATE TABLE settings(key TEXT,value TEXT); INSERT INTO settings VALUES('cash','996'); CREATE TABLE fills(id INTEGER,shares REAL,cash_delta REAL,address TEXT,asset TEXT,title TEXT,outcome TEXT); INSERT INTO wallets VALUES('0xabc','Legacy',1,1); INSERT INTO fills VALUES(1,10,-4,'0xabc','123','Market','Yes');")
            e.init();e.init()
            with e.database() as db:
                self.assertAlmostEqual(e.position(db,'filtered','0xabc','123')['qty'],10)
                self.assertEqual(db.execute("SELECT cash FROM pm_portfolios WHERE id='filtered'").fetchone()[0],996)
                self.assertTrue(Path(e.DB+'.pre-v2.bak').exists())

if __name__=='__main__':unittest.main()
