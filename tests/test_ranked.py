import tempfile,time,unittest
from pathlib import Path
import engine as e

class RankedTest(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.old=e.DB;e.DB=str(Path(self.tmp.name)/'db');e.init();self.now=int(time.time())+2
  with e.database() as db:
   for i,score in enumerate([90,80,70,60]):
    a='0x'+str(i)*40;e.add_wallet(db,a,'wallet'+str(i),True,now=self.now-100)
    db.execute('UPDATE pm_wallets SET ready=1,buy_after=? WHERE address=?',(self.now-100,a))
    db.execute('INSERT INTO pm_candidates VALUES(?,?,?,?,?,?,?,?,?,?,?)',(a,'wallet'+str(i),1,1,score,30,40,.1,1,'tested',self.now))
 def tearDown(self):e.DB=self.old;self.tmp.cleanup()
 def trade(self,db,i,side,qty,tx):
  a='0x'+str(i)*40;w=dict(db.execute('SELECT * FROM pm_wallets WHERE address=?',(a,)).fetchone())
  e.signal(db,w,dict(asset='123',side=side,size=qty,price=.4,timestamp=self.now,conditionId='m',eventSlug='e',transactionHash=tx),self.now)
 def test_rank_selection_and_exits_after_demotion(self):
  with e.database() as db:
   self.assertEqual(len(e.ranked_wallets(db,self.now)),3)
   self.trade(db,3,'BUY',1000,'excluded')
   self.assertEqual(db.execute("SELECT COUNT(*) FROM pm_orders WHERE portfolio='ranked'").fetchone()[0],0)
   self.trade(db,0,'BUY',1000,'included')
   order=dict(db.execute("SELECT * FROM pm_orders WHERE portfolio='ranked'").fetchone())
   # A wide spread/deteriorated price does not trigger optimized filters.
   q=dict(ts=self.now,asks=[[.5,1000]],bids=[[.3,1000]],fee_rate=0,settlement=None,complex=False)
   e.execute(db,order,q,self.now)
   self.assertEqual(e.position(db,'ranked','0x'+'0'*40,'123')['qty'],10)
   db.execute("UPDATE pm_candidates SET score=0 WHERE address=?",('0x'+'0'*40,))
   self.trade(db,0,'SELL',1000,'exit')
   sell=db.execute("SELECT * FROM pm_orders WHERE portfolio='ranked' AND side='SELL'").fetchone()
   self.assertIsNotNone(sell);e.execute(db,dict(sell),q,self.now)
   self.assertEqual(e.position(db,'ranked','0x'+'0'*40,'123')['qty'],0)
 def test_migration_preserves_cash(self):
  with e.database() as db:
   db.execute("DELETE FROM pm_portfolios WHERE id='ranked'")
   db.execute("UPDATE pm_portfolios SET cash=777 WHERE id='filtered'")
  e.init();e.init()
  with e.database() as db:
   self.assertEqual(db.execute("SELECT cash FROM pm_portfolios WHERE id='filtered'").fetchone()[0],777)
   self.assertEqual(db.execute("SELECT cash FROM pm_portfolios WHERE id='ranked'").fetchone()[0],1000)
 def test_unranked_and_stale_wallets_wait(self):
  with e.database() as db:
   db.execute('UPDATE pm_candidates SET updated=?',(self.now-8*86400,))
   self.assertEqual(e.ranked_wallets(db,self.now),[])
 def test_pending_buy_cancelled_after_demotion(self):
  with e.database() as db:
   self.trade(db,0,'BUY',1000,'buy')
   o=dict(db.execute("SELECT * FROM pm_orders WHERE portfolio='ranked'").fetchone())
   db.execute('UPDATE pm_candidates SET score=0 WHERE address=?',(o['address'],))
   e.execute(db,o,dict(ts=self.now),self.now)
   self.assertEqual(db.execute('SELECT state FROM pm_orders WHERE id=?',(o['id'],)).fetchone()[0],'cancelled')
