import sys,time,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[1]))
import provider as p

class ProviderTest(unittest.TestCase):
    def setUp(self):p._cache.clear()
    def test_standard_neg_risk_allowed_and_grouped(self):
        a=p.market_risk({'negRisk':True,'events':[{'id':'42','slug':'election'}]},'market-a')
        b=p.market_risk({'negRisk':True,'events':[{'id':'42','slug':'election'}]},'market-b')
        self.assertFalse(a['complex']);self.assertEqual(a['event'],b['event'])
    def test_augmented_or_missing_parent_stays_blocked(self):
        self.assertTrue(p.market_risk({'negRisk':True},'m')['complex'])
        self.assertTrue(p.market_risk({'negRisk':True,'events':[{'id':'42','negRiskAugmented':True}]},'m')['complex'])
    def test_multileg_transaction_flagged(self):
        rows=[{'transactionHash':'tx','asset':'123','timestamp':100},{'transactionHash':'tx','asset':'456','timestamp':100}]
        with patch.object(p,'paged',return_value=(rows,True)):
            result=p.trades('wallet',90,110)
            self.assertTrue(all(r.get('_complex_reason') for r in result))
    def test_final_resolution_required(self):
        market={'conditionId':'c','closed':True,'umaResolutionStatus':'proposed','clobTokenIds':'["123","456"]','outcomePrices':'["1","0"]','feesEnabled':False}
        def fake(host,path,params=None):return [market] if host==p.GAMMA else {'asks':[],'bids':[]}
        with patch.object(p,'get',side_effect=fake):self.assertIsNone(p.quote('123','c')['settlement'])
        market['umaResolutionStatus']='resolved';p._cache.clear()
        with patch.object(p,'get',side_effect=fake):self.assertEqual(p.quote('123','c')['settlement'],1)
    def test_fee_schedule_validated(self):
        market={'conditionId':'c','feesEnabled':True,'feeSchedule':{'rate':.07,'exponent':1}}
        def fake(host,path,params=None):return [market] if host==p.GAMMA else {'asks':[{'price':'.5','size':'40'}],'bids':[{'price':'.49','size':'30'}],'timestamp':int(time.time()*1000)}
        with patch.object(p,'get',side_effect=fake):
            q=p.quote('123','c');self.assertEqual(q['fee_rate'],.07);self.assertEqual(q['asks'],[[.5,40.]])
    def test_pagination_cap_reported(self):
        with patch.object(p,'get',return_value=[{'a':1}]*100):
            with self.assertRaises(ValueError):p.trades('address',100,200)
    def test_candidate_not_eligible_for_lucky_single_trade(self):
        with patch.object(p,'get',return_value=[{'realizedPnl':100000,'timestamp':time.time()}]):
            r=p.evaluate({'proxyWallet':'0xabc','pnl':100000,'vol':100000})
            self.assertFalse(r['eligible']);self.assertEqual(r['concentration'],1)

if __name__=='__main__':unittest.main()
