import http.client,json,os,sys,tempfile,threading,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[1]))
import app,engine as e

class HttpTest(unittest.TestCase):
    def test_auth_add_state_and_backup(self):
        with tempfile.TemporaryDirectory() as temp:
            e.DB=str(Path(temp)/'db.sqlite3');e.init();os.environ['HERMES_PM_KEY']='test-only'
            server=app.ThreadingHTTPServer(('127.0.0.1',0),app.Handler)
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            try:
                conn=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=5)
                conn.request('GET','/api/state');res=conn.getresponse();self.assertEqual(res.status,401);res.read()
                headers={'X-Hermes-Key':'test-only','Content-Type':'application/json'}
                conn.request('POST','/api/wallets',json.dumps({'address':'0x'+'a'*40,'label':'<script>alert(1)</script>'}),headers)
                res=conn.getresponse();self.assertEqual(res.status,200);res.read()
                conn.request('GET','/api/state',headers=headers);res=conn.getresponse();x=json.loads(res.read())
                self.assertEqual(res.status,200);self.assertEqual(len(x['portfolios']),3);self.assertEqual(len(x['wallets']),1)
                self.assertEqual([p['cash'] for p in x['portfolios']],[1000]*3)
                conn.request('GET','/api/backup',headers=headers);res=conn.getresponse();self.assertEqual(res.status,200);self.assertTrue(res.read().startswith(b'SQLite format 3'))
                conn.close()
            finally:server.shutdown();server.server_close()

if __name__=='__main__':unittest.main()
