"""Cancellation is scoped to the owning dialog, not saves or other panels."""
import sys, unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent))
from smoke import BrowserFlows, expect

class ModalRequests(BrowserFlows):
    def prepare(self):
        self.script('ts-utils.js')
        self.page.evaluate('''() => {
          document.body.innerHTML='<div id="one" class="open"></div><div id="two" class="open"></div>';
          window.requests=[];
          window.fetch=(url,opts={})=>new Promise((resolve,reject)=>{
            const row={url,aborted:false,resolve:()=>resolve(new Response(JSON.stringify({results:[]})))};
            requests.push(row);
            opts.signal?.addEventListener('abort',()=>{row.aborted=true;reject(new DOMException('Aborted','AbortError'))});
          });
          window.load=(owner,url)=>tsModalFetch(owner,url).then(r=>r.json()).catch(e=>e.name);
        }''')

    def test_close_scopes_and_reopen(self):
        self.prepare()
        self.page.evaluate("void load('one','/api/search?q=a');void load('two','/api/search?q=b');void fetch('/api/save',{method:'POST'});")
        self.page.evaluate("document.querySelector('#one').classList.remove('open')")
        self.page.wait_for_function('requests[0].aborted')
        self.assertEqual(self.page.evaluate('requests.map(r=>r.aborted)'),[True,False,False])
        self.page.evaluate("document.querySelector('#one').classList.add('open');void load('one','/api/search?q=c')")
        self.page.wait_for_function('requests.length===4')
        self.assertFalse(self.page.evaluate('requests[3].aborted'))
        self.page.evaluate("document.querySelector('#one').style.display='none'")
        self.page.wait_for_function('requests[3].aborted')
        self.assertEqual(self.page.evaluate("load('one','/api/search?q=late')"),'AbortError')
        self.assertEqual(self.page.evaluate('requests.length'),4)

    def test_parallel_reads_and_dialog_close(self):
        self.prepare()
        self.page.evaluate("void load('one','/api/frames?path=a');void load('one','/api/frames?path=b')")
        self.assertEqual(self.page.evaluate('requests.map(r=>r.aborted)'),[False,False])
        self.page.evaluate("tsCancelModalLoads('one')")
        self.assertEqual(self.page.evaluate('requests.map(r=>r.aborted)'),[True,True])
        self.page.evaluate("window.d=document.createElement('dialog');document.body.append(d);d.showModal();void load(d,'/api/tags')")
        self.page.evaluate('d.close()')
        self.page.wait_for_function('requests[2].aborted')

    def test_studio_and_movie_close(self):
        self.prepare()
        self.script('studio-popup.js');self.script('movie-popup.js')
        self.page.evaluate("void openStudioPopup({libraryRowId:3,name:'Studio'})")
        self.page.wait_for_function("requests.some(r=>r.url.includes('entity-panel'))")
        self.page.evaluate('closeStudioPopup()')
        self.assertTrue(self.page.evaluate("requests.filter(r=>r.url.includes('entity-panel')).every(r=>r.aborted)"))
        self.page.evaluate("void openMoviePopup('movie')")
        self.page.wait_for_function("requests.some(r=>r.url.includes('/api/movies/tpdb/'))")
        self.page.evaluate('closeMoviePopup()')
        self.assertTrue(self.page.evaluate("requests.filter(r=>r.url.includes('/api/movies/tpdb/')).every(r=>r.aborted)"))

    def test_filmstrip_polling_stops_on_close(self):
        self.page.goto('http://top-shelf.test/static/queue.html')
        self.page.clock.install()
        self.handlers['/api/queue/thumbs']=lambda _:(200,{'ready':False,'generating':True,'thumbs':[]})
        self.page.evaluate("void openQueueFilmstrip('pending.mp4')")
        self.page.wait_for_function("document.getElementById('qFilmstripStatus').textContent.includes('Generating')")
        self.page.evaluate('closeQueueFilmstrip()')
        count=sum(path=='/api/queue/thumbs' for _,path,_ in self.calls)
        self.page.clock.fast_forward(15000)
        self.assertEqual(sum(path=='/api/queue/thumbs' for _,path,_ in self.calls),count)

    def test_tag_picker_close(self):
        self.prepare();self.script('discover-tags.js')
        self.page.evaluate('void openTpdbTagPicker()')
        self.page.wait_for_function("requests.some(r=>r.url==='/api/discover/tpdb-tags')")
        self.page.get_by_role('button',name='Cancel',exact=True).click()
        self.page.wait_for_function('requests.every(r=>r.aborted)')

if __name__=='__main__':
    names=[n for n in ModalRequests.__dict__ if n.startswith('test_')]
    result=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(ModalRequests(n) for n in names))
    sys.exit(not result.wasSuccessful())
