#!/usr/bin/env python3
"""Validate Advanced MapReduce locally or on three EC2 instances via SSH."""
import argparse, collections, datetime, io, json, os, shlex, socket
import statistics, subprocess, sys, tarfile, tempfile, time, traceback, urllib.request
from pathlib import Path

INPUTS = {'a.txt':'cat dog cat key20\n','b.txt':'dog bird cat key20\n','c.txt':'bird fish dog key20\n'}
EXPECTED = dict(collections.Counter(' '.join(INPUTS.values()).split()))
# key20 hashes to partition 0 with ihash and the starter's 10 reducers.
PLUGIN = '''package main
import ("6.5840/mr"; "os"; "path/filepath"; "strconv"; "strings"; "time")
func delay(name string) { d,_:=time.ParseDuration(os.Getenv(name)); time.Sleep(d) }
func Map(filename,contents string) []mr.KeyValue {
    time.Sleep(600*time.Millisecond)
    if filepath.Base(filename)=="b.txt" { delay("MR_TEST_MAP_DELAY") }
    out:=[]mr.KeyValue{}
    for _,w:=range strings.Fields(contents) { out=append(out,mr.KeyValue{Key:w,Value:"1"}) }
    return out
}
func Reduce(key string,values []string) string {
    if key=="key20" { delay("MR_TEST_REDUCE_DELAY") }
    return strconv.Itoa(len(values))
}
'''

def check(ok,msg):
    if not ok: raise AssertionError(msg)

def wait(fn,label,seconds=90):
    end=time.monotonic()+seconds
    while time.monotonic()<end:
        v=fn()
        if v: return v
        time.sleep(.15)
    raise AssertionError('Timed out: '+label)

def agent():
    # All subprocess output goes to logs, not the JSON control channel.
    root=Path(tempfile.mkdtemp(prefix='mr-validation-node-')); ps={}; builds={}
    def run(cmd,cwd=None):
        p=subprocess.run(cmd,cwd=cwd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        if p.returncode: raise RuntimeError(f'{cmd}:\n{p.stdout.decode(errors="replace")}')
        return p.stdout
    def dispatch(op,**a):
        if op=='info': return dict(root=str(root),host=socket.gethostname(),go=run(['go','version']).decode().strip())
        if op=='build':
            src=Path(os.path.expanduser(os.path.expandvars(a['src']))).resolve()
            out=root/('build-'+a['version']); out.mkdir()
            if a.get('ref'):
                repo=run(['git','rev-parse','--show-toplevel'],src).decode().strip()
                prefix=run(['git','rev-parse','--show-prefix'],src).decode().strip()
                data=run(['git','archive',a['ref'],'--',prefix or '.'],repo)
                tree=out/'tree'; tree.mkdir()
                with tarfile.open(fileobj=io.BytesIO(data)) as t:
                    # The archive comes from the user's own repository.
                    if sys.version_info >= (3,12): t.extractall(tree,filter='data')
                    else: t.extractall(tree)
                src=tree/prefix
            check((src/'go.mod').is_file(),f'Not lab2/src: {src}')
            plugin=out/'wc_validation.go'; plugin.write_text(PLUGIN)
            for cmd in [['go','build','-o',str(out/'coordinator'),'./main/mrcoordinator.go'],
                        ['go','build','-o',str(out/'worker'),'./main/mrworker.go'],
                        ['go','build','-buildmode=plugin','-o',str(out/'wc.so'),str(plugin)]]: run(cmd,src)
            builds[a['version']]=out; return str(out)
        if op=='port':
            with socket.socket() as s: s.bind(('127.0.0.1',0)); return s.getsockname()[1]
        if op=='prepare':
            d=root/a['folder']; d.mkdir(parents=True)
            for name,content in a['files'].items(): (d/name).write_text(content)
            return str(d)
        if op=='start':
            d=root/a['folder']; b=builds[a['version']]; binary=a['binary']
            args=list(INPUTS) if binary=='coordinator' else [str(b/'wc.so')]
            with (d/a['log']).open('wb') as log:
                p=subprocess.Popen([str(b/binary),*args],cwd=d,env={**os.environ,**a['env']},stdout=log,stderr=subprocess.STDOUT)
            ps[p.pid]=p; return p.pid
        if op=='status': return ps[a['pid']].poll()
        if op=='kill':
            p=ps[a['pid']]
            if p.poll() is None: p.kill()
            return p.wait(timeout=5)
        if op=='state':
            try: return json.loads((root/a['folder']/'coordinator-state.json').read_text())
            except FileNotFoundError: return {}
        if op=='get':
            try:
                with urllib.request.urlopen(a['url'],timeout=2) as r: return r.read().decode()
            except OSError: return None
        if op=='collect':
            d=root/a['folder']; files={}
            for p in d.iterdir():
                if p.is_file() and (p.name.endswith('.log') or p.name.startswith('mr-out-') or p.name=='coordinator-state.json'):
                    files[p.name]=p.read_text(errors='replace')
            return dict(files=files,listing=sorted(p.name for p in d.iterdir()))
        raise ValueError(op)
    try:
        for line in sys.stdin:
            try: print(json.dumps(dict(ok=True,result=dispatch(**json.loads(line)))),flush=True)
            except Exception: print(json.dumps(dict(ok=False,error=traceback.format_exc())),flush=True)
    finally:
        for p in ps.values():
            if p.poll() is None: p.kill()
            p.wait()

class Node:
    def __init__(self,name,target,key):
        self.name=name
        if target:
            cmd=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10']
            if key: cmd+=['-i',os.path.expanduser(key)]
            cmd += [target,'python3 -u -c '+shlex.quote(Path(__file__).read_text())+' --agent']
        else: cmd=[sys.executable,'-u',str(Path(__file__).resolve()),'--agent']
        self.p=subprocess.Popen(cmd,stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True,bufsize=1)
        print(name,self.call('info'),flush=True)
    def call(self,op,**a):
        self.p.stdin.write(json.dumps(dict(op=op,**a))+'\n'); self.p.stdin.flush()
        line=self.p.stdout.readline(); check(line,f'{self.name}: agent disconnected')
        r=json.loads(line)
        if not r['ok']: raise RuntimeError(self.name+': '+r['error'])
        return r['result']
    def close(self):
        self.p.stdin.close()
        try: self.p.wait(timeout=10)
        except subprocess.TimeoutExpired: self.p.kill(); self.p.wait()

def owner(t,aid=None): return t['Attempts'][str(t['AcceptedAttempt'] if aid is None else aid)]['WorkerID']
def assigned(t,aid): return datetime.datetime.fromisoformat(t['Attempts'][str(aid)]['AssignedAt'].replace('Z','+00:00'))

class Job:
    def __init__(self,suite,name,version):
        self.s=suite; self.name=name; self.version=version; self.nodes=suite.nodes
        self.cn=self.nodes[0]; self.processes=[]; self.workers={}; self.starts=0; self.started=None
        self.folders=[name+'/'+n.name for n in self.nodes]
        cd=self.cn.call('prepare',folder=self.folders[0],files=INPUTS)
        for i in (1,2): self.nodes[i].call('prepare',folder=self.folders[i],files={})
        p=8081 if suite.remote else self.cn.call('port'); self.addr=f'{suite.ips[0]}:{p}'; self.url='http://'+self.addr
        self.env={'MR_COORDINATOR_HTTP_ADDR':f':{p}','MR_COORDINATOR_RPC_ADDR':self.addr,
                  'MR_COORDINATOR_PUBLIC_URL':self.url,'MR_COORDINATOR_STATE_FILE':cd+'/coordinator-state.json'}
    def state(self): return self.cn.call('state',folder=self.folders[0])
    def task(self,kind,i):
        ts=self.state().get(kind+'Tasks',[]); return ts[i] if len(ts)>i else {}
    def start(self,i,binary,env,log):
        pid=self.nodes[i].call('start',folder=self.folders[i],version=self.version,binary=binary,env=env,log=log)
        p=(self.nodes[i],pid); self.processes.append(p); return p
    def status(self,p): return p[0].call('status',pid=p[1])
    def alive(self,p): return self.status(p) is None
    def kill(self,p): return p[0].call('kill',pid=p[1])
    def coordinator(self):
        self.starts+=1; self.c=self.start(0,'coordinator',self.env,f'coordinator-{self.starts}.log')
        def ready():
            check(self.alive(self.c),'Coordinator exited before ready')
            return self.cn.call('get',url=self.url+'/inputs/0')==INPUTS['a.txt']
        wait(ready,'Coordinator HTTP ready',20)
    def worker(self,i,map_delay='0s',reduce_delay='0s'):
        n=self.nodes[i]; p=5001 if self.s.remote else n.call('port'); addr=f'{self.s.ips[i]}:{p}'
        env={**self.env,'MR_WORKER_PORT':str(p),'MR_WORKER_PUBLIC_ADDR':addr,
             'MR_TEST_MAP_DELAY':map_delay,'MR_TEST_REDUCE_DELAY':reduce_delay}
        if self.started is None: self.started=time.monotonic()
        proc=self.start(i,'worker',env,'worker.log'); self.workers[i]=proc
        def registered():
            check(self.alive(proc),f'Worker {i} exited before registration')
            for wid,w in self.state().get('Workers',{}).items():
                if w['Address']==addr: return int(wid)
        return wait(registered,f'Worker {i} registered',20)
    def b_running(self): return self.task('Map',0).get('State')=='completed' and self.task('Map',1).get('State')=='in_progress'
    def collect(self,i): return self.nodes[i].call('collect',folder=self.folders[i])
    def finish(self,duplicates=(),strict=True):
        def done():
            s=self.state(); ts=s.get('ReduceTasks',[])
            if ts and all(t['State']=='completed' for t in ts): return s
            check(self.alive(self.c),'Coordinator exited prematurely')
        s=wait(done,'all Reduce tasks complete',120); elapsed=time.monotonic()-self.started
        c=self.collect(0); outputs={k:v for k,v in c['files'].items() if k.startswith('mr-out-')}
        check(set(outputs)=={f'mr-out-{i}' for i in range(s['NReduce'])},'Missing/extra final files')
        actual={}
        for content in outputs.values():
            for line in content.splitlines():
                k,v=line.split(); check(k not in actual,'Duplicate output key'); actual[k]=int(v)
        check(actual==EXPECTED,f'Wrong output {actual}, expected {EXPECTED}')
        check(not any(n.startswith('mr-') and not n.startswith('mr-out-') for n in c['listing']),'Intermediate on Coordinator')
        for i in (1,2):
            check(not any(n.startswith('mr-out-') or n in INPUTS for n in self.collect(i)['listing']),'Final output or input on Worker')
        for kind in ('Map','Reduce'):
            for i,t in enumerate(s[kind+'Tasks']):
                if (kind,i) in duplicates: check(t['AttemptsIssued']>=2,f'{kind} {i} not retried')
                elif strict: check(t['AttemptsIssued']==1,f'Unexpected duplicate {kind} {i}')
        wait(lambda:self.status(self.c)==0,'Coordinator clean exit',10)
        return s,elapsed
    def cleanup(self):
        for p in self.processes:
            try:
                if self.alive(p): self.kill(p)
            except Exception as e: print('Cleanup warning:',e,flush=True)
        for i,n in enumerate(self.nodes):
            try:
                data=self.collect(i); d=self.s.results/self.name/n.name; d.mkdir(parents=True,exist_ok=True)
                (d/'listing.json').write_text(json.dumps(data['listing'],indent=2))
                for name,text in data['files'].items(): (d/name).write_text(text)
            except Exception as e: print('Archive warning:',e,flush=True)

class Suite:
    def __init__(self,args):
        self.remote=args.mode=='aws'; self.ips=args.private_ips if self.remote else ['127.0.0.1']*3
        self.nodes=[]; self.rows=[]; self.results=Path(args.results).resolve(); self.results.mkdir(parents=True)
        try:
            for name,target in zip(('coordinator','worker-1','worker-2'),args.hosts if self.remote else [None]*3): self.nodes.append(Node(name,target,args.ssh_key))
        except Exception: self.close(); raise
    def close(self):
        for n in self.nodes: n.close()
    def build(self,args):
        for n in self.nodes:
            print('Building new on',n.name,flush=True); n.call('build',version='new',src=args.src)
            if args.only!='functional':
                print('Building baseline on',n.name,flush=True); n.call('build',version='old',src=args.baseline_src or args.src,ref=None if args.baseline_src else args.baseline_ref)
    def run(self,name,fn,version='new'):
        print('\n===',name,'===',flush=True); j=Job(self,name,version)
        try:
            r=fn(j); self.rows.append(dict(scenario=name,version=version,status='PASS',**r)); print('PASS',name,r,flush=True); return r
        except Exception as e: self.rows.append(dict(scenario=name,version=version,status='FAIL',error=str(e))); raise
        finally: j.cleanup(); (self.results/'results.json').write_text(json.dumps(self.rows,indent=2))

def normal(j):
    j.coordinator(); a=j.worker(1); b=j.worker(2); s,t=j.finish()
    check({owner(x) for x in s['MapTasks']}=={a,b},'Both Workers must produce Map outputs')
    for i in (1,2): wait(lambda i=i:j.status(j.workers[i])==0,'Worker clean exit',6)
    return dict(job_seconds=round(t,3),map_attempts=3,reduce_attempts=s['NReduce'])

def worker_crash(j):
    j.coordinator(); a=j.worker(1,map_delay='30s'); wait(j.b_running,'Map 0 completed, Map 1 running')
    m=j.task('Map',0); addr=j.state()['Workers'][str(a)]['Address']
    check(j.nodes[2].call('get',url=f'http://{addr}/intermediate/{m["OutputFiles"][0]}') is not None,'Peer download failed')
    j.kill(j.workers[1]); killed=time.monotonic(); print('Killed Worker 1 after Map 0, during Map 1',flush=True)
    b=j.worker(2); wait(lambda:j.task('Map',1).get('AttemptsIssued',0)>=2,'interrupted Map reassign',15); gap=time.monotonic()-killed
    check(gap<=10,'Interrupted Map reassign exceeded 10s (driver-observed)')
    s,t=j.finish([('Map',0),('Map',1)],strict=False)
    check(s['MapTasks'][2]['AttemptsIssued']==1,'Healthy Map recomputed')
    check(all(owner(s['MapTasks'][i])==b for i in (0,1)),'Lost Maps not recovered by survivor')
    return dict(job_seconds=round(t,3),reassignment_seconds=round(gap,3),map_attempts=[x['AttemptsIssued'] for x in s['MapTasks']])

def coordinator_crash(j):
    j.coordinator(); a=j.worker(1,map_delay='4s'); wait(j.b_running,'Map 0 completed, Map 1 running')
    before=j.state(); aid=before['MapTasks'][1]['AttemptsIssued']; j.kill(j.c)
    print('Killed Coordinator; Worker continues',flush=True); time.sleep(5)
    check(j.alive(j.workers[1]),'Worker exited during Coordinator outage'); j.coordinator(); j.worker(2)
    s,t=j.finish(); check(owner(s['MapTasks'][1])==a and s['MapTasks'][1]['AcceptedAttempt']==aid,'Old attempt not accepted')
    check(s['MapTasks'][0]['AcceptedAttempt']==before['MapTasks'][0]['AcceptedAttempt'],'Completed Map lost')
    return dict(job_seconds=round(t,3),old_attempt_accepted=True)

def straggler(j,kind='Map'):
    i=1 if kind=='Map' else 0
    j.coordinator(); a=j.worker(1,map_delay='22s' if kind=='Map' else '0s',reduce_delay='22s' if kind=='Reduce' else '0s')
    wait(j.b_running if kind=='Map' else lambda:j.task('Reduce',0).get('State')=='in_progress','slow attempt assigned')
    b=j.worker(2); wait(lambda:j.task(kind,i).get('AttemptsIssued',0)>=2,'backup assigned',30)
    check(j.alive(j.workers[1]),'Slow Worker crashed: not speculation'); s,t=j.finish([(kind,i)])
    task=s[kind+'Tasks'][i]; check(task['AttemptsIssued']==2 and owner(task,1)==a and owner(task)==b and task['AcceptedAttempt']==2,'Wrong speculative winner or extra copies')
    gap=(assigned(task,2)-assigned(task,1)).total_seconds()
    logs=j.collect(0)['files']; check(f'Worker {a} is unreachable' not in '\n'.join(logs.values()),'Slow Worker declared unreachable')
    # The losing attempt may still be executing when the Coordinator exits.
    return dict(job_seconds=round(t,3),backup_after_seconds=round(gap,3),winning_attempt=2)

def main():
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('--mode',choices=['local','aws'],default='local')
    p.add_argument('--src',default='.',help='lab2/src path on each host'); p.add_argument('--hosts',nargs=3); p.add_argument('--private-ips',nargs=3); p.add_argument('--ssh-key')
    p.add_argument('--baseline-ref',default='5e3c6dc'); p.add_argument('--baseline-src',help='Alternative pre-2.3 source directory on each host')
    p.add_argument('--repeats',type=int,default=3); p.add_argument('--only',choices=['all','functional','benchmark'],default='all')
    p.add_argument('--results',default='mr-validation-'+datetime.datetime.now().strftime('%Y%m%d-%H%M%S')); a=p.parse_args()
    if a.mode=='aws' and (not a.hosts or not a.private_ips or len(set(a.hosts))!=3 or len(set(a.private_ips))!=3): p.error('AWS requires three distinct hosts and private IPs')
    if a.repeats<1: p.error('repeats must be positive')
    s=Suite(a)
    try:
        s.build(a)
        if a.only!='benchmark':
            for name,fn in [('01-normal',normal),('02-worker-crash',worker_crash),('03-coordinator-crash',coordinator_crash),('04-map-straggler',straggler),('05-reduce-straggler',lambda j:straggler(j,'Reduce'))]: s.run(name,fn)
        if a.only!='functional':
            samples={'old':[],'new':[]}
            for i in range(a.repeats):
                for v in (('old','new') if i%2==0 else ('new','old')): samples[v].append(s.run(f'benchmark-{i+1:02d}-{v}',straggler,v))
            med={v:statistics.median(r['job_seconds'] for r in rows) for v,rows in samples.items()}; gaps={v:statistics.median(r['backup_after_seconds'] for r in rows) for v,rows in samples.items()}
            summary=dict(repeats_per_scheduler=a.repeats,median_job_seconds=med,median_backup_after_seconds=gaps,speedup=med['old']/med['new'],reduction_percent=100*(1-med['new']/med['old']))
            (s.results/'benchmark.json').write_text(json.dumps(summary,indent=2)); print('\nBENCHMARK:',json.dumps(summary,indent=2),flush=True)
            check(med['new']<med['old'],'No median job-time improvement; inspect results')
        print('\nALL REQUESTED SCENARIOS PASSED:',s.results,flush=True)
    finally: s.close()

if __name__=='__main__':
    if sys.argv[1:]==['--agent']: agent()
    else: main()
