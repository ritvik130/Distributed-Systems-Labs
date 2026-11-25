#!/usr/bin/env python3
# Run from lab2/src on Linux, with the two Worker environment options enabled.
import collections
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.request

src = Path.cwd()
if not (src/'go.mod').is_file() or not (src/'main/mrworker.go').is_file():
    raise SystemExit('Run from lab2/src')
root = Path(tempfile.mkdtemp(prefix='mr-advanced-e2e-'))
print('Logs and files:', root, flush=True)
processes = []
inputs = {'a.txt':'cat dog cat\n','b.txt':'dog bird cat\n','c.txt':'bird fish dog\n'}
expected = dict(collections.Counter(' '.join(inputs.values()).split()))
plugin = root/'wc_e2e.go'
plugin.write_text('''package main
import ("6.5840/mr"; "os"; "path/filepath"; "strconv"; "strings"; "time")
func Map(filename, contents string) []mr.KeyValue {
    time.Sleep(600*time.Millisecond)
    if filepath.Base(filename)=="b.txt" {
        d, _ := time.ParseDuration(os.Getenv("MR_TEST_DELAY")); time.Sleep(d)
    }
    result := []mr.KeyValue{}
    for _, w := range strings.Fields(contents) { result=append(result,mr.KeyValue{Key:w,Value:"1"}) }
    return result
}
func Reduce(key string, values []string) string { return strconv.Itoa(len(values)) }
''')

def build():
    for cmd in [['go','build','-o',str(root/'coordinator'),'./main/mrcoordinator.go'],
                ['go','build','-o',str(root/'worker'),'./main/mrworker.go'],
                ['go','build','-buildmode=plugin','-o',str(root/'wc.so'),str(plugin)]]:
        subprocess.run(cmd,check=True)

def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1',0)); return s.getsockname()[1]

def wait(fn, label, seconds=40):
    end=time.monotonic()+seconds
    while time.monotonic()<end:
        value=fn()
        if value: return value
        time.sleep(.05)
    raise AssertionError('Timed out: '+label)

def kill(p):
    if p.poll() is None: p.kill()
    p.wait(timeout=5)

def start(binary,args,cwd,env,log):
    with (cwd/log).open('wb') as f:
        p=subprocess.Popen([str(root/binary),*map(str,args)],cwd=cwd,env={**os.environ,**env},stdout=f,stderr=subprocess.STDOUT)
    processes.append(p); return p

class Job:
    def __init__(self,name):
        self.base=root/name; self.cd=self.base/'coordinator'; self.cd.mkdir(parents=True)
        for n,content in inputs.items(): (self.cd/n).write_text(content)
        addr=f'127.0.0.1:{port()}'; self.url='http://'+addr
        self.env={'MR_COORDINATOR_HTTP_ADDR':addr,'MR_COORDINATOR_RPC_ADDR':addr,
                  'MR_COORDINATOR_PUBLIC_URL':self.url,'MR_COORDINATOR_STATE_FILE':str(self.cd/'coordinator-state.json')}
        self.workers=[]; self.starts=0
    def state(self):
        try: return json.loads((self.cd/'coordinator-state.json').read_text())
        except (FileNotFoundError,json.JSONDecodeError): return {}
    def maps(self): return self.state().get('MapTasks',[])
    def coordinator(self):
        self.starts+=1
        self.c=start('coordinator',list(inputs),self.cd,self.env,f'coordinator-{self.starts}.log')
        def ready():
            if self.c.poll() is not None: raise AssertionError('Coordinator exited; inspect log')
            try:
                with urllib.request.urlopen(self.url+'/inputs/0',timeout=.4) as r: return r.read().decode()==inputs['a.txt']
            except OSError: return False
        wait(ready,'Coordinator GET',10)
    def worker(self,name,delay='0s'):
        wd=self.base/name; wd.mkdir(); p=port(); addr=f'127.0.0.1:{p}'
        env={**self.env,'MR_WORKER_PORT':str(p),'MR_WORKER_PUBLIC_ADDR':addr,'MR_TEST_DELAY':delay}
        proc=start('worker',[root/'wc.so'],wd,env,'worker.log'); self.workers.append((proc,wd))
        def registered():
            if proc.poll() is not None: raise AssertionError(f'{name} exited; inspect log')
            return any(w['Address']==addr for w in self.state().get('Workers',{}).values())
        wait(registered,f'{name} registered at {addr}; check port/address configuration',10)
        return proc
    def first_map_done(self):
        m=self.maps(); return len(m)==3 and m[0]['State']=='completed' and m[1]['State']=='in_progress'
    def finish(self,clean):
        def done():
            s=self.state(); ts=s.get('ReduceTasks',[])
            if ts and all(t['State']=='completed' for t in ts): return s
            if self.c.poll() is not None: raise AssertionError('Coordinator exited prematurely')
            return False
        s=wait(done,'all Reduce tasks complete',90)
        files=list(self.cd.glob('mr-out-*')); assert len(files)==s['NReduce'],'Missing output partitions'
        actual={}
        for f in files:
            for line in f.read_text().splitlines():
                k,v=line.split(); assert k not in actual,'Duplicate key'; actual[k]=int(v)
        assert actual==expected,(actual,expected)
        assert not list(self.cd.glob('mr-[0-9]*')),'Intermediate files on Coordinator'
        for _,wd in self.workers:
            assert not list(wd.glob('mr-out-*')),'Final output on Worker'
            assert not any((wd/n).exists() for n in inputs),'Input copied to Worker directory'
        if clean: assert all(t['AttemptsIssued']==1 for t in s['MapTasks']+s['ReduceTasks']),'Unexpected duplicate attempts'
        self.c.wait(timeout=10); assert self.c.returncode==0
        for p,wd in self.workers:
            if p.poll()==-9: continue
            try: p.wait(timeout=5)
            except subprocess.TimeoutExpired: raise AssertionError(f'{wd.name} did not receive ExitTask')
            assert p.returncode==0,f'{wd.name} failed'
        return s

def normal():
    j=Job('01-normal'); j.coordinator(); j.worker('worker-1'); j.worker('worker-2')
    s=j.finish(True)
    owners={t['Attempts'][str(t['AcceptedAttempt'])]['WorkerID'] for t in s['MapTasks']}
    assert len(owners)>=2,'Both workers must produce Map output'
    print('PASS 1: normal distributed run; correct output; one attempt per task',flush=True)

def worker_crash():
    j=Job('02-worker-crash'); j.coordinator(); dead=j.worker('worker-1','30s')
    wait(j.first_map_done,'Map 0 accepted and Map 1 running')
    t=j.maps()[0]; owner=t['Attempts'][str(t['AcceptedAttempt'])]['WorkerID']; addr=j.state()['Workers'][str(owner)]['Address']
    with urllib.request.urlopen('http://'+addr+'/intermediate/'+t['OutputFiles'][0],timeout=3) as r:
        assert r.read()==(j.base/'worker-1'/t['OutputFiles'][0]).read_bytes()
    kill(dead); print('Killed Worker 1 after completed Map 0, during Map 1',flush=True)
    j.worker('worker-2'); j.worker('worker-3'); s=j.finish(False)
    m=s['MapTasks']; assert m[0]['AttemptsIssued']>=2,'Lost Map not recovered'; assert m[1]['AttemptsIssued']>=2,'Interrupted Map not retried'
    assert m[2]['AttemptsIssued']==1,'Unrelated Map recomputed'
    assert m[0]['Attempts'][str(m[0]['AcceptedAttempt'])]['WorkerID']!=owner
    print('PASS 2: worker crash; lost and interrupted Maps recovered; healthy Map preserved',flush=True)

def coordinator_crash():
    j=Job('03-coordinator-crash'); j.coordinator(); j.worker('worker-1','4s')
    wait(j.first_map_done,'Map 0 accepted and Map 1 running')
    before=j.state(); old=before['MapTasks'][1]; aid=old['AttemptsIssued']; owner=old['Attempts'][str(aid)]['WorkerID']
    kill(j.c); print('Killed Coordinator; Worker stays alive and retries old report',flush=True)
    time.sleep(5); j.coordinator(); j.worker('worker-2'); s=j.finish(True)
    t=s['MapTasks'][1]; assert t['AcceptedAttempt']==aid and t['Attempts'][str(aid)]['WorkerID']==owner
    assert s['MapTasks'][0]['AcceptedAttempt']==before['MapTasks'][0]['AcceptedAttempt']
    print('PASS 3: coordinator restarted; old attempt accepted; no unnecessary repeats',flush=True)

try:
    build(); normal(); worker_crash(); coordinator_crash()
    print('ALL 3 SCENARIOS PASSED. Logs:',root,flush=True)
finally:
    for p in processes: kill(p)
