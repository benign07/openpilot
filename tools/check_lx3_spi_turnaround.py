"""Same inert readiness model against installed, bounded and timing drivers."""
from pathlib import Path
import argparse,hashlib,json,subprocess
R=Path(__file__).resolve().parents[1]
def main():
 p=argparse.ArgumentParser();p.add_argument('--cxx',default='clang++');p.add_argument('--output',type=Path,required=True);args=p.parse_args()
 out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
 def run(cmd,name):
  r=subprocess.run(cmd,cwd=R,capture_output=True,timeout=90)
  (out/(name+'.log')).write_bytes(r.stdout+r.stderr)
  return r
 sources={'installed':'32ff4a1101fc522ce43d44e0989cd52862abc203','bounded':'cadd97de0d690db8540d1d35adb34e3e9af6516b','candidate':None}
 result={}
 for name,commit in sources.items():
  source=R/'selfdrive/pandad/spi.cc'
  if commit:
   source=out/(name+'.cc');source.write_bytes(subprocess.check_output(['git','show',commit+':selfdrive/pandad/spi.cc'],cwd=R))
  exe=out/name
  r=run([args.cxx,'-std=c++17','-O1','-g','-pthread','-Wall','-Wextra','-Wno-unused-parameter',
   '-fsanitize=undefined','-fno-sanitize-recover=all','-I'+str(R/'selfdrive/pandad/tests/spi_host_fixtures'),'-I'+str(R),
   '-DSPI_PRODUCTION_SOURCE="'+source.as_posix()+'"','-DEXPECT_SPI_DIAGNOSTICS',str(R/'selfdrive/pandad/tests/spi_turnaround_probe.cc'),'-o',str(exe)],name+'-compile')
  assert r.returncode==0,(name,r.stderr.decode(errors='replace'))
  results={}
  for case in ('no_delay','phase','header','both','interrupt'):
   r=run([str(exe),case],name+'-'+case)
   row=next(json.loads(s) for s in r.stdout.decode().splitlines() if s.startswith('{'))
   if name=='candidate' or case=='no_delay':
    assert r.returncode==0 and row['passed'] and row['transactions']==100,(name,case,row)
   elif name=='installed':
    assert r.returncode==42 and row['fixture_stop']=='unbounded SPI recovery/retry',(name,case,row)
   else:
    assert r.returncode==1 and not row['passed'] and row['transactions']==0,(name,case,row)
   results[case]={'exit_code':r.returncode,'result':row}
  result[name]={'commit':commit,'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'cases':results}
 record={'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=R).decode().strip(),
  'drivers':result,'UBSAN':True,'scope':'Linux same production sources and inert readiness thresholds; not a hardware diagnosis',
  'probe_sha256':hashlib.sha256((R/'selfdrive/pandad/tests/spi_turnaround_probe.cc').read_bytes()).hexdigest()}
 (out/'result.json').write_text(json.dumps(record,indent=2))
 print('PASS exact production SPI timing model: installed/bounded fail ready-time cases; candidate five cases each100transactions, including interrupted sleep; UBSAN; no hardware')
if __name__=='__main__':main()
