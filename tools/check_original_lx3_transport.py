"""Same inert SPI fixture against sealed original, guarded and turnaround profiles."""
from pathlib import Path
import argparse,hashlib,json,os,subprocess,re

ROOT=Path(__file__).resolve().parents[1]
CASES=('good','oversized','request','recovery','nack','header_nack','checksum','header_stuck',
       'control_data_stuck','can_header_stuck','mixed_nack','disconnect_backpressure','boundary_ok','boundary_fail','intervals','burst','sleep','length_alternating')


def main():
  parser=argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--profiles',type=Path,required=True);parser.add_argument('--out',type=Path,required=True)
  parser.add_argument('--prefix',default='')
  args=parser.parse_args();out=args.out.resolve();out.mkdir(parents=True,exist_ok=True)
  fixture=ROOT/'selfdrive/pandad/tests/spi_turnaround_probe.cc'
  include=ROOT/'selfdrive/pandad/tests/spi_host_fixtures'
  summary={'scope':'inert actual driver, simulated time; no hardware/closed-loop qualification','profiles':{}}
  for name in ('baseline','observed','turnaround','baseline_noalign'):
    directory=args.profiles/(args.prefix+('baseline' if name=='baseline_noalign' else name))
    source=(directory/'selfdrive/pandad/spi.cc').resolve();exe=out/name
    flags=[] if name.startswith('baseline') else ['-DEXPECT_SPI_DIAGNOSTICS']
    if not name.startswith('baseline'):
      flags+=['-DEXPECT_SPI_PROFILE','-DLX3_SPI_READY_PROFILE='+('1' if name=='turnaround' else '0'),
        '-DLX3_SPI_SOURCE_SHA="'+hashlib.sha256(source.read_bytes()).hexdigest()+'"']
    if name=='turnaround':flags+=['-DEXPECT_SPI_TURNAROUND']
    # Preserve the full UBSAN failure first. This second original-only run
    # disables its already-reproduced misaligned-load check, to reach other
    # independent retry/length counterexamples without changing source bytes.
    if name=='baseline_noalign':flags+=['-fno-sanitize=alignment']
    command=['g++','-std=c++17','-O2','-g','-pthread','-Wall','-Wextra','-fsanitize=undefined',
      '-fno-sanitize-recover=all',*flags,'-I'+str(include),'-I'+str(directory.resolve()),'-I'+str(ROOT),
      '-DSPI_PRODUCTION_SOURCE="'+source.as_posix()+'"',str(fixture),'-o',str(exe)]
    build=subprocess.run(command,capture_output=True,text=True)
    (out/(name+'-compile.log')).write_text(build.stdout+build.stderr)
    assert build.returncode==0,(name,build.stderr[-4000:])
    results={}
    for case in (('good','request','header_stuck') if name.startswith('baseline') else CASES):
      run=subprocess.run([str(exe),case],capture_output=True,text=True)
      output=run.stdout+run.stderr;(out/(name+'-'+case+'.log')).write_text(output)
      results[case]={'returncode':run.returncode,'log_sha256':hashlib.sha256(output.encode()).hexdigest()}
      metrics=re.search(r'SPI_METRICS ([^\n]+)',output)
      if metrics:results[case]['metrics']={key:int(value) for key,value in (token.split('=') for token in metrics[1].split())}
      if not name.startswith('baseline'):assert run.returncode==0,(name,case,output)
      elif name=='baseline':assert run.returncode!=0 and 'misaligned address' in output,(name,case,output)
      elif case=='good':assert run.returncode==0,output
      else:assert run.returncode!=0,('original counterexample missing',case,output)
    summary['profiles'][name]={'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'cases':results}
  for case in CASES:
    observed=summary['profiles']['observed']['cases'][case].get('metrics')
    ready=summary['profiles']['turnaround']['cases'][case].get('metrics')
    if observed and ready:
      assert observed['attempts']==ready['attempts'],('ready policy changed retry attempt envelope',case,observed,ready)
      assert abs(observed['charged_us']-ready['charged_us'])<=1,('charged-time drift',case,observed,ready)
  summary['fixture_sha256']=hashlib.sha256(fixture.read_bytes()).hexdigest()
  (out/'result.json').write_text(json.dumps(summary,indent=2))
  print('PASS original counterexamples; observed/turnaround18 scenarios; same attempts/IO budget; inert, observed sleep N/A')


if __name__=='__main__':main()
