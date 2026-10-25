"""Original-device native profile; no device access, flash or CAN publisher.

baseline is an exact source comparison, not a recommended deployment.
observed adds only previously reviewed SPI bounds/retry guards/host counters.
turnaround differs from observed only by the stock-inspired ready intervals.
"""
from pathlib import Path
import argparse,hashlib,io,json,shutil,subprocess,tarfile
import os
from original_lx3_spi_variant import generate

ROOT=Path(__file__).resolve().parents[1]


def prepare(profile,out):
  assert profile in ('baseline','observed','turnaround')
  base=ROOT/'tools/original_lx3_transport';manifest=json.loads((base/'manifest.json').read_text())
  blob=(base/'sources.tar.gz').read_bytes();assert hashlib.sha256(blob).hexdigest()==manifest['archive_sha256']
  files={}
  with tarfile.open(fileobj=io.BytesIO(blob)) as tar:
    for item in tar:
      path=Path(item.name)
      assert item.isfile() and not path.is_absolute() and '..' not in path.parts
      raw=tar.extractfile(item).read();assert hashlib.sha256(raw).hexdigest()==manifest['build_source_hashes'][item.name]
      files[item.name]=raw
  assert set(files)==set(manifest['build_source_hashes'])
  original_spi=files['selfdrive/pandad/spi.cc']
  commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
  if profile!='baseline':
    # The entire rest of pandad is the original source profile, not HEAD's
    # newer authority/schema implementation.
    source=subprocess.check_output(['git','show',commit+':selfdrive/pandad/spi.cc'],cwd=ROOT).decode()
    files['selfdrive/pandad/spi.cc']=generate(source).encode()
    files['selfdrive/pandad/spi_diagnostics.h']=subprocess.check_output(['git','show',commit+':selfdrive/pandad/spi_diagnostics.h'],cwd=ROOT)
  files['cereal/include/c++.capnp']=files['opendbc_repo/opendbc/car/include/c++.capnp']
  out.mkdir(parents=True,exist_ok=False)
  for name,raw in files.items():
    path=out/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
  # Build machinery is explicit and separate from the protected runtime code.
  shutil.copytree(ROOT/'site_scons',out/'site_scons',ignore=shutil.ignore_patterns('__pycache__'))
  if os.name=='nt':shutil.copytree(out/'msgq_repo/msgq',out/'msgq')
  else:(out/'msgq').symlink_to('msgq_repo/msgq',target_is_directory=True)
  (out/'SConstruct').write_bytes((ROOT/'tools/lx3_original_transport.SConstruct').read_bytes())
  record={'source_commit':commit,'profile':profile,'installed_baseline_pandad':manifest['installed_pandad_sha256'],
    'working_tree_dirty':bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT)),
    'generator_sha256':hashlib.sha256((ROOT/'tools/original_lx3_spi_variant.py').read_bytes()).hexdigest(),
    'original_spi_sha256':hashlib.sha256(original_spi).hexdigest(),
    'files':{n:hashlib.sha256(raw).hexdigest() for n,raw in files.items()},
    'unchanged_original_native_sources':[n for n in manifest['build_source_hashes'] if files[n] and
      hashlib.sha256(files[n]).hexdigest()==manifest['build_source_hashes'][n]],
    'allowed_native_changes':[] if profile=='baseline' else ['selfdrive/pandad/spi.cc','selfdrive/pandad/spi_diagnostics.h'],
    'spi_ready_profile':1 if profile=='turnaround' else 0,
    'ready_profile_scope':'experimental HACK400us/CAN400us/post0xdc1000us; no control->control delay; parked cadence gate required',
    'vehicle_installed':False,'Panda_firmware_changed':False}
  changed={n for n in manifest['build_source_hashes'] if record['files'][n]!=manifest['build_source_hashes'][n]}
  assert changed==({'selfdrive/pandad/spi.cc'} if profile!='baseline' else set())
  (out/'profile.json').write_text(json.dumps(record,indent=2))
  return record


if __name__=='__main__':
  p=argparse.ArgumentParser(description=__doc__);p.add_argument('profile',choices=['baseline','observed','turnaround']);p.add_argument('--out',type=Path,required=True)
  a=p.parse_args();r=prepare(a.profile,a.out);print(json.dumps({'profile':a.profile,'verified_files':len(r['files']),'source_commit':r['source_commit']}))
