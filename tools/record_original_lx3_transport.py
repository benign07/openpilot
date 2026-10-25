"""Bind an original-native ARM artifact to its exact sealed source profile."""
from pathlib import Path
import argparse,hashlib,json,re,subprocess,tempfile,shlex

ROOT=Path(__file__).resolve().parents[1]
sha=lambda data:hashlib.sha256(data).hexdigest()


def main():
  p=argparse.ArgumentParser(description=__doc__);p.add_argument('directory',type=Path);args=p.parse_args()
  directory=args.directory.resolve();profile=json.loads((directory/'profile.json').read_text())
  baseline=json.loads((ROOT/'tools/original_lx3_transport/manifest.json').read_text())
  assert not profile['working_tree_dirty'],'ARM artifact must bind a clean source commit'
  for name,digest in profile['files'].items():assert sha((directory/name).read_bytes())==digest,name
  binary=directory/'selfdrive/pandad/pandad'
  run=lambda *args:subprocess.check_output(args,text=True,cwd=directory)
  header=run('readelf','-h',str(binary));dynamic=run('readelf','-d',str(binary))
  versions=run('readelf','--version-info',str(binary));deps=run('ldd',str(binary))
  assert 'AArch64' in header and 'not found' not in deps
  assert 'RPATH' not in dynamic and 'RUNPATH' not in dynamic
  tags=set(re.findall(r'GLIBC(?:XX)?_[0-9]+(?:\.[0-9]+)+|CXXABI_[0-9]+(?:\.[0-9]+)+|GCC_[0-9]+(?:\.[0-9]+)+',versions))
  assert tags<=set(baseline['installed_elf_required_tags']),('new ABI',tags-set(baseline['installed_elf_required_tags']))
  needed=[line.split('[')[1].split(']')[0] for line in dynamic.splitlines() if '(NEEDED)' in line]
  assert set(needed)<=set(baseline['installed_elf_needed']),needed
  protocol=run(str(directory/'selfdrive/pandad/tests/test_pandad_canprotocol'))
  assert 'All tests passed' in protocol,protocol
  out=directory/'evidence';out.mkdir(exist_ok=True)
  compiler=run('clang++','--version');comment=run('readelf','-p','.comment',str(binary))
  for name,text in {'elf-header':header,'elf-dynamic':dynamic,'elf-versions':versions,'ldd':deps,'can-protocol':protocol,
                    'builder-compiler':compiler,'elf-comment':comment}.items():
    (out/(name+'.log')).write_text(text)
  raw=binary.read_bytes()
  if profile['profile']!='baseline':
    assert b'SPI_HOST_STATS' in raw and b'SPI_PROFILE_STATS' in raw
    assert profile['files']['selfdrive/pandad/spi.cc'].encode() in raw
    assert profile['profile'].encode() in raw
  object_hashes={}
  with tempfile.TemporaryDirectory() as tmp:
    for obj in sorted(directory.rglob('*.o')):
      if obj.name=='spi.o':continue
      stripped=Path(tmp)/'object.o'
      run('objcopy','--strip-debug',str(obj),str(stripped))
      object_hashes[obj.relative_to(directory).as_posix()]=sha(stripped.read_bytes())
  assert 'selfdrive/pandad/pandad.o' in object_hashes
  log=(directory/'build.log').read_text()
  commands=[shlex.split(line) for line in log.splitlines() if re.match(r'clang(?:\+\+)? -o ',line) and ' -c ' in line]
  assert commands and all('-D__TICI__' in command for command in commands)
  (out/'compile_commands.json').write_text(json.dumps(commands,indent=2))
  for prefix in ('common','cereal','panda','selfdrive','msgq_repo'):
    assert str(ROOT/prefix) not in log,'latest checkout source leaked into original build'
  runtime=out/'pandad.runtime';run('objcopy','--strip-debug',str(binary),str(runtime))
  record={**profile,'artifact_sha256':sha(raw),'bytes':len(raw),'needed':needed,'required_abi_tags':sorted(tags),
    'schema_sha256':profile['files']['cereal/car.capnp'],'protocol_pass':True,
    'not_device_qualification':True,'installation_not_performed':True,
    'non_spi_object_sha256_stripped_debug':object_hashes,
    'runtime_sha256':sha(runtime.read_bytes()),'runtime_bytes':runtime.stat().st_size,
    'baseline_rebuild_is_source_exact_not_claimed_binary_identical':profile['profile']=='baseline',
    'log_sha256':{f.name:sha(f.read_bytes()) for f in out.glob('*.log')}}
  (out/'artifact.json').write_text(json.dumps(record,indent=2))
  print('PASS isolated original native ARM source/ABI/schema/CAN protocol',profile['profile'],sha(raw))


if __name__=='__main__':main()
