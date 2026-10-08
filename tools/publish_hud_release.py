"""Publish a tested, signed source release to the existing HUD channel.

The private key/spec stay outside Git. No device writes or CAN transmission.
Only explicit --publish updates GitHub. Native/firmware updates use a separate
installer; the source updater's allowlist and before-hash checks are retained.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from openpilot.selfdrive.carrot.hud_update import core
from tools.hud_release import build

CHANNEL = 'hud-device-updates-20260928'

def git(*args, data=None, env=None):
  return subprocess.check_output(['git', *args], cwd=ROOT, input=data,
                                 env=env, stderr=subprocess.PIPE)

def commit_files(parent, files, message):
  # A private temporary index preserves the user's checkout and staging area.
  with tempfile.TemporaryDirectory(prefix='hud-publish-index-') as temp:
    env = {**os.environ, 'GIT_INDEX_FILE': str(Path(temp)/'index')}
    git('read-tree', parent, env=env)
    for name, blob in files.items():
      digest = git('hash-object', '-w', '--stdin', data=blob).decode().strip()
      git('update-index', '--add', '--cacheinfo', '100644', digest, name, env=env)
    tree = git('write-tree', env=env).decode().strip()
    return git('commit-tree', tree, '-p', parent, data=(message+'\n').encode()).decode().strip()

def main():
  p = argparse.ArgumentParser(description=__doc__)
  p.add_argument('--spec', type=Path, required=True)
  p.add_argument('--private-key', type=Path, required=True)
  p.add_argument('--source-commit', required=True)
  p.add_argument('--verification', type=Path, required=True,
                 help='Public Markdown evidence/limitations for this release')
  p.add_argument('--out', type=Path, required=True)
  p.add_argument('--publish', action='store_true')
  args = p.parse_args()
  if args.private_key.resolve().is_relative_to(ROOT.resolve()):
    raise ValueError('Signing key must stay outside Git')
  spec = core.load(args.spec)
  raw, template = build(ROOT, spec, args.private_key, args.source_commit)
  args.out.mkdir(parents=True, exist_ok=True)
  filename = template['release_id']+'.json'
  dest = args.out/filename
  if dest.exists() and dest.read_bytes()!=raw:
    raise ValueError('Release ID content must not be replaced')
  core.atomic(dest, raw, 0o644)
  core.save(args.out/(template['release_id']+'.index-template.json'), template)
  if not args.publish:
    print(json.dumps({'prepared':True,'release_id':template['release_id'],
                      'bundle_sha256':core.sha(raw),'published':False}))
    return
  git('fetch', 'origin', CHANNEL)
  parent = git('rev-parse', 'FETCH_HEAD').decode().strip()
  old = json.loads(git('show', parent+':updates/hud/latest.json'))
  if spec['sequence'] <= old['sequence']:
    raise ValueError('Published sequence must increase')
  existing = git('ls-tree', '-r', '--name-only', parent,
                 'updates/hud/bundles/'+filename).strip()
  if existing:
    raise ValueError('A published release ID cannot be reused')
  bundle_commit = commit_files(parent, {'updates/hud/bundles/'+filename:raw},
                               'Publish signed '+spec['release_id']+' bundle')
  index = {**template,'bundle_commit':bundle_commit}
  core.validate_index(index)
  readme = git('show', parent+':updates/hud/README.md').decode()
  note = ('\n\n## Latest verified publication\n\n'+
          f"[{spec['release_id']}](releases/{spec['release_id']}.md) / sequence {spec['sequence']}. "
          'The source branch alone is not an OTA release. Publish the signed bundle, '
          'pinned index, change notes and verification record together. '
          'Application remains parked and hash-bound.\n')
  record = args.verification.read_bytes()
  final = commit_files(bundle_commit, {
    'updates/hud/latest.json':core.canonical(index),
    'updates/hud/releases/'+spec['release_id']+'.md':record,
    'updates/hud/README.md':(readme+note).encode()},
    'Select '+spec['release_id']+' for HUD updates')
  git('push', 'origin', final+':refs/heads/'+CHANNEL)
  # Read the immutable pushed objects before accepting the moving public index.
  for path, expected in [('updates/hud/bundles/'+filename,raw),
                         ('updates/hud/latest.json',core.canonical(index))]:
    url = core.RAW+(bundle_commit if 'bundles/' in path else final)+'/'+path
    with urlopen(Request(url,headers={'Cache-Control':'no-cache'}),timeout=30) as r:
      actual=r.read(core.LIMIT+1)
    if actual != expected: raise ValueError('Published bytes mismatch: '+path)
  public = json.loads((args.private_key.parent/'release-public.json').read_text())['public_key']
  release = core.verify_bundle(raw,index,public)
  tag = spec['release_id']
  git('tag','-a',tag,final,'-m',tag+' signed source update')
  git('push','origin','refs/tags/'+tag)
  result={'release_id':tag,'sequence':spec['sequence'],'source_commit':release['source_commit'],
          'bundle_commit':bundle_commit,'channel_commit':final,
          'bundle_sha256':core.sha(raw),'bundle_bytes':len(raw),
          'remote_signature_and_bytes_verified':True,'files':[
            {k:row[k] for k in ['path','before','sha256','bytes']} for row in release['files']]}
  core.save(args.out/'publication.json',result)
  print(json.dumps(result))

if __name__=='__main__': main()
