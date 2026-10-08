"""Build a signed, bounded source update from an already committed PC revision.

No remote mutations. Commit/push the bundle, then pin that commit in the index.
Keep the Ed25519 private key outside the repository.
"""
import argparse
import base64
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from openpilot.selfdrive.carrot.hud_update import core
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, PublicFormat, NoEncryption


def build(repo, spec, private_key, commit):
  # Read Git blobs, not working-tree bytes: a release always refers to reviewable source.
  source = subprocess.check_output(['git', 'rev-parse', '--verify', commit + '^{commit}'], cwd=repo, text=True).strip()
  key = Ed25519PrivateKey.from_private_bytes(private_key.read_bytes())
  release = {k: spec[k] for k in ('release_id', 'sequence', 'notes', 'car_fingerprint')}
  release.update(schema=1, source_commit=source, files=[])
  for row in spec['files']:
    core.checked_path(repo, row['path'])
    data = subprocess.check_output(['git', 'show', source + ':' + row['path']], cwd=repo)
    release['files'].append({'path': row['path'], 'before': row['before'], 'bytes': len(data),
                            'sha256': core.sha(data), 'data': base64.b64encode(data).decode()})
  payload = core.canonical(release)
  envelope = core.canonical({'payload': base64.b64encode(payload).decode(), 'signature': base64.b64encode(key.sign(payload)).decode()})
  index = {k: release[k] for k in ('schema', 'release_id', 'sequence', 'notes')}
  index.update(bundle_commit='0'*40, bundle_bytes=len(envelope), bundle_sha256=core.sha(envelope))
  pub = base64.b64encode(key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)).decode()
  core.verify_bundle(envelope, index, pub)
  return envelope, index


def main():
  parser = argparse.ArgumentParser(description=__doc__)
  sub = parser.add_subparsers(dest='action', required=True)
  init = sub.add_parser('init-key'); init.add_argument('--private-dir', type=Path, required=True)
  release = sub.add_parser('build'); release.add_argument('--spec', type=Path, required=True)
  release.add_argument('--private-key', type=Path, required=True); release.add_argument('--commit', required=True)
  release.add_argument('--out', type=Path, required=True)
  index = sub.add_parser('index'); index.add_argument('--template', type=Path, required=True)
  index.add_argument('--bundle-commit', required=True); index.add_argument('--out', type=Path, required=True)
  args = parser.parse_args(); repo = Path(__file__).resolve().parents[1]
  if args.action == 'init-key':
    target = args.private_dir.resolve()
    if target.is_relative_to(repo.resolve()): raise ValueError('Private key must stay outside the repository')
    target.mkdir(parents=True, exist_ok=True)
    key = Ed25519PrivateKey.generate()
    with (target/'release-signing.key').open('xb') as stream:
      stream.write(key.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption()))
    (target/'release-signing.key').chmod(0o600)
    core.save(target/'release-public.json', {'public_key': base64.b64encode(key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)).decode()})
    print('Private signing key created. Preserve it outside GitHub and device backups.')
  elif args.action == 'build':
    raw, template = build(repo, core.load(args.spec), args.private_key, args.commit)
    args.out.mkdir(parents=True, exist_ok=True)
    dest = args.out/(template['release_id']+'.json')
    if dest.exists() and dest.read_bytes() != raw: raise ValueError('A published release ID cannot be reused')
    core.atomic(dest, raw, 0o644)
    core.save(args.out/(template['release_id']+'.index-template.json'), template)
    print(json.dumps({'release_id': template['release_id'], 'bytes': len(raw), 'sha256': core.sha(raw)}))
  else:
    value = core.load(args.template)
    commit = subprocess.check_output(['git', 'rev-parse', '--verify', args.bundle_commit+'^{commit}'], cwd=repo, text=True).strip()
    data = subprocess.check_output(['git', 'show', commit+':updates/hud/bundles/'+value['release_id']+'.json'], cwd=repo)
    if core.sha(data) != value['bundle_sha256']: raise ValueError('Committed bundle differs from index template')
    value['bundle_commit'] = commit; core.validate_index(value)
    core.save(args.out, value)
    print('Pinned release index written. Commit and push it after verifying the source and tests.')


if __name__ == '__main__': main()
