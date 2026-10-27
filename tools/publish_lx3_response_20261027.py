"""Publish an already signed separate LX3 audit package; preserve shared HUD latest.

Without --publish this only validates local files. No device actions. A later
sanitized installation record may be signed and added without moving the tag.
"""
from pathlib import Path
import argparse
import base64
import json
import subprocess
import sys
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from selfdrive.carrot.hud_update import core
from tools.publish_hud_release import CHANNEL, commit_files, git
from tools import install_lx3_response_20261027 as deploy

NAMES = ('installer.py', 'haptic_core.py', 'feature_core.py', 'VERIFICATION.md',
         'package.signed.json', 'release-public.json', 'PREPARED.json')
PREFIX = 'updates/hud/original-reference/audit/' + deploy.RID + '/'
LATEST = 'updates/hud/latest.json'
PROOF = 'INSTALLATION_VERIFIED.signed.json'


def load_bundle(folder):
  blobs = {name: deploy.checked(folder, name).read_bytes() for name in NAMES}
  manifest, files, bundle, _, _ = deploy.verify(folder, folder / 'unused-source-root', folder / 'release-public.json')
  prepared = json.loads(blobs['PREPARED.json'])
  deploy.require(deploy.sha(blobs['installer.py']) == manifest['installer_sha256'], 'Packaged installer differs')
  deploy.require(deploy.sha(blobs['VERIFICATION.md']) == manifest['verification_sha256'], 'Verification record differs')
  for name in ('release_id', 'source_commit'):
    deploy.require(prepared[name] == manifest[name], 'PREPARED identity differs')
  deploy.require(prepared['bundle_sha256'] == bundle and prepared['bundle_bytes'] == len(blobs['package.signed.json']), 'PREPARED bundle differs')
  expected = [{k: row[k] for k in ('path', 'before', 'sha256', 'bytes')} for row in manifest['files']]
  deploy.require(prepared['files'] == expected and len(files) == len(deploy.ORDER), 'PREPARED source inventory differs')
  deploy.require(prepared['protected_count'] == len(manifest['protected']), 'PREPARED protected inventory differs')
  return manifest, prepared, blobs


def public_bytes(commit, name):
  url = core.RAW + commit + '/' + name
  with urlopen(Request(url, headers={'Cache-Control': 'no-cache'}), timeout=30) as response:
    data = response.read(core.LIMIT + 1)
  deploy.require(len(data) <= core.LIMIT, 'Oversize public file')
  return data


def remote_tag():
  lines = git('ls-remote', '--tags', 'origin', 'refs/tags/' + deploy.RID, 'refs/tags/' + deploy.RID + '^{}').decode().splitlines()
  refs = {ref: oid for oid, ref in (line.split() for line in lines)}
  if not refs:
    return None
  deploy.require('refs/tags/' + deploy.RID + '^{}' in refs, 'Existing release tag is not annotated')
  return refs['refs/tags/' + deploy.RID + '^{}']


def local_tag():
  try:
    return git('rev-parse', '--verify', 'refs/tags/' + deploy.RID + '^{}').decode().strip()
  except subprocess.CalledProcessError:
    return None


def require_git_bytes(commit, blobs):
  for name, data in blobs.items():
    deploy.require(git('show', commit + ':' + PREFIX + name) == data, 'Published immutable file differs: ' + name)


def installation_envelope(record_path, key_path, manifest, bundle):
  from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
  from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
  deploy.require(key_path is not None and not key_path.resolve().is_relative_to(ROOT.resolve()), 'Signing key must remain outside Git')
  raw = record_path.read_bytes()
  deploy.require(0 < len(raw) < 512 * 1024, 'Invalid installation record size')
  evidence = json.loads(raw)
  deploy.require(isinstance(evidence, dict) and evidence.get('release_id') == deploy.RID, 'Wrong installation record release')
  if 'source_commit' in evidence:
    deploy.require(evidence['source_commit'] == manifest['source_commit'], 'Installation record source differs')
  payload = core.canonical({'schema': 1, 'kind': 'lx3-audit-installation-verification',
    'release_id': deploy.RID, 'target_serial': deploy.SERIAL, 'source_commit': manifest['source_commit'],
    'bundle_sha256': bundle, 'evidence': evidence})
  key = Ed25519PrivateKey.from_private_bytes(key_path.read_bytes())
  public = key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
  deploy.require(deploy.sha(public) == deploy.PUBLIC_SHA, 'Wrong installation signing key')
  signature = key.sign(payload)
  Ed25519PublicKey.from_public_bytes(public).verify(signature, payload)
  return core.canonical({'payload': base64.b64encode(payload).decode(), 'signature': base64.b64encode(signature).decode()})


def publish(args):
  manifest, prepared, blobs = load_bundle(args.folder)
  proof = None
  if args.installation_record is not None:
    proof = installation_envelope(args.installation_record, args.private_key, manifest, prepared['bundle_sha256'])
  else:
    deploy.require(args.private_key is None, 'Package publication needs no private key')
  mode = 'installation_record' if proof is not None else 'package'
  if not args.publish:
    return {'release_id': deploy.RID, 'mode': mode, 'validated': True, 'published': False,
            'bundle_sha256': prepared['bundle_sha256'], 'shared_HUD_latest_changed': False}

  git('fetch', 'origin', CHANNEL)
  parent = git('rev-parse', 'FETCH_HEAD').decode().strip()
  latest = git('show', parent + ':' + LATEST)
  sequence = json.loads(latest)['sequence']
  existing = set(git('ls-tree', '-r', '--name-only', parent, PREFIX).decode().splitlines())
  expected_names = {PREFIX + n for n in NAMES}
  tag_commit = remote_tag()
  if existing:
    deploy.require(expected_names <= existing <= expected_names | {PREFIX + PROOF}, 'Incomplete or unexpected published package')
    require_git_bytes(parent, blobs)
    added = git('log', '--format=%H', '--diff-filter=A', '--reverse', parent, '--', PREFIX + 'package.signed.json').decode().splitlines()
    deploy.require(len(added) == 1, 'Ambiguous original publication commit')
    package_commit = added[0]
    require_git_bytes(package_commit, blobs)
    if tag_commit is not None:
      deploy.require(tag_commit == package_commit, 'Release tag points to another commit')
  else:
    deploy.require(proof is None and tag_commit is None and local_tag() is None, 'Publication name already reserved or package missing')
    package_commit = commit_files(parent, {PREFIX + n: b for n, b in blobs.items()}, 'Publish separate signed ' + deploy.RID)
    deploy.require(git('show', package_commit + ':' + LATEST) == latest, 'Shared HUD latest changed')
    git('push', 'origin', package_commit + ':refs/heads/' + CHANNEL)
    parent = package_commit

  publication_commit = parent
  if proof is not None:
    deploy.require(tag_commit == package_commit, 'Verify the package release tag before publishing installation evidence')
    if PREFIX + PROOF in existing:
      deploy.require(git('show', parent + ':' + PREFIX + PROOF) == proof, 'Installation proof is immutable')
    else:
      publication_commit = commit_files(parent, {PREFIX + PROOF: proof}, 'Record verified original-device installation of ' + deploy.RID)
      deploy.require(git('show', publication_commit + ':' + LATEST) == latest, 'Shared HUD latest changed')
      git('push', 'origin', publication_commit + ':refs/heads/' + CHANNEL)

  # Read immutable public objects, not only local Git objects or a moving URL.
  for name, expected in blobs.items():
    deploy.require(public_bytes(package_commit, PREFIX + name) == expected, 'Public package bytes differ: ' + name)
  deploy.require(public_bytes(publication_commit, LATEST) == latest, 'Public shared latest changed')
  envelope = json.loads(public_bytes(package_commit, PREFIX + 'package.signed.json'))
  from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
  public = base64.b64decode(json.loads(blobs['release-public.json'])['public_key'], validate=True)
  Ed25519PublicKey.from_public_bytes(public).verify(base64.b64decode(envelope['signature'], validate=True),
                                                  base64.b64decode(envelope['payload'], validate=True))
  if proof is not None:
    actual = public_bytes(publication_commit, PREFIX + PROOF)
    deploy.require(actual == proof, 'Public installation proof differs')
    signed = json.loads(actual)
    Ed25519PublicKey.from_public_bytes(public).verify(base64.b64decode(signed['signature'], validate=True),
                                                    base64.b64decode(signed['payload'], validate=True))
  elif tag_commit is None:
    old_local = local_tag()
    if old_local is None:
      git('tag', '-a', deploy.RID, package_commit, '-m', deploy.RID + ' signed separate parked update')
    else:
      deploy.require(old_local == package_commit and git('cat-file', '-t', 'refs/tags/' + deploy.RID).decode().strip() == 'tag', 'Local release tag differs')
    git('push', 'origin', 'refs/tags/' + deploy.RID)
  deploy.require(remote_tag() == package_commit, 'Public annotated tag verification failed')
  git('fetch', 'origin', CHANNEL)
  current = git('rev-parse', 'FETCH_HEAD').decode().strip()
  deploy.require(git('show', current + ':' + LATEST) == latest, 'Concurrent shared HUD latest change detected')
  return {'release_id': deploy.RID, 'mode': mode, 'source_commit': manifest['source_commit'],
          'bundle_sha256': prepared['bundle_sha256'], 'package_commit': package_commit,
          'publication_commit': publication_commit, 'channel_commit_observed': current,
          'tag_commit': package_commit, 'published': True, 'public_bytes_and_signature_verified': True,
          'shared_HUD_latest_changed': False, 'shared_HUD_sequence': sequence,
          'shared_HUD_latest_sha256': deploy.sha(latest), 'installation_proof_published': proof is not None}


def main():
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--folder', type=Path, required=True)
  parser.add_argument('--out', type=Path, required=True)
  parser.add_argument('--publish', action='store_true')
  parser.add_argument('--installation-record', type=Path)
  parser.add_argument('--private-key', type=Path)
  args = parser.parse_args()
  deploy.require(not args.out.resolve().is_relative_to(ROOT.resolve()), 'Keep publication audit outside the source checkout')
  result = publish(args)
  core.save(args.out, result)
  print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
  main()
