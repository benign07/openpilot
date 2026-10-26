"""Prepare the immutable signed original-LX3 audit layer; never connect/install.

Only committed source is packaged. Installed evidence, the signing key and
private staging remain outside Git. Shared HUD latest.json is not modified.
"""
from pathlib import Path
import argparse
import base64
import importlib.util
import json
import re
import subprocess

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = 'tools/install_lx3_audit_20261026.py'
spec = importlib.util.spec_from_file_location('audit_deployment', ROOT / INSTALLER)
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


def canonical(value):
  return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def committed(commit, name):
  return subprocess.check_output(['git', 'show', commit + ':' + name], cwd=ROOT)


def compose(commit, before, installed, verification):
  deploy.require(re.fullmatch(r'[a-f0-9]{40}', commit) is not None, 'Use a full source commit')
  installer = committed(commit, INSTALLER)
  deploy.require(installer == Path(deploy.__file__).read_bytes(), 'Run the installer source bound to this commit')
  helpers = {
    'haptic_core.py': committed(commit, 'tools/install_original_lx3_haptics.py'),
    'feature_core.py': committed(commit, 'tools/install_original_lx3_feature_parity.py'),
  }
  deploy.require(deploy.sha(helpers['haptic_core.py']) == deploy.CORE_SHA
                 and deploy.sha(helpers['feature_core.py']) == deploy.FEATURE_SHA, 'Unqualified support source')
  inventory = dict(installed['hashes'])
  for name, digest in before['hashes'].items():
    if name in inventory:
      deploy.require(inventory[name] == digest, 'Installed evidence disagrees: ' + name)
    inventory[name] = digest
  rows = []
  for name in deploy.ORDER:
    original = before['hashes'][name]
    deploy.require(isinstance(original, str) and deploy.HEX.fullmatch(original), 'Missing live before hash')
    data = committed(commit, name)
    compile(data, name, 'exec')
    deploy.require(deploy.sha(data) != original, 'Unchanged file must not be advertised as a correction: ' + name)
    rows.append({'path': name, 'before': original, 'sha256': deploy.sha(data), 'bytes': len(data),
                 'data': base64.b64encode(data).decode()})
  protected = {n: h for n, h in inventory.items() if n not in deploy.ORDER}
  deploy.require(deploy.REQUIRED_PROTECTED <= protected.keys(), 'Incomplete live protected inventory')
  deploy.require(all(protected.get(n) == h for n, h in deploy.FIXED_PROTECTED.items()), 'Wrong native/schema baseline')
  prefs = {}
  for name, row in before['prefs'].items():
    deploy.require(name in deploy.JSON_NAMES, 'Unexpected owner JSON')
    prefs[name] = row['sha256'] if row['exists'] else None
  deploy.require(set(deploy.JSON_NAMES[:2]) <= prefs.keys(), 'Missing driving/cruise JSON evidence')
  manifest = {
    'schema': 1, 'release_id': deploy.RID, 'target_serial': deploy.SERIAL,
    'car_fingerprint': 'HYUNDAI_PALISADE_LX3_HEV', 'source_commit': commit,
    'installer_sha256': deploy.sha(installer), 'support_sha256': {n: deploy.sha(b) for n, b in helpers.items()},
    'files': rows, 'protected': protected, 'preserved_json': prefs,
    'before_evidence_sha256': deploy.sha(canonical(before)),
    'installed_evidence_sha256': deploy.sha(canonical(installed)),
    'verification_sha256': deploy.sha(verification),
    'scope': 'separate_signed_parked_seven_python_files',
    'native_changed': False, 'preferences_changed': False, 'shared_HUD_latest_changed': False,
    'road_qualified': False,
  }
  return manifest, {'installer.py': installer, **helpers, 'VERIFICATION.md': verification}


def prepare(args):
  for path in (args.before, args.installed, args.private_key, args.out):
    deploy.require(not path.resolve().is_relative_to(ROOT.resolve()), 'Private input/output must remain outside Git')
  deploy.require(not args.out.exists(), 'An immutable release folder must not be reused')
  before = json.loads(args.before.read_text(encoding='utf-8'))
  installed = json.loads(args.installed.read_text(encoding='utf-8'))
  manifest, files = compose(args.source_commit, before, installed, args.verification.read_bytes())
  key = Ed25519PrivateKey.from_private_bytes(args.private_key.read_bytes())
  public = key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
  deploy.require(deploy.sha(public) == deploy.PUBLIC_SHA, 'Key does not match installed public pin')
  payload = canonical(manifest)
  envelope = canonical({'payload': base64.b64encode(payload).decode(),
                        'signature': base64.b64encode(key.sign(payload)).decode()})
  args.out.mkdir(parents=True, exist_ok=False)
  for name, data in {**files, 'package.signed.json': envelope,
                     'release-public.json': canonical({'public_key': base64.b64encode(public).decode()})}.items():
    with (args.out / name).open('xb') as stream:
      stream.write(data)
  checked, _, bundle, _, _ = deploy.verify(args.out, args.out / 'unused-source-root', args.out / 'release-public.json')
  result = {
    'release_id': deploy.RID, 'source_commit': args.source_commit, 'bundle_sha256': bundle,
    'bundle_bytes': len(envelope), 'files': [{k: row[k] for k in ('path', 'before', 'sha256', 'bytes')} for row in checked['files']],
    'protected_count': len(checked['protected']), 'owner_JSON_preserved': True,
    'device_installed': False, 'published': False, 'road_qualified': False,
    'shared_HUD_latest_changed': False,
    'device_folder': '/data/lx3_separate_installs/' + deploy.RID,
    'publication_scope': 'updates/hud/original-reference/audit/' + deploy.RID,
  }
  (args.out / 'PREPARED.json').write_bytes(canonical(result) + b'\n')
  return result


def main():
  parser = argparse.ArgumentParser(description=__doc__)
  for name in ('before', 'installed', 'private-key', 'verification', 'out'):
    parser.add_argument('--' + name, type=Path, required=True)
  parser.add_argument('--source-commit', required=True)
  print(json.dumps(prepare(parser.parse_args()), ensure_ascii=False))


if __name__ == '__main__':
  main()
