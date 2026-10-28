from __future__ import annotations

import json
import os
import re
import subprocess
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..config import CARROT_SETTING_PROFILES_PATH
from .params import (
  get_param_values,
  preview_param_restore_values,
  restore_param_values_validated,
)
from .settings import get_settings_cached


REPO_DIR = "/data/openpilot"
MAX_SETTING_PROFILES = 40
MAX_PROFILE_NAME_LEN = 40


def _now_iso() -> str:
  return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _git(args: List[str], timeout: float = 3.0) -> str:
  try:
    out = subprocess.check_output(
      ["git", *args],
      cwd=REPO_DIR,
      stderr=subprocess.DEVNULL,
      timeout=timeout,
    )
    return out.decode("utf-8", "replace").strip()
  except Exception:
    return ""


def _commit_url(remote: str, commit: str) -> str:
  remote = str(remote or "").strip()
  commit = str(commit or "").strip()
  if not remote or not commit:
    return ""

  match = re.match(r"^https?://github\.com/([^/]+)/([^/#?]+?)(?:\.git)?/?$", remote)
  if not match:
    match = re.match(r"^git@github\.com:([^/]+)/(.+?)(?:\.git)?$", remote)
  if not match:
    return ""

  owner, repo = match.groups()
  return f"https://github.com/{owner}/{repo}/commit/{commit}"


def read_git_profile_meta() -> Dict[str, Any]:
  branch = _git(["branch", "--show-current"])
  commit = _git(["rev-parse", "HEAD"])
  commit_date = _git(["show", "-s", "--format=%cI", "HEAD"])
  remote = _git(["config", "--get", "remote.origin.url"])
  return {
    "branch": branch,
    "commit": commit,
    "commit_short": commit[:7] if commit else "",
    "commit_date": commit_date,
    "remote": remote,
    "commit_url": _commit_url(remote, commit),
  }


def _clean_name(value: Any) -> str:
  name = str(value or "").strip()
  name = re.sub(r"\s+", " ", name)
  return name[:MAX_PROFILE_NAME_LEN]


def _clean_values(values: Any) -> Dict[str, Any]:
  if not isinstance(values, dict):
    return {}
  _, _, by_name, _ = get_settings_cached()
  allowed = {name for name, meta in by_name.items() if not meta.get('virtual')}
  return {
    str(key): value
    for key, value in values.items()
    if str(key) in allowed
  }


def _setting_defaults() -> Dict[str, Any]:
  _, _, by_name, _ = get_settings_cached()
  return {
    name: meta.get("default", 0)
    for name, meta in by_name.items()
    if not meta.get('virtual')
  }


def snapshot_current_setting_values() -> Dict[str, Any]:
  defaults = _setting_defaults()
  return get_param_values(list(defaults.keys()), defaults)


def _sanitize_profile(raw: Any) -> Optional[Dict[str, Any]]:
  if not isinstance(raw, dict):
    return None
  profile_id = str(raw.get("id") or "").strip()
  name = _clean_name(raw.get("name"))
  values = _clean_values(raw.get("values"))
  if not profile_id or not name or not values:
    return None

  created_at = str(raw.get("created_at") or "").strip()
  updated_at = str(raw.get("updated_at") or created_at).strip()
  meta = raw.get("meta") if isinstance(raw.get("meta"), dict) else {}
  return {
    "id": profile_id,
    "name": name,
    "created_at": created_at,
    "updated_at": updated_at,
    "meta": {
      "branch": str(meta.get("branch") or ""),
      "commit": str(meta.get("commit") or ""),
      "commit_short": str(meta.get("commit_short") or ""),
      "commit_date": str(meta.get("commit_date") or ""),
      "remote": str(meta.get("remote") or ""),
      "commit_url": str(meta.get("commit_url") or ""),
    },
    "values": values,
  }


def read_setting_profiles() -> Dict[str, Any]:
  try:
    with open(CARROT_SETTING_PROFILES_PATH, "r", encoding="utf-8") as f:
      raw = json.load(f)
  except Exception:
    raw = {}

  profiles = raw.get("profiles") if isinstance(raw, dict) else []
  clean = []
  seen = set()
  for item in profiles if isinstance(profiles, list) else []:
    profile = _sanitize_profile(item)
    if not profile or profile["id"] in seen:
      continue
    seen.add(profile["id"])
    clean.append(profile)
    if len(clean) >= MAX_SETTING_PROFILES:
      break
  return {"profiles": clean}


def write_setting_profiles(data: Dict[str, Any]) -> Dict[str, Any]:
  clean = {"profiles": []}
  seen = set()
  for item in data.get("profiles", []) if isinstance(data, dict) else []:
    profile = _sanitize_profile(item)
    if not profile or profile["id"] in seen:
      continue
    seen.add(profile["id"])
    clean["profiles"].append(profile)
    if len(clean["profiles"]) >= MAX_SETTING_PROFILES:
      break

  os.makedirs(os.path.dirname(CARROT_SETTING_PROFILES_PATH), exist_ok=True)
  tmp_path = CARROT_SETTING_PROFILES_PATH + ".tmp"
  with open(tmp_path, "w", encoding="utf-8") as f:
    json.dump(clean, f, ensure_ascii=False, indent=2, sort_keys=True)
    f.write("\n")
  os.replace(tmp_path, CARROT_SETTING_PROFILES_PATH)
  return clean


def get_setting_profile(profile_id: str) -> Optional[Dict[str, Any]]:
  profile_id = str(profile_id or "").strip()
  for profile in read_setting_profiles()["profiles"]:
    if profile["id"] == profile_id:
      return profile
  return None


def create_setting_profile(name: str) -> Dict[str, Any]:
  clean_name = _clean_name(name)
  if not clean_name:
    raise ValueError("missing profile name")

  data = read_setting_profiles()
  now = _now_iso()
  profile = {
    "id": uuid.uuid4().hex,
    "name": clean_name,
    "created_at": now,
    "updated_at": now,
    "meta": read_git_profile_meta(),
    "values": snapshot_current_setting_values(),
  }
  data["profiles"].append(profile)
  write_setting_profiles(data)
  return profile


def update_setting_profile(profile_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
  data = read_setting_profiles()
  for profile in data["profiles"]:
    if profile["id"] != profile_id:
      continue
    if "name" in updates:
      name = _clean_name(updates.get("name"))
      if not name:
        raise ValueError("missing profile name")
      profile["name"] = name
    if "values" in updates:
      profile["values"] = _clean_values(updates.get("values"))
    profile["updated_at"] = _now_iso()
    write_setting_profiles(data)
    return profile
  raise KeyError("profile not found")


def delete_setting_profile(profile_id: str) -> None:
  data = read_setting_profiles()
  next_profiles = [profile for profile in data["profiles"] if profile["id"] != profile_id]
  if len(next_profiles) == len(data["profiles"]):
    raise KeyError("profile not found")
  data["profiles"] = next_profiles
  write_setting_profiles(data)


def preview_setting_profile(profile_id: str, values: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
  profile = get_setting_profile(profile_id)
  if profile is None:
    raise KeyError("profile not found")
  restore_values = _clean_values(values) if values is not None else profile["values"]
  return preview_param_restore_values(restore_values)


def apply_setting_profile(profile_id: str, values: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
  profile = get_setting_profile(profile_id)
  if profile is None:
    raise KeyError("profile not found")
  restore_values = _clean_values(values) if values is not None else profile["values"]
  return restore_param_values_validated(restore_values)


# Portable longitudinal tuning contract. Legacy full-setting profiles above keep
# their existing API. This path never filters unknown keys or clamps values.
TUNING_SCHEMA = 1
TUNING_UNITS = {'MyDrivingMode': 'mode-enum-v1', 'ApproachComfortEnabled': 'bool-int', 'ApproachCoastStrength': 'percent-strength-v1', 'ApproachBrakeMargin': 'percent-strength-v1', 'ApproachStopSmooth': 'percent-strength-v1', 'ApproachTrafficCalm': 'percent-strength-v1', 'ApproachFlexibleGap': 'bool-int', 'ApproachFlexibleGapMin': 'moving-tf-percent', 'TFollowGap1': 'centisecond', 'TFollowGap2': 'centisecond', 'TFollowGap3': 'centisecond', 'TFollowGap4': 'centisecond', 'EnableSpeedTF': 'speed-tf-selector-v1', 'TFollowDecelBoost': 'percent-strength-v1', 'DynamicTFollow': 'percent-strength-v1', 'StopDistanceCarrot': 'centimeter', 'AChangeCostStarting': 'cost-weight', 'CruiseMaxVals0': 'centi-mps2', 'CruiseMaxVals1': 'centi-mps2', 'CruiseMaxVals2': 'centi-mps2', 'CruiseMaxVals3': 'centi-mps2', 'CruiseMaxVals4': 'centi-mps2', 'CruiseMaxVals5': 'centi-mps2', 'CruiseMaxVals6': 'centi-mps2', 'LaunchFollowStrength': 'percent-strength-v1', 'MyDrivingModeAuto': 'auto-mode-enum-v1', 'DynamicTFollowLC': 'percent-strength-v1'}
TUNING_STATE_PATH = '/data/community/tuning_profiles/transaction-v1.json'
_TUNING_SERVER_EPOCH = uuid.uuid4().hex


def tuning_contract():
  import hashlib
  _, _, metadata, _ = get_settings_cached()
  fields = {}
  for key, unit in TUNING_UNITS.items():
    item = metadata.get(key)
    if item is None:
      continue
    fields[key] = {'unit': unit, 'min': item['min'], 'max': item['max'], 'title': item.get('title', key)}
  digest = hashlib.sha256(json.dumps(fields, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
  return {'schema': TUNING_SCHEMA, 'family': 'LX3', 'fields': fields, 'contract_revision': digest}


def validate_tuning_profile(profile):
  if not isinstance(profile, dict) or profile.get('kind') != 'CarrotTuningProfile' or type(profile.get('schema')) is not int or profile['schema'] != 1:
    raise ValueError('지원하지 않는 튜닝 프로필 형식입니다. HUD용 프로필을 내보내세요.')
  if profile.get('family') != 'LX3' or profile.get('scope') != 'longitudinal':
    raise ValueError('이 기기용 LX3 종방향 프로필이 아닙니다.')
  if not isinstance(profile.get('id'), str) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,63}', profile['id']):
    raise ValueError('프로필 식별자가 올바르지 않습니다.')
  if not isinstance(profile.get('name'), str) or not 1 <= len(profile['name'].strip()) <= 80:
    raise ValueError('프로필 이름을 확인하세요.')
  allowed = {'kind','schema','family','scope','id','name','description','values','units','source_commit','behavior_revision'}
  if set(profile) - allowed:
    raise ValueError('지원하지 않는 최상위 프로필 필드가 있습니다.')
  values, units = profile.get('values'), profile.get('units')
  fields = tuning_contract()['fields']
  if not isinstance(values, dict) or not 1 <= len(values) <= len(TUNING_UNITS) or not isinstance(units, dict) or set(units) != set(values):
    raise ValueError('값과 단위 목록이 일치하지 않습니다.')
  for key, value in values.items():
    f = fields.get(key)
    if f is None or units[key] != f['unit']:
      raise ValueError(key + ': 지원하지 않는 설정 또는 단위입니다. 자동 변환하지 않았습니다.')
    if type(value) is not int or not f['min'] <= value <= f['max']:
      raise ValueError(key + ': 정수 범위를 확인하세요. 자동 보정하지 않았습니다.')
  return dict(values)


def _tuning_context():
  from . import params as store
  from cereal import car
  from openpilot.system.hardware import HARDWARE
  from opendbc.car.hyundai.values import CAR
  if not store.HAS_PARAMS:
    raise ValueError('실기기 Params를 읽을 수 없습니다.')
  raw = store.Params().get('CarParams')
  if raw is None:
    raise ValueError('차량 인식 정보가 없습니다.')
  with car.CarParams.from_bytes(raw) as cp:
    fingerprint = cp.carFingerprint
  if fingerprint != str(CAR.HYUNDAI_PALISADE_LX3_HEV):
    raise ValueError('LX3로 인식된 기기에서만 적용할 수 있습니다. EV9에는 적용하지 않습니다.')
  return {'serial': _tuning_serial(), 'fingerprint': fingerprint}


def _tuning_values():
  from pathlib import Path
  from . import params as store
  if not store.HAS_PARAMS:
    raise ValueError('실기기 설정을 읽을 수 없습니다.')
  params = store.Params()
  output = {}
  store.driving_preferences.signature = None
  virtual = store.driving_preferences.read()
  if store.driving_preferences.error:
    raise ValueError('선호값 파일 오류: 기존 파일을 보존했습니다.')
  for key in tuning_contract()['fields']:
    if key == 'LaunchFollowStrength':
      output[key] = virtual[key]
    else:
      expected_type = getattr(store.ParamKeyType, 'BOOL' if key in ('ApproachComfortEnabled', 'ApproachFlexibleGap') else 'INT', None)
      if expected_type is None or params.get_type(key) != expected_type:
        raise ValueError(key + ': 실행 중 Params 등록 타입이 맞지 않습니다.')
      path = Path(params.get_param_path(key))
      if path.is_symlink():
        raise ValueError(key + ': 설정 파일 경로를 확인하세요.')
      try:
        with path.open('rb') as stream: raw = stream.read(65)
      except FileNotFoundError:
        raise ValueError(key + ': 저장값이 없습니다. 기본값으로 대체하지 않습니다.')
      pattern = rb'[01]' if key in ('ApproachComfortEnabled', 'ApproachFlexibleGap') else rb'(?:0|-?[1-9][0-9]*)'
      if len(raw) > 64 or re.fullmatch(pattern, raw) is None:
        raise ValueError(key + ': 원본 숫자 형식을 확인하세요. 자동 변환하지 않습니다.')
      output[key] = int(raw)
  for key, field in tuning_contract()['fields'].items():
    if not field['min'] <= output[key] <= field['max']:
      raise ValueError(key + ': 현재 저장값이 펌웨어 범위 밖입니다. 자동 변경하지 않습니다.')
  return output


def _tuning_state():
  from pathlib import Path
  path = Path(TUNING_STATE_PATH)
  try:
    if path.is_symlink() or not 0 < path.stat().st_size <= 131072:
      raise ValueError('state size/path')
    envelope = strict_tuning_json(path.read_text(encoding='utf8'))
    import hashlib
    if not isinstance(envelope, dict) or type(envelope.get('journal_schema')) is not int or envelope.get('journal_schema') != 1:
      raise ValueError('journal envelope')
    row = envelope.get('payload')
    digest = hashlib.sha256(json.dumps(row, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if envelope.get('sha256') != digest:
      raise ValueError('journal checksum')
    if (not isinstance(row, dict) or row.get('schema') != 1 or
        row.get('phase') not in ('applying', 'complete', 'rolled_back', 'recovery_required') or
        not isinstance(row.get('before'), dict) or not isinstance(row.get('after'), dict) or
        set(row['before']) != set(row['after']) or set(row['after']) - set(TUNING_UNITS) or
        not all(type(v) is int for v in [*row['before'].values(), *row['after'].values()]) or
        not re.fullmatch(r'[a-f0-9]{32}', str(row.get('request_id', ''))) or
        not isinstance(row.get('attempted'), list) or set(row['attempted']) - set(row['after'])):
      raise ValueError('state schema')
    if (type(row.get('schema')) is not int or not isinstance(row.get('context'), dict) or
        not all(isinstance(row['context'].get(k), str) and row['context'][k] for k in ('serial', 'fingerprint')) or
        not re.fullmatch(r'[a-f0-9]{64}', str(row.get('profile_hash', ''))) or
        not isinstance(row.get('units'), dict) or set(row['units']) != set(row['after']) or
        any(row['units'][k] != TUNING_UNITS[k] for k in row['units'])):
      raise ValueError('state context/units')
    return row
  except FileNotFoundError:
    return None
  except (ValueError, OSError, TypeError):
    return {'phase': 'corrupt', 'error': '프로필 적용 기록이 손상됐습니다. 파일을 보존하고 적용을 중단했습니다.'}


def save_tuning_state(row):
  from pathlib import Path
  path = Path(TUNING_STATE_PATH)
  _save_tuning_json(path, row)


def _save_tuning_json(path, row):
  if path.is_symlink():
    raise ValueError('프로필 기록 경로를 확인하세요.')
  path.parent.mkdir(parents=True, exist_ok=True)
  tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
  try:
    with tmp.open('x', encoding='utf8') as stream:
      import hashlib
      digest = hashlib.sha256(json.dumps(row, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
      json.dump({'journal_schema': 1, 'sha256': digest, 'payload': row}, stream, ensure_ascii=False, allow_nan=False)
      stream.flush()
      os.fsync(stream.fileno())
    os.replace(tmp, path)
    if os.name == 'posix':
      fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
      try: os.fsync(fd)
      finally: os.close(fd)
  finally:
    tmp.unlink(missing_ok=True)


def tuning_snapshot():
  import hashlib
  context = _tuning_context()
  contract = tuning_contract()
  values = _tuning_values()
  state = _tuning_state()
  from pathlib import Path
  from . import params as store
  preference_path = store.driving_preferences.path
  try:
    stat = preference_path.stat()
    preference_state = [stat.st_mtime_ns, stat.st_size]
  except FileNotFoundError:
    preference_state = None
  behavior = tuning_behavior_revision()
  unverified = tuning_archive_warning(state)
  generation = [state.get('request_id'), state.get('phase')] if state else None
  generation = [generation, unverified['archive_warning_id']]
  revision = hashlib.sha256(json.dumps([context, _TUNING_SERVER_EPOCH, behavior, contract['contract_revision'], values, preference_state, generation], sort_keys=True, separators=(',', ':')).encode()).hexdigest()
  blocked = bool(state and (state['phase'] not in ('complete', 'rolled_back') or state.get('context') != context))
  return {'ok': True, **contract, 'context': context, 'values': values, 'revision': revision,
          'receipt': state, 'recovery_required': blocked, **unverified, 'behavior_revision': behavior, 'journal_sha256': tuning_journal_digest(),
          'virtual_origin': '저장 파일' if preference_state is not None else '파일 없음 · 기존 기본값',
          'current_matches_last': bool(state and state['phase'] == 'complete' and all(values.get(k) == v for k, v in state['after'].items())),
          'note': '저장값이며 주행 중 실제 제어 적용 여부와는 별개입니다. P 정차·제어 해제 후 적용합니다.'}


def preview_tuning_profile(profile):
  values = validate_tuning_profile(profile)
  snapshot = tuning_snapshot()
  if snapshot['recovery_required']:
    raise ValueError('이전 적용의 복구 확인이 필요합니다. 새 프로필은 적용하지 않았습니다.')
  return {'ok': True, 'revision': snapshot['revision'], 'context': snapshot['context'], 'profile_hash': tuning_profile_digest(profile),
          'profile': profile, 'changes': [{'key': k, 'before': snapshot['values'][k], 'after': v}
                                         for k, v in values.items() if v != snapshot['values'][k]],
          'note': '프로필에 포함된 종방향 항목 전체가 지정값으로 돌아갑니다. MyDrivingMode가 포함되면 운행모드도 변경됩니다. DM·조향·카메라는 변경하지 않습니다.' +
                  (' 제어코드 버전이 다르거나 기록되지 않았습니다. 항목 호환과 주행 품질은 별개이므로 다시 비교하세요.'
                   if profile.get('behavior_revision') != snapshot['behavior_revision'] else '')}



def tuning_write(key, value):
  import time
  from . import params as store
  from .params import set_param_value
  old_stamp = None
  if key == 'LaunchFollowStrength':
    store.driving_preferences.signature = None
    store.driving_preferences.read()
    if store.driving_preferences.error:
      raise ValueError('가상 선호값 오류: 원본 보존')
    try: old_stamp = store.driving_preferences.path.stat().st_mtime_ns
    except FileNotFoundError: pass
  _, _, metadata, _ = get_settings_cached()
  set_param_value(key, value, metadata[key])
  if key == 'LaunchFollowStrength':
    path = store.driving_preferences.path
    if old_stamp is not None and path.stat().st_mtime_ns == old_stamp:
      stamp = max(time.time_ns(), old_stamp + 1_000_000)
      os.utime(path, ns=(stamp, stamp))
    store.driving_preferences.signature = None
    result = store.driving_preferences.read()
    if store.driving_preferences.error or result[key] != value:
      raise ValueError('가상 선호값 저장 확인 실패')
  else:
    from pathlib import Path
    if Path(store.Params().get_param_path(key)).read_bytes() != str(value).encode('ascii'):
      raise ValueError(key + ': 원본 저장 바이트 불일치')


def strict_tuning_json(raw):
  def unique(pairs):
    result = {}
    for key, value in pairs:
      if key in result: raise ValueError('중복 JSON 키: ' + key)
      result[key] = value
    return result
  def invalid_constant(value):
    raise ValueError('유효하지 않은 JSON 숫자: ' + value)
  def strict_integer(value):
    if value == '-0': raise ValueError('음수 영은 지원하지 않습니다.')
    return int(value)
  return json.loads(raw, object_pairs_hook=unique, parse_constant=invalid_constant, parse_int=strict_integer)


TUNING_BEHAVIOR_SOURCES = (
  'selfdrive/carrot/carrot_functions.py',
  'selfdrive/controls/lib/approach_comfort.py',
  'selfdrive/controls/lib/flexible_gap.py',
  'selfdrive/controls/lib/longitudinal_planner.py',
  'selfdrive/controls/lib/longitudinal_mpc_lib/long_mpc.py',
)


def tuning_behavior_revision():
  from pathlib import Path
  import hashlib
  root = Path(__file__).resolve().parents[4]
  signature = tuple((str(root / f), (root / f).stat().st_mtime_ns, (root / f).stat().st_size) for f in TUNING_BEHAVIOR_SOURCES)
  cached = getattr(tuning_behavior_revision, '_cache', None)
  if cached and cached[0] == signature:
    return cached[1]
  digest = hashlib.sha256()
  for f in TUNING_BEHAVIOR_SOURCES:
    digest.update(f.encode() + b'\0' + (root / f).read_bytes().replace(b'\r\n', b'\n') + b'\0')
  result = digest.hexdigest()
  tuning_behavior_revision._cache = signature, result
  return result


def _tuning_serial():
  from openpilot.system.hardware import HARDWARE
  serial = HARDWARE.get_serial()
  if not isinstance(serial, str) or not re.fullmatch(r'[A-Za-z0-9_-]{4,128}', serial):
    raise ValueError('기기 식별자가 없습니다.')
  return serial


def tuning_receipt():
  """Readable even when a malformed parameter prevents a full value snapshot."""
  state = _tuning_state()
  serial = _tuning_serial()
  wrong_device = bool(state and state.get('context') and state['context']['serial'] != serial)
  try: current_context = _tuning_context()
  except Exception: current_context = None
  context_mismatch = bool(state and state.get('context') and current_context and state['context'] != current_context)
  blocked = bool(state and (state['phase'] not in ('complete', 'rolled_back') or wrong_device or context_mismatch))
  return {'ok': True, 'schema': 1, 'family': 'LX3', 'context': {'serial': serial},
          'receipt': state, 'recovery_required': blocked, **tuning_archive_warning(state), 'wrong_device': wrong_device, 'journal_sha256': tuning_journal_digest()}


def tuning_journal_digest():
  from pathlib import Path
  import hashlib
  path = Path(TUNING_STATE_PATH)
  try:
    if path.is_symlink() or path.stat().st_size > 131072:
      return None
    with path.open('rb') as stream: raw = stream.read(131073)
    if len(raw) > 131072: return None
    return hashlib.sha256(raw).hexdigest()
  except FileNotFoundError:
    return None


def archive_tuning_journal(expected_sha, serial):
  from pathlib import Path
  if serial != _tuning_serial() or not isinstance(expected_sha, str) or not re.fullmatch('[a-f0-9]{64}', expected_sha):
    raise ValueError('기기와 보존할 원본 해시를 확인하세요.')
  if tuning_journal_digest() != expected_sha:
    raise ValueError('기록이 미리보기 이후 바뀌었습니다. 다시 확인하세요.')
  state = _tuning_state()
  same_context = bool(state and state.get('context') and state['context'].get('serial') == serial and state['context'] == _tuning_context())
  if same_context and not (state['phase'] == 'recovery_required' and state.get('error')):
    raise ValueError('동일 기기의 복구 가능한 기록은 먼저 복구를 시도하세요.')
  path = Path(TUNING_STATE_PATH)
  archived = path.with_name(path.name + '.archived.' + uuid.uuid4().hex)
  # Persist the warning before moving the evidence; even failed archive keeps it visible.
  _save_tuning_json(path.with_name(path.name+'.unverified'), {'serial':serial,'archive_sha256':expected_sha,'id':uuid.uuid4().hex})
  os.replace(path, archived)
  if os.name == 'posix':
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)
  return archived.name


def tuning_profile_digest(profile):
  import hashlib
  return hashlib.sha256(json.dumps(profile, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def tuning_archive_warning(state):
  from pathlib import Path
  import hashlib
  path=Path(TUNING_STATE_PATH).with_name(Path(TUNING_STATE_PATH).name+'.unverified')
  try:
    if path.is_symlink() or path.stat().st_size>131072: raise ValueError('미확인 기록을 점검하세요.')
    raw=path.read_bytes()
  except FileNotFoundError:
    return {'unverified_previous':False,'archive_warning_id':None}
  digest=hashlib.sha256(raw).hexdigest()
  verified=bool(state and state.get('phase')=='complete' and state.get('archive_warning_id')==digest and set(state.get('after',{}))==set(TUNING_UNITS))
  return {'unverified_previous':not verified,'archive_warning_id':digest}
