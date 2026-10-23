"""Prepare a hash-bound ORIGINAL LX3 haptic overlay; never install or send CAN.

The original Params extension must be rebuilt in its matching ARM environment.
This output is not a HUD source OTA and is not qualified by Python tests alone.
"""
import argparse
import ast
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTROLLER = 'opendbc_repo/opendbc/car/hyundai/carcontroller.py'
FACTORY = 'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py'
HELPER = 'opendbc_repo/opendbc/car/hyundai/lx3_haptic_notifications.py'
KEYS = 'common/params_keys.h'
SETTINGS = 'selfdrive/carrot_settings.json'
PRODUCER = 'selfdrive/controls/controlsd.py'
BASE = {
  CONTROLLER: 'e62d84e99928928823f9d526b63a23878939e14732d22c73946724049a158e87',
  FACTORY: '554dde792342f3dc3ca9abe56428bae7bb98b53f9d821f7aeea309bca9226ccd',
  KEYS: '07debde2b9648c3fe97af0077e407fe56babf5bacaf9d97c8b8dd570a3b3003c',
  PRODUCER: 'fd5f457c1685831e646abfcb7664aa9e26d2f4a72dc0229312aecc5d4692ff7b',
}


def sha(raw):
  return hashlib.sha256(raw).hexdigest()


def replace_once(text, before, after):
  if text.count(before) != 1:
    raise ValueError('Expected one exact original anchor: '+before[:90])
  return text.replace(before, after, 1)


def prepare(installed):
  sources = {n: (installed/n).read_bytes() for n in (*BASE, SETTINGS)}
  for name, expected in BASE.items():
    if sha(sources[name]) != expected:
      raise ValueError('Original source mismatch: '+name)
  c = sources[CONTROLLER].decode('utf8')
  c = replace_once(c, 'from opendbc.car.hyundai.manual_steering import ManualSteeringCeiling\n',
    'from opendbc.car.hyundai.manual_steering import ManualSteeringCeiling\n'
    'from opendbc.car.hyundai.lx3_haptic_notifications import HapticNotifications\n')
  c = replace_once(c, '    self.hapticFeedbackWhenSpeedCamera = 0\n',
    '    self.hapticFeedbackWhenSpeedCamera = 0\n'
    '    self.haptic_notifications = HapticNotifications()\n'
    '    self.haptic_notifications_enabled = False\n')
  c = replace_once(c, '      self.hapticFeedbackWhenSpeedCamera = int(params.get_int("HapticFeedbackWhenSpeedCamera"))\n',
    '      self.hapticFeedbackWhenSpeedCamera = int(params.get_int("HapticFeedbackWhenSpeedCamera"))\n'
    '      self.haptic_notifications_enabled = params.get_bool("C3xLiteHapticNotifications")\n')
  c = replace_once(c, '    lx3_guard = uses_lx3_authority(self.CP)\n',
    '    lx3_guard = uses_lx3_authority(self.CP)\n'
    '    notification_vibrate = self.haptic_notifications.update(\n'
    '      hud_control.audibleAlert, frame=self.frame,\n'
    '      eligible=(lx3_guard and self.haptic_notifications_enabled and CS.out.canValid and not CS.out.canTimeout\n'
    '                and (CC.latActive or CC.longActive) and not (CS.out.leftBlinker or CS.out.rightBlinker)),\n'
    '    )\n')
  # Port the independently reviewed camera timer fix, not unrelated controls.
  start = c.index('    active_speed_decel = hud_control.activeCarrot == 3')
  end = c.index('    if self.frame % self.blinking_frame == 0:', start)
  latest = (ROOT/CONTROLLER).read_text(encoding='utf8')
  begin = latest.index('    speed_decel_active = hud_control.activeCarrot == 3')
  finish = latest.index('    if self.frame % self.blinking_frame == 0:', begin)
  c = c[:start]+latest[begin:finish]+c[end:]
  c = replace_once(c, 'self.enable_corner_radar, stopping, self.canfd_debug))',
    'self.enable_corner_radar, stopping, self.canfd_debug, notification_vibrate=notification_vibrate))')
  f = sources[FACTORY].decode('utf8')
  f = replace_once(f, '                         enable_corner_radar, stopping, canfd_debug):',
    '                         enable_corner_radar, stopping, canfd_debug, notification_vibrate=False):')
  f = replace_once(f, '          values["VIBRATE"] = 1\n\n        if canfd_debug > 0:',
    '          values["VIBRATE"] = 1\n'
    '        if notification_vibrate:\n'
    '          from opendbc.car.hyundai.lx3_buttons import uses_lx3_authority\n'
    '          if uses_lx3_authority(CP):\n'
    '            values["VIBRATE"] = 1\n\n        if canfd_debug > 0:')
  k = sources[KEYS].decode('utf8')
  if 'C3xLiteHapticNotifications' in k:
    raise ValueError('Unexpected existing key')
  k = replace_once(k, '    {"HardwareC3xLite", {PERSISTENT, INT, "0"}},\n',
    '    {"HardwareC3xLite", {PERSISTENT, INT, "0"}},\n'
    '    {"C3xLiteHapticNotifications", {PERSISTENT, BOOL, "0"}},\n')
  settings = json.loads(sources[SETTINGS])
  if any(p.get('name') == 'C3xLiteHapticNotifications' for p in settings['params']):
    raise ValueError('Unexpected existing setting')
  settings['params'].append({
    'name':'C3xLiteHapticNotifications','group':'개발 중 기능','egroup':'Development features',
    'title':'일반 알림 핸들 진동 요청','etitle':'Routine steering-wheel haptic request',
    'descr':'LX3 전용 시험 기능. 일반 알림에 최대0.8초의 순정 VIBRATE 요청을 보냅니다. 실제 모터 반응은 검증 중이며 조향 토크로 진동을 만들지 않습니다. 제어 해제·CAN 오류·방향지시등·중요 경고 시 추가 요청을 즉시 취소합니다. 부저·중요 경고음과 과속카메라 설정은 변경하지 않습니다.',
    'min':0,'max':1,'default':0,'unit':1,
  })
  menu = settings.get('menu')
  if not isinstance(menu, list):
    raise ValueError('Original menu layout missing')
  development = next((n for n in menu if n.get('id') == 'development_features'), None)
  if development is None:
    development = {'id':'development_features','ko':'개발 중 기능','en':'Development features','groups':[]}
    menu.insert(0, development)
  development.setdefault('groups', []).append({'id':'haptic_notifications_original',
    'ko':'핸들 진동','en':'Steering-wheel haptics','params':['C3xLiteHapticNotifications']})
  output = {CONTROLLER:c.encode(), FACTORY:f.encode(), KEYS:k.encode(),
    SETTINGS:(json.dumps(settings,ensure_ascii=False,indent=2)+'\n').encode(),
    HELPER:(ROOT/HELPER).read_bytes()}
  producer = sources[PRODUCER].decode('utf8')
  producer = replace_once(producer,
    "    hudControl.visualAlert = self.sm['selfdriveState'].alertHudVisual\n",
    "    hudControl.visualAlert = self.sm['selfdriveState'].alertHudVisual\n"
    "    hudControl.audibleAlert = self.sm['selfdriveState'].alertSound\n")
  output[PRODUCER] = producer.encode()
  for name, raw in output.items():
    if name.endswith('.py'):
      ast.parse(raw.decode('utf8'), filename=name)
  return sources, output


def main():
  parser=argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--installed',type=Path,required=True)
  parser.add_argument('--out',type=Path,required=True)
  args=parser.parse_args()
  before, files=prepare(args.installed)
  args.out.mkdir(parents=True,exist_ok=False)
  for name, raw in files.items():
    path=args.out/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
  manifest={'target_serial':'80acfe79','target':'original0627_compatible_haptic_only',
    'not_a_latest_control_stack_migration':True,'device_installed':False,
    'arm_params_build_verified':False,'motor_response_verified':False,
    'native_rebuild_required':'common/params_pyx.so from original matching source/ABI with added key',
    'preserved':['Panda firmware','pandad','MDPS handling','steering control','schema','calibration','identity','DisableDM'],
    'files':[{'path':n,'before':sha(before[n]) if n in before else None,
              'sha256':sha(raw),'bytes':len(raw)} for n,raw in files.items()]}
  (args.out/'manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf8')
  print(json.dumps(manifest))


if __name__=='__main__':
  main()
