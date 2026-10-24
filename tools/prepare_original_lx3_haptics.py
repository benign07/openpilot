"""Prepare a hash-bound ORIGINAL LX3 haptic overlay; never install or send CAN.

The original Params extension must be rebuilt in its matching ARM environment.
This output is not a HUD source OTA and is not qualified by Python tests alone.
"""
import argparse
import ast
import hashlib
import json
import subprocess
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
  SETTINGS: 'b0bbd5acf822fd7a13011bb3b273aa16a2f8a3fd23c3c35d59be84c3e6a473e1',
}


def sha(raw):
  return hashlib.sha256(raw).hexdigest()


def replace_once(text, before, after):
  if text.count(before) != 1:
    raise ValueError('Expected one exact original anchor: '+before[:90])
  return text.replace(before, after, 1)


def committed(commit, name):
  return subprocess.check_output(['git','show',commit+':'+name],cwd=ROOT)


def prepare(installed, source_commit):
  helper_source = committed(source_commit, HELPER)
  helper_ns = {}
  exec(compile(helper_source, HELPER, 'exec'), helper_ns)
  sources = {n: (installed/n).read_bytes() for n in BASE}
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
    '      self.haptic_notifications_enabled = params.get_bool("C3xLiteHapticNotifications")\n'
    '      if uses_lx3_authority(self.CP):\n'
    '        self.haptic_notifications.configure_from_params(params)\n')
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
  latest = committed(source_commit, CONTROLLER).decode('utf8')
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
    '    {"C3xLiteHapticNotifications", {PERSISTENT, BOOL, "0"}},\n'+helper_ns['haptic_param_declarations']())
  settings = json.loads(sources[SETTINGS])
  if any(p.get('name') == 'C3xLiteHapticNotifications' for p in settings['params']):
    raise ValueError('Unexpected existing setting')
  settings['params'].append({
    'name':'C3xLiteHapticNotifications','group':'개발 중 기능','egroup':'Development features',
    'title':'일반 알림 핸들 진동 요청','etitle':'Routine steering-wheel haptic request',
    'descr':'LX3 일반 알림 진동의 전체 스위치입니다. 인게이지 진동은 2026-10-24 실차 확인. 아래에서 발생 상황과 패턴을 선택합니다. 제어 해제·CAN 오류·방향지시등·중요 경고 시 추가 요청을 취소합니다. 순정 VIBRATE는 켜짐/꺼짐 신호이며 모터 세기는 조절하지 않습니다. 부저·중요 경고음은 변경하지 않습니다.',
    'min':0,'max':1,'default':0,'unit':1,
  })
  for key, default, names, title in helper_ns['HAPTIC_EVENT_OPTIONS']:
    detail = ('방향지시등이 켜진 동안 항상 억제되므로 현재 일반적인 차선 변경 상황에서는 진동하지 않습니다.'
      if key == 'C3xLiteHapticLaneChange' else '후진 기어에서는 제어가 비활성이므로 현재 이 항목으로 진동하지 않습니다.'
      if key == 'C3xLiteHapticReverse' else '')
    english = {'C3xLiteHapticEngage':'Engage / control start','C3xLiteHapticTraffic':'Traffic signal alerts',
      'C3xLiteHapticLaneChange':'Lane change (muted with blinkers)', 'C3xLiteHapticStopping':'Stopping / auto hold',
      'C3xLiteHapticNavigation':'Navigation alerts','C3xLiteHapticReverse':'Reverse (inactive control: muted)'}[key]
    settings['params'].append({'name':key,'group':'개발 중 기능','egroup':'Development features',
      'title':title,'etitle':english,'min':0,'max':1,'default':int(default),'unit':1,
      'descr':detail+' 일반 알림 핸들 진동 요청이 켜져 있을 때 해당 알림의 진동을 선택합니다. 실제 알림과 제어 활성 상태가 함께 있어야 하며 방향지시등이 켜진 동안에는 요청하지 않습니다. 모든 운행에서 이 알림이 발생하는 것은 아닙니다.'})
  settings['params'].append({'name':helper_ns['HAPTIC_PATTERN_KEY'],'group':'개발 중 기능',
    'egroup':'Development features','title':'일반 알림 진동 패턴','etitle':'Routine haptic pulse pattern',
    'min':0,'max':2,'default':0,'unit':1,
    'descr':'0: 한 번 0.8초(인게이지 실차 확인)\n1: 두 번 0.3초·쉼 0.2초·0.3초(시험)\n2: 세 번 0.2초·쉼 0.1초(시험)\n세기 조절이 아닙니다. 반복 패턴의 실제 모터 반응은 미검증입니다. 과속카메라는 별도의 순정 패턴을 유지합니다.'})
  menu = settings.get('menu')
  if not isinstance(menu, list):
    raise ValueError('Original menu layout missing')
  development = next((n for n in menu if n.get('id') == 'development_features'), None)
  if development is None:
    development = {'id':'development_features','ko':'개발 중 기능','en':'Development features','groups':[]}
    menu.insert(0, development)
  development.setdefault('groups', []).append({'id':'haptic_notifications_original',
    'ko':'핸들 진동','en':'Steering-wheel haptics','params':['C3xLiteHapticNotifications']+
      [x[0] for x in helper_ns['HAPTIC_EVENT_OPTIONS']]+[helper_ns['HAPTIC_PATTERN_KEY']]})
  output = {CONTROLLER:c.encode(), FACTORY:f.encode(), KEYS:k.encode(),
    SETTINGS:(json.dumps(settings,ensure_ascii=False,indent=2)+'\n').encode(),
    HELPER:committed(source_commit, HELPER)}
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
  parser.add_argument('--source-commit',required=True)
  args=parser.parse_args()
  source_commit=subprocess.check_output(['git','rev-parse','--verify',args.source_commit+'^{commit}'],cwd=ROOT,text=True).strip()
  before, files=prepare(args.installed,source_commit)
  args.out.mkdir(parents=True,exist_ok=False)
  for name, raw in files.items():
    path=args.out/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
  manifest={'source_commit':source_commit,'target_serial':'80acfe79','target':'original0627_compatible_haptic_only',
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
