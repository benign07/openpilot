import json
import os
from typing import Any, Dict, List, Tuple

from ..config import DEFAULT_SETTINGS_PATH
from openpilot.selfdrive.controls.lib.approach_comfort import DRIVING_PREFERENCES, CRUISE_BUTTON_PREFERENCES, RESPONSE_PREFERENCES


def inject_driving_preferences(data):
  """Virtual options use their own versioned store, never unknown Params keys."""
  descriptions = {
    'DriverPositionHold': ('수동 위치 조정 잠시 유지',
      'LX3 시험 기능·기본 꺼짐. 깜빡이 없이 확정된 조향 개입을0.3초 이상 유지하고, 개입 전과 놓기 직전의 안정된0.3초 평균 위치가 최소5cm 이상 달라졌을 때, 힘을 풀면 조정 위치를3초 유지하고5초 동안 기존 중앙 경로로 돌아옵니다. 조향보조 힘의0.2초 복구와 별개이며, 조향각이나 토크를 고정하지 않습니다. 추가 이동량은±10cm와 기존 총편향±40cm 범위로 제한합니다. 속도30~110km/h·신선하고 신뢰도 높은 양쪽 차선·충분한 차폭·정상 제어가 필요합니다. 점유 측 이동·깜빡이·차선변경 때 해제하며 일시적 인식 저하는 편향을 초당5cm씩 줄입니다. 관측창의 변동폭은4cm 이하여야 하며 불확실할수록 필요한 변위를 늘립니다. 예측 양보만 있고 확정 운전자 개입이 없는 경우나 위치가 계속 움직이는 중에 놓은 경우에는 유지하지 않을 수 있습니다. 저장 차로 추종속도와 현재 모드가 모두0이면30km/h부터 기존 차로 추종을 함께 사용합니다. 보정 위치·개입 전후와 영상 시점을 기록하지만 자동 학습으로 주행값을 변경하지 않습니다. 정차 후 변경합니다.'),
    'CruiseOffKeepLateral': ('크루즈 해제 시 조향보조 유지',
      'LX3 전용. 켜면 핸들 크루즈 버튼을 다시 눌러 속도 제어를 끌 때, 이미 요청된 조향보조는 유지합니다. 조향보조가 꺼져 있었다면 새로 켜지지 않습니다. LFA 버튼으로 조향보조를 끌 수 있으며 고장·입력 이상·통신 및 Panda 권한 검사는 그대로 적용됩니다. 꺼짐은 기존처럼 크루즈와 조향 요청을 함께 해제합니다. 기본 꺼짐. 버튼 전환 때 정상 권한 확인을 다시 거치며, 허용되지 않은 조향 출력을 유지하는 기능이 아닙니다. 옵션은 정차 후 변경합니다.'),
    'LaunchFollowStrength': ('재출발 앞차 추종 보완',
      '정지 후 움직이기 시작한 뒤, 같은 레이더 앞차가 안정적으로 멀어지고 목표거리보다1m 넘게 여유가 생기면 기존 가속표를 이 비율(%) 이내에서 보완합니다. 상대속도가 약0.6초 동안 지속적으로 증가하면 이 설정으로 허용한 보완 상한까지 점진적으로 사용합니다. 한 번의 값 튐은 증가로 인정하지 않으며 실제 가속은 MPC·커브·감속 제한이 결정합니다. 정지 해제·최소거리·제동·Panda 권한은 바꾸지 않습니다. 접근 감속·끼어들기·가까운 두 번째 앞차에는 적용하지 않습니다. 0은 기존 동작, 시험 시작은10을 권장합니다. 최대 양가속1.6m/s²를 넘기는 보완은 하지 않습니다. 실제 출발 지연이나 간격 개선을 보장하지 않습니다.'),
    'YellowLineBiasCm': ('노란 중앙선 쪽 여유 조절', '왼쪽의 노란 차선이2초 이상 지속되고 양쪽 차선 신뢰도·차폭 여유가 충분할 때 차로 안에서 이 거리(cm) 이내로 왼쪽에 붙습니다. 오른쪽 노란선·양쪽 노란선·도로 경계·왼쪽 차량·중앙선 방향 커브에서는 해제합니다.  반대 차로와의 여유는 줄어드므로 안전 개선을 보장하는 기능이 아닙니다. 기본0, 첫 비교는5cm 이하 권장. 기록 전용을 끄고 이동량을 켜면, 저장된 차로 추종 사용속도와 현재 LFA 차로 모드가 모두0일 경우30km/h부터 기존 차로 추종도 함께 사용합니다(기존 속도 히스테리시스±2km/h). 양수 설정과 LFA 길게 누름으로 바꾼 차로 모드는 그대로 따릅니다. 끄거나 기록 전용으로 바꾸면 원래 설정으로 돌아갑니다. 차로 추종이 선택돼도 폭·점유·신뢰도 등의 보정 조건을 통과하지 못하면 추가 편향은0일 수 있습니다.'),
    'NearbyVehicleBiasCm': ('주변 차량 반대쪽 여유 조절', 'HUD 차량 아이콘의 화면 좌표 대신 원본 전방 레이더 추적을 사용합니다. 앞쪽 약5~25m의 이동 차량을1초 이상 일관되게 확인하고 차선·차폭 여유가 충분할 때, 그 차량 반대쪽으로 차로 안에서 이 거리(cm) 이내의 편향을 요청합니다. 순정 측방 점유 신호는 이동을 제한하는 용도이며 그 신호만으로 반대쪽 편향을 만들지는 않습니다. 양쪽 점유·불확실한 위치·운전자 개입·깜빡이·차선변경 때 해제합니다. 이미 나란히 있거나 뒤에 있는 차량과의 여유를 보장하지 않습니다.  기본0, 첫 비교는5cm 이하 권장. 기록 전용을 끄고 이동량을 켜면, 저장된 차로 추종 사용속도와 현재 LFA 차로 모드가 모두0일 경우30km/h부터 기존 차로 추종도 함께 사용합니다(기존 속도 히스테리시스±2km/h). 양수 설정과 LFA 길게 누름으로 바꾼 차로 모드는 그대로 따릅니다. 끄거나 기록 전용으로 바꾸면 원래 설정으로 돌아갑니다. 차로 추종이 선택돼도 폭·점유·신뢰도 등의 보정 조건을 통과하지 못하면 추가 편향은0일 수 있습니다.'),
    'LateralMarginShadow': ('측방 여유 기록만 하기', '켜면 노란선/주변 차량 이동 후보와 해제 이유를 기록하되 조향 경로에는 적용하지 않습니다. 기본 켜짐입니다. 끄고 이동량을 설정해야 실제 적용 후보가 됩니다. 실제 적용도 차로 모드·유효한 양쪽 차선·신선한 차량/레이더 정보가 필요합니다. 시험 옵션 변경은 P·속도0·제어 해제 상태에서만 가능합니다. 기록 전용은 차로 추종이 꺼져 있어도 후보를 계산하며 경로를 바꾸지 않습니다.'),
  }
  existing = {p.get('name') for p in data.get('params', [])}
  names = []
  for name, (lo, hi, default) in {**DRIVING_PREFERENCES, **CRUISE_BUTTON_PREFERENCES, **RESPONSE_PREFERENCES}.items():
    if name in existing:continue
    title, description = descriptions[name]
    data.setdefault('params', []).append({'name': name, 'group': 'LX3 주행 보완',
      'egroup': 'LX3 driving preferences', 'cgroup': 'LX3 driving preferences',
      'title': title, 'descr': description, 'etitle': title, 'edescr': description,
      'ctitle': title, 'cdescr': description, 'min': lo, 'max': hi, 'default': default,
      'unit': 1, 'virtual': True})
    names.append(name)
  # Development controls have one prominent home. Move existing comfort/gap
  # entries by name, preserving defaults/limits and profile/favorite identity.
  sections = [
    ('cruise_button','크루즈 버튼·조향보조','Cruise button and steering',['CruiseOffKeepLateral']),
    ('launch_follow','재출발 추종','Launch following',['LaunchFollowStrength']),
    ('lateral_margin','차선 내 측방 여유','Within-lane margin',
      ['DriverPositionHold','YellowLineBiasCm','NearbyVehicleBiasCm','LateralMarginShadow']),
    ('approach_comfort','부드러운 감속·차간거리','Approach comfort and gap',
      ['ApproachComfortEnabled','ApproachCoastStrength','ApproachBrakeMargin',
       'ApproachStopSmooth','ApproachTrafficCalm','ApproachFlexibleGap','ApproachFlexibleGapMin']),
  ]
  definitions = {p.get('name'):p for p in data.get('params',[])}
  # Both preferences can bind the same positive acceleration ceiling. Keep
  # the existing IDs/ranges/defaults; a display label is not a freewheel sensor.
  suppression_notes = {
    'descr':'화면의 가속 억제 표시는 이 기능이나 정체 추종 차분함으로 양의 가속 상한이 낮아진 상태를 뜻합니다. 정차 중에도 나타날 수 있으며, 실제 타력주행·무동력 상태를 감지했다는 뜻은 아닙니다.',
    'edescr':'The acceleration-suppression indicator means this preference or stop-go calmness has lowered the positive acceleration ceiling. It can appear at standstill and does not detect physical coasting or zero propulsion.',
    'cdescr':'加速抑制指示表示本功能或拥堵跟车平缓功能降低了正加速度上限。停车时也可能显示，不表示检测到实际滑行或零驱动力。',
  }
  for name in ('ApproachCoastStrength','ApproachTrafficCalm'):
    if name in definitions:
      for field,note in suppression_notes.items():
        text=definitions[name].get(field,'')
        if note not in text:definitions[name][field]=(text+' '+note).strip()
  if 'ManualSteerWithBlinker' in definitions:
    definitions['ManualSteerWithBlinker'].update(
      descr='LX3에서 자동차선변경이 꺼진 경우(-1), 한쪽 깜빡이가 조향보조를 약1초 동안 줄입니다. 차량 신호상 깜빡이 꺼짐이 확인되고 분명한 조향 힘이 풀리면 현재 출력에서 약0.2초 동안 복귀합니다. 가볍게 잡는 것은 유지하며 분명한 힘에는 양보합니다. 조향각15도 미만이거나 속도약29km/h 이상·85도 미만의 안정된 커브(조향속도12도/초 이하0.2초)일 때 복귀합니다. 저속 회전·계속 힘을 주는 중에는 기다립니다. 물리 레버 종료부터는 기존 깜빡이 신호 필터 지연이 추가될 수 있습니다. 실제 제어권한·정상CAN·기존 각도/토크 제한이 우선하고 취소·고장 정지는 즉시 적용합니다. 가감속 제어는 별개입니다.',
      edescr='LX3 with automatic lane change OFF (-1): a single blinker fades assistance out over about1second. Once CarState confirms signals OFF and deliberate steering force is released, assistance recovers from its current output over about0.2seconds. Light grip retains tracking. Recovery requires angle below15degrees or a stable curve at >=8m/s, angle <85degrees and rate <=12degrees/s for0.2seconds. Low-speed turns or continued force wait. Existing lamp filtering may add delay after the physical lever ends. Actual permissions, healthy CAN and native angle/torque limits remain; cancellation and faults stop immediately. Longitudinal control is separate.',
      cdescr='仅LX3且自动变道关闭(-1)：单侧转向灯约1秒减弱辅助。CarState确认关灯且明显转向力释放后约0.2秒恢复。轻握保持循迹，明显用力让行。需转角小于15度，或速度至少8m/s、转角小于85度、转向速率不超过12度/秒稳定0.2秒。低速转弯或继续用力则等待。实体开关关闭后可能有原灯信号滤波延迟。权限、正常CAN及原角度/扭矩限制优先，取消和故障立即停止，纵向独立。')
    sections.append(('manual_steering','깜빡이 수동조향','Manual steering',['ManualSteerWithBlinker']))
  # Reuse the existing switch; do not introduce an unknown native Params key
  # or a virtual preference that older rollback readers would reject.
  if 'EnableCornerRadar' in definitions:
    option = definitions['EnableCornerRadar']
    description = 'LX3에서는 순정 모듈의 표시 대상·유형·숨김을 보존하고, 양쪽 순정 형식이 완전히 비어 있을 때 이전처럼 신선한 레이더의 전방 측방 대상을 1초 이상 연속 추적한 뒤 일반 회색 대상으로 보완합니다. 정지·저속 대상도 표시하지만 차량 분류가 아니어서 벽·분리대 등 구조물이 섞일 수 있습니다. 차선 인식이나 이동속도3m/s를 별도 표시 조건으로 삼지 않습니다. 오파 화면에서는 실제 송신과 현재 레이더가 맞는 미분류 대상을 중립 마커로 표시하며, 유형이 확인된 순정 대상은 해당 아이콘을 유지합니다. 신선한 영상·위치·보정이 없거나 전방 카메라 밖이면 그리지 않습니다. 0은 영상 표시 끄기, 1은 켜기, 2는 기존 끼어들기 검출도 유지합니다. 차선 내 거리두기는 별도 조건을 따르며 이 표시나 화면 좌표를 제어 입력으로 사용하지 않습니다.'
    option.update(title='주변 대상 상시 표시', etitle='Surrounding object display',
      ctitle='周围目标持续显示', descr=description,
      edescr='LX3 preserves OEM display types and hidden states, supplementing wholly empty front-side slots with the previous fresh, continuously tracked (at least one second) radar occupancy. Stationary and slow objects are included; walls and barriers can also appear because this is not vehicle classification. Lane classification and a >3m/s speed gate are not extra display requirements. Unclassified actual-transmission targets corroborated by current radar use neutral camera markers; classified OEM targets retain their icons. Stale, uncalibrated and off-camera targets are hidden. 0 disables the overlay; 1 enables it; 2 retains existing cut-in behavior. Within-lane adjustment has separate requirements and never consumes these screen coordinates.',
      cdescr='LX3保留原厂显示类型和隐藏状态，空白前侧方项恢复以前的雷达占用显示：要求新鲜且连续跟踪至少1秒。静止及低速目标也显示，墙壁和隔离带也可能出现，因为这不是车辆分类；车道识别和大于3m/s速度不是额外显示条件。实际发出且与当前雷达匹配的未分类目标使用中性相机标记，原厂已分类目标保留对应图标。过期、未标定或相机外目标不绘制。0关闭，1开启，2保留既有切入检测。车道内微调有独立条件，不使用这些屏幕坐标。')
    sections.insert(0, ('surround_display','주변 대상 표시','Surrounding objects',['EnableCornerRadar']))
  border_name = 'NotificationBorderPulse'
  if border_name not in definitions:
    option = {'name':border_name,'title':'비긴급 알림 화면 테두리','etitle':'Non-critical notification border',
      'ctitle':'普通提示边框','group':'개발 중 기능','min':0,'max':1,'default':1,'unit':1,'virtual':True,
      'descr':'일반 알림과 주황색 주의 경고 등 비긴급 알림 때 화면 테두리를 주황색으로 초당1회 강조합니다. 짧은 알림도 시작부터0.5초 표시하며 부저·진동과 독립입니다. 긴급 경고의 빨간 테두리와 기존 경고 문구·중요 경고음은 이 설정을 꺼도 유지합니다. 실제 진동 모터 반응은 별도 검증이 필요합니다.',
      'edescr':'Independently flashes the border amber once per second for non-critical alerts, including routine notifications and user prompts. Short notifications receive an onset-anchored half-second flash. Critical red emphasis, original alert text and protected sounds remain even when this is OFF. Physical wheel-motor response remains unverified.',
      'cdescr':'普通提示每秒闪烁一次橙色边框，与声音和振动独立。关闭也保留紧急红色边框、原警告文本和重要警告音。实际方向盘振动反应仍待验证。'}
    data.setdefault('params', []).append(option)
    definitions[border_name] = option
  notification_options = (border_name, 'C3xLiteBuzzerMute', 'C3xLiteHapticNotifications') + tuple(
    name for name in definitions if name.startswith('C3xLiteHaptic') and name != 'C3xLiteHapticNotifications')
  if 'C3xLiteBuzzerMute' in definitions:
    definitions['C3xLiteBuzzerMute'].update(
      title='일반 부저·알림음 끄기', etitle='Mute routine notification sounds', ctitle='关闭普通提示音',
      descr='C3xLite 본체 옵션과 독립입니다. 켜면 일반 알림의 기기 부저·스피커 소리를 끕니다. 핸들 진동 알림은 별도 설정이므로 둘 다 켜거나 진동만 사용할 수 있습니다. 제어 해제·제어 거부·운전자 주의·긴급 경고 등 기존 보호 경고음은 유지합니다.',
      edescr='Independent of the C3xLite hardware selector. ON mutes routine device buzzer/speaker notifications only. Wheel-haptic notifications have their own switch; either, both or neither routine output may be selected. Existing protected disengagement, refusal, attention and urgent warnings remain audible.',
      cdescr='与C3xLite硬件选项独立。开启仅关闭普通蜂鸣器和扬声器提示音。方向盘振动有独立开关，可同时开启、仅开启一种或均关闭。原有退出、拒绝、注意和紧急警告音保留。')
  if 'C3xLiteHapticNotifications' in definitions:
    definitions['C3xLiteHapticNotifications'].update(
      title='일반 알림 핸들 진동', etitle='Routine wheel-haptic notifications', ctitle='普通提示方向盘振动',
      descr='LX3 실험 기능입니다. C3xLite 본체·일반 부저 끄기와 독립적으로 일반 알림 때 기존 VIBRATE 신호를 요청합니다. 조향토크로 진동을 만들지 않으며 실제 진동모터 반응은 아직 미검증입니다. 유효한 CAN·활성 제어가 필요하고 깜빡이 때는 요청하지 않습니다. 과속카메라 햅틱은 별도 설정입니다.',
      edescr='Experimental LX3 feature, independent of the C3xLite hardware selector and routine sound mute. Requests the existing VIBRATE field for routine alerts, never oscillatory steering torque. Physical motor response is unverified. Requires healthy CAN and active control; suppressed with blinkers. Speed-camera haptics use a separate setting.',
      cdescr='LX3实验功能，与C3xLite硬件和普通提示音关闭选项独立。普通提示请求既有VIBRATE字段，不振荡转向扭矩；实际振动电机反应未验证。需正常CAN和有效控制，转向灯亮时不请求。测速摄像头振动是独立设置。')
  if any(name in definitions for name in notification_options):
    sections.insert(0, ('notification_methods', '테두리·부저·핸들 진동 알림', 'Notification methods', notification_options))
  development = {name for _,_,_,items in sections for name in items if name in definitions}
  for name in development:
    definitions[name].update(group='개발 중 기능',egroup='Development features',cgroup='Development features')
  if development and isinstance(data.get('menu'),list):
    def move(nodes):
      out=[]
      for node in nodes:
        if node.get('id') in ('lx3_preferences','development_features'):continue
        previous=node.get('params',[])
        node['params']=[name for name in previous if name not in development]
        children=node.get('groups',[])
        if children:node['groups']=move(children)
        if previous or children:
          if not node.get('params') and not node.get('groups'):continue
        out.append(node)
      return out
    data['menu']=move(data['menu'])
    groups=[{'id':sid,'ko':ko,'en':en,'zh':en,'params':[n for n in items if n in definitions]}
            for sid,ko,en,items in sections if any(n in definitions for n in items)]
    data['menu'].insert(0,{'id':'development_features','ko':'개발 중 기능','en':'Development features',
      'zh':'Development features','groups':[{'id':'lx3_preferences_group','ko':'신기능 모아보기',
        'en':'New features','zh':'New features','groups':groups}]})
  return data


# mtime-based cache for carrot_settings.json
# - "path" is mutated by carrot_server.py at startup if --settings is passed
# - "mtime" tracks the last loaded file mtime so reload happens only on change
settings_cache: dict = {
  "path": DEFAULT_SETTINGS_PATH,
  "mtime": 0,
  "data": None,        # full json
  "groups": None,      # {group: [param,...]}
  "by_name": None,     # {name: param}
  "groups_list": None, # [{group, egroup, count}, ...]
  "categories": None,  # 대>중>소 트리 ([{id,ko,en,zh,groups:[...]}]) or None when no "menu"
}


def read_settings_file(path: str) -> Dict[str, Any]:
  with open(path, "r", encoding="utf-8") as f:
    return json.load(f)


def group_index(settings: Dict[str, Any]) -> Tuple[Dict[str, list], Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
  groups: Dict[str, list] = {}
  by_name: Dict[str, Dict[str, Any]] = {}
  groups_list: List[Dict[str, Any]] = []

  params = settings.get("params", [])
  for p in params:
    g = p.get("group", "기타")
    if g == "기타":
        if "egroup" not in p: p["egroup"] = "Other"
        if "cgroup" not in p: p["cgroup"] = "其他"

    groups.setdefault(g, []).append(p)
    n = p.get("name")
    if n:
      by_name[n] = p

  # group list with egroup/cgroup guess
  for g, items in groups.items():
    egroup = None
    cgroup = None
    for it in items:
      if not egroup and it.get("egroup"):
        egroup = it.get("egroup")
      if not cgroup and it.get("cgroup"):
        cgroup = it.get("cgroup")
      if egroup and cgroup:
        break
    groups_list.append({"group": g, "egroup": egroup, "cgroup": cgroup, "count": len(items)})

  return groups, by_name, groups_list


def _label(node: Dict[str, Any]) -> Dict[str, Any]:
  return {"ko": node.get("ko"), "en": node.get("en"), "zh": node.get("zh")}


def build_menu_categories(data: Dict[str, Any], by_name: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]] | None:
  """Build the 대>중>소 tree from the optional top-level "menu" block.

  Returns None when no "menu" is present so the frontend falls back to the
  flat group_index view. Leaf "items" carry param *names* only; the frontend
  resolves definitions from items_by_group to avoid duplicating param data.

  Shape:
    [ {id, ko, en, zh,
       groups: [ {id, ko, en, zh, count,
                  sections: [ {id, ko, en, zh, items:[name,...]} ]} ]} ]

  A 중-group whose menu node holds params directly (no nested "groups") is
  normalized to a single label-less section.
  """
  menu = data.get("menu")
  if not menu:
    return None

  def join_labels(nodes: List[Dict[str, Any]], key: str) -> str | None:
    parts = [str(n.get(key) or "").strip() for n in nodes]
    parts = [p for p in parts if p]
    return " · ".join(parts) if parts else None

  def sections_from(nodes: List[Dict[str, Any]], parents: List[Dict[str, Any]] | None = None) -> List[Dict[str, Any]]:
    sections: List[Dict[str, Any]] = []
    parents = parents or []
    for node in nodes:
      path = parents + [node]
      children = node.get("groups") or []
      if children:
        sections.extend(sections_from(children, path))
        continue
      items = [n for n in node.get("params", []) if n in by_name]
      if not items:
        continue
      sections.append({
        "id": "__".join(str(n.get("id") or "") for n in path if n.get("id")),
        "ko": join_labels(path, "ko"),
        "en": join_labels(path, "en"),
        "zh": join_labels(path, "zh"),
        "items": items,
      })
    return sections

  cats: List[Dict[str, Any]] = []
  for cat in menu:
    groups_out: List[Dict[str, Any]] = []
    for grp in cat.get("groups", []):
      if "groups" in grp:
        sections = sections_from(grp["groups"])
      else:
        # params directly under the 중-group → single label-less section
        sections = [{"id": grp.get("id"), "ko": None, "en": None, "zh": None,
                     "items": [n for n in grp.get("params", []) if n in by_name]}]
      count = sum(len(s["items"]) for s in sections)
      groups_out.append({**_label(grp), "id": grp.get("id"), "count": count, "sections": sections})
    cats.append({**_label(cat), "id": cat.get("id"), "groups": groups_out})
  return cats


def get_settings_cached() -> Tuple[Dict[str, Any], Dict[str, list], Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
  path = settings_cache["path"]
  st = os.stat(path)
  mtime = int(st.st_mtime)
  if settings_cache["data"] is None or settings_cache["mtime"] != mtime:
    data = inject_driving_preferences(read_settings_file(path))
    groups, by_name, groups_list = group_index(data)
    settings_cache.update({
      "mtime": mtime,
      "data": data,
      "groups": groups,
      "by_name": by_name,
      "groups_list": groups_list,
      "categories": build_menu_categories(data, by_name),
    })
  return (
    settings_cache["data"],
    settings_cache["groups"],
    settings_cache["by_name"],
    settings_cache["groups_list"],
  )
