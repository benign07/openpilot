import json
import os
from typing import Any, Dict, List, Tuple

from ..config import DEFAULT_SETTINGS_PATH
from openpilot.selfdrive.controls.lib.approach_comfort import DRIVING_PREFERENCES


def inject_driving_preferences(data):
  """Virtual options use their own versioned store, never unknown Params keys."""
  descriptions = {
    'LaunchFollowStrength': ('재출발 앞차 추종 보완',
      '정지 후 움직이기 시작한 뒤, 같은 레이더 앞차가 안정적으로 멀어지고 충분한 간격이 있을 때 기존 가속표를 이 비율(%) 이내에서 보완합니다. 정지 해제·최소거리·제동·Panda 권한은 바꾸지 않습니다. 접근 감속·끼어들기·가까운 두 번째 앞차에는 적용하지 않습니다. 0은 기존 동작, 시험 시작은10을 권장합니다. 최대 양가속1.6m/s²를 넘기는 보완은 하지 않습니다. 실제 출발 지연이나 간격 개선을 보장하지 않습니다.'),
    'YellowLineBiasCm': ('노란 중앙선 쪽 여유 조절',
      '왼쪽의 노란 차선이2초 이상 지속되고 양쪽 차선 신뢰도·차폭 여유가 충분할 때 차로 안에서 이 거리(cm) 이내로 왼쪽에 붙습니다. 오른쪽 노란선·양쪽 노란선·도로 경계·왼쪽 차량·중앙선 방향 커브에서는 해제합니다. 차로 추종 사용속도가0이면 실제 조향에는 반영되지 않습니다. 반대 차로와의 여유는 줄어드므로 안전 개선을 보장하는 기능이 아닙니다. 기본0, 첫 비교는5cm 이하 권장.'),
    'NearbyVehicleBiasCm': ('주변 차량 반대쪽 여유 조절',
      'HUD 차량 아이콘의 화면 좌표 대신 원본 레이더 추적과 순정 측방 점유 신호를 사용합니다. 위치·진행방향이 일치하는 주변 차량 반대쪽으로 차로 안에서 이 거리(cm) 이내 이동합니다. 양쪽 점유·불확실한 위치·운전자 개입·깜빡이·차선변경 때 해제합니다. 전방 레이더로 차량 옆과 뒤의 빈 공간을 보장할 수 없습니다. 차로 추종 사용속도가0이면 실제 조향에는 반영되지 않습니다. 기본0, 첫 비교는5cm 이하 권장.'),
    'LateralMarginShadow': ('측방 여유 기록만 하기',
      '켜면 노란선/주변 차량 이동 후보와 해제 이유를 기록하되 조향 경로에는 적용하지 않습니다. 기본 켜짐입니다. 끄고 이동량을 설정해야 실제 적용 후보가 됩니다. 실제 적용도 차로 모드·유효한 양쪽 차선·신선한 차량/레이더 정보가 필요합니다. 시험 옵션 변경은 P·속도0·제어 해제 상태에서만 가능합니다.'),
  }
  for name in ('YellowLineBiasCm','NearbyVehicleBiasCm'):
    title, text = descriptions[name]
    text = text.replace('차로 추종 사용속도가0이면 실제 조향에는 반영되지 않습니다.',
      '기록 전용을 끄고 이동량을 켜면30km/h부터 기존 차로 추종도 함께 사용합니다(기존 속도 히스테리시스±2km/h). 끄거나 기록 전용으로 바꾸면 원래 차로 추종 설정으로 돌아갑니다.')
    descriptions[name] = title, text
  title, text = descriptions['LateralMarginShadow']
  descriptions['LateralMarginShadow'] = title, text+' 기록 전용은 차로 추종이 꺼져 있어도 후보를 계산하며 경로를 바꾸지 않습니다.'
  existing = {p.get('name') for p in data.get('params', [])}
  names = []
  for name, (lo, hi, default) in DRIVING_PREFERENCES.items():
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
    ('launch_follow','재출발 추종','Launch following',['LaunchFollowStrength']),
    ('lateral_margin','차선 내 측방 여유','Within-lane margin',
      ['YellowLineBiasCm','NearbyVehicleBiasCm','LateralMarginShadow']),
    ('approach_comfort','부드러운 감속·차간거리','Approach comfort and gap',
      ['ApproachComfortEnabled','ApproachCoastStrength','ApproachBrakeMargin',
       'ApproachStopSmooth','ApproachTrafficCalm','ApproachFlexibleGap','ApproachFlexibleGapMin']),
  ]
  definitions = {p.get('name'):p for p in data.get('params',[])}
  # Reuse the existing switch; do not introduce an unknown native Params key
  # or a virtual preference that older rollback readers would reject.
  if 'EnableCornerRadar' in definitions:
    option = definitions['EnableCornerRadar']
    description = 'LX3에서는 오파 제어 여부와 무관하게 순정 주변 정보를 우선 표시합니다. 순정 항목이 비면 신선하고 1초 이상 안정적으로 추적된 전방 측방 레이더 대상을 회색 상자로 보완합니다. 후방·바로 옆의 전체 감지와 차종 인식은 보장하지 않습니다. 0은 보완 끄기, 1은 표시 보완, 2는 기존 끼어들기 검출 설정도 유지합니다. 차선 내 미세조정은 아래의 별도 옵션과 차선·제어 조건을 따릅니다.'
    option.update(title='주변 대상 상시 표시', etitle='Surrounding object display',
      ctitle='周围目标持续显示', descr=description,
      edescr='LX3 keeps OEM surrounding objects first, whether openpilot controls are active or not. Empty forward-side slots may use fresh radar tracks stable for at least one second, shown as generic gray objects. Full rear/alongside coverage and vehicle classification are not provided. 0 disables filling; 1 enables it; 2 also keeps existing cut-in detection. Within-lane adjustment has separate options and lane/control requirements.',
      cdescr='LX3优先保留原厂周围目标，不要求openpilot控制已启用。前侧空白位置可由持续稳定至少一秒的新鲜雷达轨迹补充为灰色通用目标。不提供完整侧后方覆盖或车型识别。0关闭补充，1开启补充，2同时保留现有切入检测。车道内微调仍需单独选项及车道和控制条件。')
    sections.insert(0, ('surround_display','주변 대상 표시','Surrounding objects',['EnableCornerRadar']))
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
