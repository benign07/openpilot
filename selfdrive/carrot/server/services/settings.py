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
  if names and isinstance(data.get('menu'), list):
    data['menu'].append({'id': 'lx3_preferences', 'ko': 'LX3 주행 보완', 'en': 'LX3 preferences',
      'zh': 'LX3 preferences', 'groups': [{'id': 'lx3_preferences_group', 'ko': '시험·기록',
        'en': 'Test and record', 'zh': 'Test and record', 'params': names}]})
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
