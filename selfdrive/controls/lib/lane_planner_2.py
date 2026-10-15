import math
import numpy as np
from cereal import log
from openpilot.common.filter_simple import FirstOrderFilter
from openpilot.common.realtime import DT_MDL
# from openpilot.common.swaglog import cloudlog
# from openpilot.common.logger import sLogger
from openpilot.common.params import Params

TRAJECTORY_SIZE = 33
# positive numbers go right
CAMERA_OFFSET = 0 #0.08
MIN_LANE_DISTANCE = 2.6
MAX_LANE_DISTANCE = 3.7
MAX_LANE_CENTERING_AWAY = 1.85
KEEP_MIN_DISTANCE_FROM_LANE = 1.35
KEEP_MIN_DISTANCE_FROM_EDGELANE = 1.15

def clamp(num, min_value, max_value):
  # weird broken case, do something reasonable
  if min_value > num > max_value:
    return (min_value + max_value) * 0.5
  # ok, basic min/max below
  if num < min_value:
    return min_value
  if num > max_value:
    return max_value
  return num

def sigmoid(x, scale=1, offset=0):
  return (1 / (1 + math.exp(x*scale))) + offset

def lerp(start, end, t):
  t = clamp(t, 0.0, 1.0)
  return (start * (1.0 - t)) + (end * t)

def max_abs(a, b):
  return a if abs(a) > abs(b) else b


class LateralMargin:
  """Small optional path preference, not obstacle avoidance or free-space proof.

  Model path y is right-positive; radar yRel/dPath are left-positive. OEM side
  occupancy vetoes movement toward that side; display icons/vehicle sizes never
  enter this calculation. Only previously verified forward tracks trigger an
  away preference. Rear/alongside radar coverage and CAN color age are unknown.
  """
  def __init__(self, dt=DT_MDL):
    self.dt = dt
    self.value = self.target = 0.0
    self.reason = 'off'
    self.tracks = {}
    self.last_radar = 0
    self.yellow_since = None
    self.last_steer = -10_000_000_000
    self.hold = [0, 0]
    self.previous_shadow = None

  def reset(self, reason):
    self.value = self.target = 0.0
    self.reason = reason
    self.tracks.clear()
    self.last_radar = 0
    self.yellow_since = None
    self.hold = [0, 0]
    return 0.0

  def update(self, settings, radar, *, now_ns, radar_ns, valid, lane_mode, lat_active,
             blinkers, steering_pressed, maneuver, speed, curvature, left_code,
             right_code, left_occupied, right_occupied, probabilities, stds,
             corridor, legacy_offset):
    yellow_cm, nearby_cm = settings['YellowLineBiasCm'], settings['NearbyVehicleBiasCm']
    shadow = bool(settings['LateralMarginShadow'])
    if not yellow_cm and not nearby_cm:
      return self.reset('off')
    if self.previous_shadow != shadow:
      self.previous_shadow = shadow
      return self.reset('shadow_mode_changed')
    if not lat_active or not lane_mode:
      return self.reset('inactive_laneless_or_control_off')
    if not valid or not 0 <= now_ns-radar_ns <= 200_000_000:
      return self.reset('stale_or_invalid_inputs')
    if steering_pressed:self.last_steer = now_ns
    if (len(probabilities)!=2 or len(stds)!=2 or not corridor or
        not all(math.isfinite(v) for v in (*probabilities, *stds, speed, curvature, legacy_offset)) or
        min(probabilities)<0.8 or max(stds)>0.15 or not 30/3.6<=speed<=110/3.6 or abs(curvature)>0.001):
      return self.reset('lane_or_curve_unqualified')
    # Every row is (left/right line y, final base path y, path heading), aligned
    # spatially for the now..2s path. A conservative2.24m lateral envelope plus
    #0.30m line margin and heading projection is used; not a mirror-clearance proof.
    lower, upper = -0.10, 0.10
    for left, right, path, yaw in corridor:
      if not all(math.isfinite(v) for v in (left,right,path,yaw)) or not 3.0<=right-left<=4.0:
        return self.reset('corridor_unqualified')
      footprint = 1.12+2.53*abs(math.sin(yaw))
      lower = max(lower, left+footprint+0.30-path)
      upper = min(upper, right-footprint-0.30-path)
    # Do not repair an already out-of-corridor stock path through this preference.
    # No new term is permitted to extend the legacy total0.4m offset bound.
    lower, upper = max(lower,-0.4-legacy_offset), min(upper,0.4-legacy_offset)
    if not lower<=0.0<=upper:
      return self.reset('base_path_outside_margin')
    occupied = [bool(left_occupied), bool(right_occupied)]
    verified = [False, False]
    uncertain = False
    new_tracks = {}
    new_radar = radar_ns != self.last_radar
    for side, leads in enumerate((radar.leadsLeft, radar.leadsRight)):
      sign = 1 if side==0 else -1
      for lead in leads:
        if not lead.status or not lead.radar or lead.radarTrackId<0:continue
        values = (lead.dRel,lead.dPath,lead.yRel,lead.vLead,lead.vRel,lead.vLat)
        if not all(math.isfinite(v) for v in values):
          uncertain=True;continue
        if not 0<lead.dRel<50:continue
        # A sign-binned track is not automatically an adjacent-lane vehicle.
        if lead.dPath*sign<=0 or lead.yRel*sign<=0:
          uncertain=True;continue
        if not 2.0<lead.dPath*sign<5.0:continue
        occupied[side]=True
        key=(side,int(lead.radarTrackId))
        previous=self.tracks.get(key)
        since=radar_ns
        if previous:
          stamp,start,d,path,velocity=previous
          dt=(radar_ns-stamp)/1e9
          if (0<=dt<=0.2 and abs(lead.dRel-d-lead.vRel*dt)<1.5
              and abs(lead.dPath-path)<0.5 and abs(lead.vLead-velocity)<5.0):
            since=start
          else:
            self.hold[side]=0  # A reused radar slot is not the old vehicle.
        new_tracks[key]=(radar_ns,since,lead.dRel,lead.dPath,lead.vLead)
        if (5.0<lead.dRel<25.0 and lead.vLead>3.0 and abs(lead.vLat)<0.8
            and radar_ns-since>=1_000_000_000):
          verified[side]=True
          if new_radar:self.hold[side]=now_ns+3_000_000_000
    self.tracks = new_tracks
    self.last_radar = radar_ns
    if uncertain or any(t.status and (not math.isfinite(t.dRel) or 0<t.dRel<40) for t in radar.leadsCutIn):
      return self.reset('side_geometry_or_cutin_uncertain')
    # Retain a just-observed same-direction car briefly as it leaves front radar.
    # OEM occupancy prevents a contradictory movement, not a metric distance.
    away = [verified[i] or now_ns<self.hold[i] for i in (0,1)]
    occupied = [occupied[i] or away[i] for i in (0,1)]
    yellow = (left_code in (20,21,24,25) and right_code//10!=2 and not occupied[0]
              and legacy_offset<=0.0 and curvature>=-0.0002)
    if yellow:
      if self.yellow_since is None:self.yellow_since=now_ns
    else:self.yellow_since=None
    self.target=0.0;self.reason='no_persistent_target'
    if blinkers or maneuver or now_ns-self.last_steer<3_000_000_000:
      self.reason='maneuver_or_driver_holdoff'
      self.yellow_since=None
      self.hold=[0,0]
    elif all(occupied):
      self.reason='both_sides_occupied'
    elif nearby_cm and away[0] and not occupied[1]:
      self.target=nearby_cm*0.01;self.reason='away_from_left_vehicle'
    elif nearby_cm and away[1] and not occupied[0]:
      self.target=-nearby_cm*0.01;self.reason='away_from_right_vehicle'
    elif yellow_cm and self.yellow_since is not None and now_ns-self.yellow_since>=2_000_000_000:
      self.target=-yellow_cm*0.01;self.reason='toward_left_yellow'
    # Occupancy/corridor constraints override the smoothing history.
    if occupied[0]:lower=max(0.0,lower)
    if occupied[1]:upper=min(0.0,upper)
    self.target=max(lower,min(upper,self.target))
    rate=0.02 if abs(self.target)>abs(self.value) and self.target*self.value>=0 else 0.05
    self.value += max(-rate*self.dt,min(rate*self.dt,self.target-self.value))
    self.value=max(lower,min(upper,self.value))
    return 0.0 if shadow else self.value

class LanePlanner:
  def __init__(self):
    self.ll_t = np.zeros((TRAJECTORY_SIZE,))
    self.ll_x = np.zeros((TRAJECTORY_SIZE,))
    self.lll_y = np.zeros((TRAJECTORY_SIZE,))
    self.rll_y = np.zeros((TRAJECTORY_SIZE,))
    self.le_y = np.zeros((TRAJECTORY_SIZE,))
    self.re_y = np.zeros((TRAJECTORY_SIZE,))
    #self.lane_width_estimate = FirstOrderFilter(3.2, 9.95, DT_MDL)
    self.lane_width_estimate = FirstOrderFilter(3.2, 3.0, DT_MDL)
    self.lane_width = 3.2
    self.lane_width_last = self.lane_width
    self.lane_change_multiplier = 1
    #self.lane_width_updated_count = 0

    self.lll_prob = 0.
    self.rll_prob = 0.
    self.d_prob = 0.

    self.lll_std = 0.
    self.rll_std = 0.

    self.l_lane_change_prob = 0.
    self.r_lane_change_prob = 0.

    self.debugText = ""
    self.lane_width_left = 0.0
    self.lane_width_right = 0.0
    self.lane_width_left_filtered = FirstOrderFilter(1.0, 1.0, DT_MDL)
    self.lane_width_right_filtered = FirstOrderFilter(1.0, 1.0, DT_MDL)
    self.lane_offset_filtered = FirstOrderFilter(0.0, 2.0, DT_MDL)

    self.lanefull_mode = False
    self.d_prob_count = 0

    self.params = Params()

  def parse_model(self, md):

    lane_lines = md.laneLines
    edges = md.roadEdges

    if len(lane_lines) >= 4 and len(lane_lines[0].t) == TRAJECTORY_SIZE:
      self.ll_t = (np.array(lane_lines[1].t) + np.array(lane_lines[2].t))/2
      # left and right ll x is the same
      self.ll_x = lane_lines[1].x
      self.lll_y = np.array(lane_lines[1].y)
      self.rll_y = np.array(lane_lines[2].y)
      self.lll_prob = md.laneLineProbs[1]
      self.rll_prob = md.laneLineProbs[2]
      self.lll_std = md.laneLineStds[1]
      self.rll_std = md.laneLineStds[2]

    if len(edges[0].t) == TRAJECTORY_SIZE:
      self.le_y = np.array(edges[0].y) + md.roadEdgeStds[0] * 0.4
      self.re_y = np.array(edges[1].y) - md.roadEdgeStds[1] * 0.4
    else:
      self.le_y = self.lll_y
      self.re_y = self.rll_y

    desire_state = md.meta.desireState
    if len(desire_state):
      self.l_lane_change_prob = desire_state[log.Desire.laneChangeLeft]
      self.r_lane_change_prob = desire_state[log.Desire.laneChangeRight]

  def get_d_path(self, CS, v_ego, path_t, path_xyz, curve_speed):
    #if v_ego > 0.1:
    #  self.lane_width_updated_count = max(0, self.lane_width_updated_count - 1)
    # Reduce reliance on lanelines that are too far apart or
    # will be in a few seconds
    l_prob, r_prob = self.lll_prob, self.rll_prob
    width_pts = self.rll_y - self.lll_y
    prob_mods = []
    for t_check in (0.0, 1.5, 3.0):
      width_at_t = np.interp(t_check * (v_ego + 7), self.ll_x, width_pts)
      #prob_mods.append(np.interp(width_at_t, [4.0, 5.0], [1.0, 0.0]))
      prob_mods.append(np.interp(width_at_t, [4.5, 6.0], [1.0, 0.0]))
    mod = min(prob_mods)
    l_prob *= mod
    r_prob *= mod

    # Reduce reliance on uncertain lanelines
    l_std_mod = np.interp(self.lll_std, [.15, .3], [1.0, 0.0])
    r_std_mod = np.interp(self.rll_std, [.15, .3], [1.0, 0.0])
    l_prob *= l_std_mod
    r_prob *= r_std_mod

    self.l_prob, self.r_prob = l_prob, r_prob

    # Find current lanewidth
    current_lane_width = abs(self.rll_y[0] - self.lll_y[0])

    max_updated_count = 10.0 * DT_MDL
    both_lane_available = False
    #speed_lane_width = np.interp(v_ego*3.6, [0., 60.], [2.8, 3.5])
    if l_prob > 0.5 and r_prob > 0.5 and self.lane_change_multiplier > 0.5:
      both_lane_available = True
      #self.lane_width_updated_count = max_updated_count
      self.lane_width_estimate.update(current_lane_width)
      self.lane_width_last = self.lane_width_estimate.x
    #elif self.lane_width_updated_count <= 0 and v_ego > 0.1:   # 양쪽차선이 없을때.... 일정시간후(10초)부터 speed차선폭 적용함.
    #  self.lane_width_estimate.update(speed_lane_width)
    else:
      self.lane_width_estimate.update(self.lane_width_last)

    self.lane_width =  self.lane_width_estimate.x
    clipped_lane_width = min(4.0, self.lane_width)
    path_from_left_lane = self.lll_y + clipped_lane_width / 2.0
    path_from_right_lane = self.rll_y - clipped_lane_width / 2.0

    # 가장 차선이 진한쪽으로 골라서..
    self.d_prob = max(l_prob, r_prob) if not both_lane_available else 1.0

    # 좌/우의 차선폭을 필터링.
    if self.lane_width_left > 0:
      self.lane_width_left_filtered.update(self.lane_width_left)
      #self.lane_width_left_filtered.x = self.lane_width_left #바로적용
    if self.lane_width_right > 0:
      self.lane_width_right_filtered.update(self.lane_width_right)
      #self.lane_width_right_filtered.x = self.lane_width_right #바로적용

    self.adjustLaneOffset = float(self.params.get_int("AdjustLaneOffset")) * 0.01
    self.adjustCurveOffset = self.adjustLaneOffset #float(self.params.get_int("AdjustCurveOffset")) * 0.01
    ADJUST_OFFSET_LIMIT = 0.4 #max(self.adjustLaneOffset, self.adjustCurveOffset)
    offset_curve = 0.0
    ## curve offset
    offset_curve = np.interp(abs(curve_speed), [50, 200], [self.adjustCurveOffset, 0.0]) * np.sign(curve_speed)

    offset_lane = 0.0
    if self.lane_width_left_filtered.x > 2.2 and self.lane_width_right_filtered.x > 2.2: #양쪽에 차로가 여유 있는경우
      offset_lane = 0.0
    elif self.lane_width_left_filtered.x < 2.0 and self.lane_width_right_filtered.x < 2.0: #양쪽에 차로가 여유 없는경우
      offset_lane = 0.0
    elif self.lane_width_left_filtered.x > self.lane_width_right_filtered.x:
      offset_lane = np.interp(self.lane_width, [2.5, 2.9], [0.0, self.adjustLaneOffset]) # 차선이 좁으면 안함..
    else:
      offset_lane = np.interp(self.lane_width, [2.5, 2.9], [0.0, -self.adjustLaneOffset]) # 차선이 좁으면 안함..

    #select lane path
    # 차선이 좁아지면, 도로경계쪽에 있는 차선 위주로 따라가도록함.
    if self.lane_width < 2.5:
      if r_prob > 0.5 and self.lane_width_right_filtered.x < self.lane_width_left_filtered.x:
        lane_path_y = path_from_right_lane
      elif l_prob > 0.5 and self.lane_width_left_filtered.x < 2.0:
        lane_path_y = path_from_left_lane
      else:
        lane_path_y = path_from_left_lane if l_prob > 0.5 or l_prob > r_prob else path_from_right_lane
    elif l_prob > 0.7 and r_prob > 0.7:
      lane_path_y = (path_from_left_lane + path_from_right_lane) / 2.
      # lane_width filtering에 의해서, 점점 줄어들때, 중앙선으로 붙어가는 현상이 생김.. 
      #if self.lane_width > 3.2:
      #  lane_path_y = path_from_right_lane
      #else:
      #  lane_path_y = (path_from_left_lane + path_from_right_lane) / 2.
    # 그외 진한차선을 따라가도록함.
    else:
      lane_path_y = (l_prob * path_from_left_lane + r_prob * path_from_right_lane) / (l_prob + r_prob + 0.0001)

    use_laneless_center_adjust = False
    if use_laneless_center_adjust:
      ## 0.5초 앞의 중심을 보도록함.
      lane_path_y_center = np.interp(0.5, path_t, lane_path_y)
      path_xyz_y_center = np.interp(0.5, path_t, path_xyz[:,1])
      #lane_path_y_center = lane_path_y[0]
      #path_xyz_y_center = path_xyz[:,1][0]
      diff_center = (lane_path_y_center - path_xyz_y_center) if not self.lanefull_mode else 0.0
    else:
      diff_center = 0.0
    #print("center = {:.2f}={:.2f}-{:.2f}, lanefull={}".format(diff_center, lane_path_y_center, path_xyz_y_center, self.lanefull_mode))
    #diff_center = lane_path_y[5] - path_xyz[:,1][5] if not self.lanefull_mode else 0.0
    if offset_curve * offset_lane < 0:
      offset_total = np.clip(offset_curve + offset_lane + diff_center, - ADJUST_OFFSET_LIMIT, ADJUST_OFFSET_LIMIT)
    else:
      offset_total = np.clip(max(offset_curve, offset_lane, key=abs) + diff_center, - ADJUST_OFFSET_LIMIT, ADJUST_OFFSET_LIMIT)

    ## self.d_prob = 0 if lane_changing
    self.d_prob *= self.lane_change_multiplier  ## 차선변경중에는 꺼버림.
    if self.lane_change_multiplier < 0.5:
      #self.lane_offset_filtered.x = 0.0
      pass
    else:
      self.lane_offset_filtered.update(np.interp(self.d_prob, [0, 0.3], [0, offset_total]))

    ## laneless at lowspeed
    self.d_prob *= np.interp(v_ego*3.6, [5., 10.], [0.0, 1.0])

    #self.debugText = "OFFSET({:.2f}={:.2f}+{:.2f}+{:.2f}),Vc:{:.2f},dp:{:.1f},lf:{},lrw={:.1f}|{:.1f}|{:.1f}".format(
    #  self.lane_offset_filtered.x,
    #  diff_center, offset_lane, offset_curve,
    #  curve_speed,
    #  self.d_prob, self.lanefull_mode,
    #  self.lane_width_left_filtered.x, self.lane_width, self.lane_width_right_filtered.x)

    adjustLaneTime = self.params.get_float("LatMpcInputOffset") * 0.01 # 0.06 
    laneline_active = False
    self.d_prob_count = self.d_prob_count + 1 if self.d_prob > 0.3 else 0
    if self.lanefull_mode and self.d_prob_count > int(1 / DT_MDL):
      laneline_active = True
      use_dist_mode = False  ## 아무리생각해봐도.. 같은 방법인듯...
      if use_dist_mode:
        lane_path_y_interp = np.interp(path_xyz[:,0] + v_ego * adjustLaneTime, self.ll_x, lane_path_y)
        path_xyz[:,1] = self.d_prob * lane_path_y_interp + (1.0 - self.d_prob) * path_xyz[:,1]
      else:
        safe_idxs = np.isfinite(self.ll_t)
        if safe_idxs[0]:
          lane_path_y_interp = np.interp(path_t * (1.0 + adjustLaneTime), self.ll_t[safe_idxs], lane_path_y[safe_idxs])
          path_xyz[:,1] = self.d_prob * lane_path_y_interp + (1.0 - self.d_prob) * path_xyz[:,1]


    path_xyz[:, 1] += (CAMERA_OFFSET + self.lane_offset_filtered.x)

    self.offset_total = self.lane_offset_filtered.x

    return path_xyz, laneline_active

  def calculate_plan_yaw_and_yaw_rate(self, path_xyz):
    if path_xyz.shape[0] < 3:
        # 너무 짧으면 직진 가정
        N = path_xyz.shape[0]
        return np.zeros(N), np.zeros(N)

    # x, y 추출
    x = path_xyz[:, 0]
    y = path_xyz[:, 1]

    # 모두 동일한 점인지 확인
    if np.allclose(x, x[0]) and np.allclose(y, y[0]):
        return np.zeros(len(x)), np.zeros(len(x))

    # 안전한 diff 계산
    dx = np.diff(x)
    dy = np.diff(y)
    mask = (dx == 0) & (dy == 0)
    dx[mask] = 1e-4
    dy[mask] = 0.0

    yaw = np.arctan2(dy, dx)
    yaw = np.append(yaw, yaw[-1])  # N-1 → N
    yaw = np.unwrap(yaw)

    dx_full = np.clip(np.diff(x), 1e-4, None)
    yaw_rate = np.diff(yaw) / dx_full
    yaw_rate = np.append(yaw_rate, yaw_rate[-1])
    yaw_rate = np.append(yaw_rate, 0.0)

    # NaN/Inf 방어
    if np.any(np.isnan(yaw_rate)) or np.any(np.isinf(yaw_rate)):
        yaw_rate = np.zeros_like(yaw_rate)
    if np.any(np.isnan(yaw)) or np.any(np.isinf(yaw)):
        yaw = np.zeros_like(yaw)

    return yaw, yaw_rate
