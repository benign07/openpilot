import time
import math
import re
import numpy as np
import pyray as rl
from cereal import log, messaging
from msgq.visionipc import VisionStreamType
from openpilot.selfdrive.ui import UI_BORDER_SIZE
from openpilot.selfdrive.ui.ui_state import ui_state, UIStatus
from openpilot.selfdrive.ui.onroad.alert_renderer import AlertRenderer
from openpilot.selfdrive.ui.onroad.driver_state import DriverStateRenderer
from openpilot.selfdrive.ui.onroad.hud_renderer import HudRenderer
from openpilot.selfdrive.ui.onroad.model_renderer import ModelRenderer
from openpilot.selfdrive.ui.onroad.cameraview import CameraView
from openpilot.system.ui.lib.application import gui_app
from openpilot.system.ui.lib.text_draw import draw_text_ui_style
from openpilot.common.transformations.camera import DEVICE_CAMERAS, DeviceCameraConfig, view_frame_from_device_frame
from openpilot.common.transformations.orientation import rot_from_euler

OpState = log.SelfdriveState.OpenpilotState
CALIBRATED = log.LiveCalibrationData.Status.calibrated
ROAD_CAM = VisionStreamType.VISION_STREAM_ROAD
WIDE_CAM = VisionStreamType.VISION_STREAM_WIDE_ROAD
DEFAULT_DEVICE_CAMERA = DEVICE_CAMERAS["tici", "ar0231"]

BORDER_COLORS = {
  UIStatus.DISENGAGED: rl.Color(0x12, 0x28, 0x39, 0xFF),  # Blue for disengaged state
  UIStatus.OVERRIDE: rl.Color(0x89, 0x92, 0x8D, 0xFF),  # Gray for override state
  UIStatus.ENGAGED: rl.Color(0x16, 0x7F, 0x40, 0xFF),  # Green for engaged state
}

WIDE_CAM_MAX_SPEED = 10.0  # m/s (22 mph)
ROAD_CAM_MIN_SPEED = 15.0  # m/s (34 mph)
INF_POINT = np.array([1000.0, 0.0, 0.0])


def _oem_bits(raw, start, size, little=True):
  if little:
    return (int.from_bytes(raw, 'little') >> start) & ((1 << size)-1)
  value = 0
  for _ in range(size):
    value = (value << 1) | ((raw[start//8] >> (start % 8)) & 1)
    start = start+15 if start % 8 == 0 else start-1
  return value


def _decode_oem_surround(address, raw):
  """Only actual OEM slots; 1EA cannot identify vehicle type. CRC checked by caller."""
  if len(raw) != 32 or address not in (0x1EA, 0x162):
    return {}
  layout = ({'LF':(74,46,70),'RF':(98,75,94),'LR':(162,139,152),'RR':(186,163,172)}
            if address == 0x1EA else {'LF':(112,117,128),'RF':(136,141,152),'LR':(163,175,176),'RR':(192,197,205)})
  result = {}
  for side, (typ, distance, lateral) in layout.items():
    code = _oem_bits(raw, typ, 3 if address == 0x1EA else 5, address != 0x1EA)
    # Keep hidden/unknown object codes distinct from a generic visible object.
    if not (1 <= code <= (3 if address == 0x1EA else 12)):
      if code:
        result[side] = None  # Hidden/unsupported class wins over a generic slot.
      continue
    d = .1*_oem_bits(raw, distance, 11 if side.endswith('F') else 8, not (address == 0x162 and side == 'LR'))
    lat = .1*_oem_bits(raw, lateral, 6 if address == 0x1EA and side.endswith('R') else 7,
                       not (address == 0x1EA and side.endswith('F')))
    if 0.0 < d <= 100.0 and 0.0 < lat <= 8.0:
      result[side] = {'distance':d,'lateral':lat,'class':0 if address == 0x1EA else code,
                      'white':code in (2,3) if address == 0x1EA else code % 2 == 0}
  return result


def _oem_margin_status(text):
  match = re.search(r'(?:^| \| )lx3_margin_v1=(-?\d+(?:\.\d+)?),([a-z_]+),([01])(?:$| \| )', text)
  if match is None:
    return None
  cm, reason, shadow = float(match[1]), match[2], match[3] == '1'
  if shadow or not math.isfinite(cm) or not .005 <= abs(cm) <= 10.001:
    return None
  if (reason == 'away_from_left_vehicle' and cm > 0 or
      reason == 'away_from_right_vehicle' and cm < 0):
    return cm
  return None


def _oem_comfort_status(text, now_ns):
  match = re.search(r'(?:^| \| )lx3_comfort_v1=(\d+),([0-7])(?:$| \| )', text)
  if match is None or not 0 <= now_ns-int(match[1]) <= 200_000_000:
    return 0
  return int(match[2])


class OemSurroundOverlay:
  """Small receive-only OEM relative-position map; never a control input."""
  def __init__(self):
    self.socket = None
    self.frames = {}
    self.last_poll = 0.0
    self.param_at = 0.0
    self.enabled = False
    self.bus = None
    self.checksum = None
    self.warned = False

  def render(self, rect):
    sm = ui_state.sm
    cp = ui_state.CP
    if cp is None or str(cp.carFingerprint) != 'HYUNDAI_PALISADE_LX3_HEV':
      return
    now = time.monotonic()
    if now >= self.param_at:
      self.enabled = ui_state.params.get_int('EnableCornerRadar') > 0
      self.param_at = now+2.0
    if not self.enabled:
      self.frames.clear()
      self.socket = None
    fresh = lambda k: sm.valid[k] and sm.alive[k] and 0 <= now-sm.logMonoTime[k]/1e9 <= .2
    if not fresh('carState') or not sm['carState'].canValid or sm['carState'].canTimeout:
      self.frames.clear()
      return
    if self.enabled and self.socket is None:
      from opendbc.car.hyundai.hyundaicanfd import CanBus, hkg_can_fd_checksum
      self.bus = CanBus(cp).CAM
      self.checksum = hkg_can_fd_checksum
      self.socket = messaging.sub_sock('can')
    if self.enabled and now-self.last_poll >= .04:
      self.last_poll = now
      # Bound work per rendered frame. Queued old messages cannot refresh a
      # slot because Event timestamps are checked, rather than receive time.
      for _ in range(12):
        event = messaging.recv_one_or_none(self.socket)
        if event is None:
          break
        stamp = int(event.logMonoTime)/1e9
        if not event.valid or not 0 <= time.monotonic()-stamp <= .2:
          continue
        for packet in event.can:
          if int(packet.src) != self.bus or packet.address not in (0x1EA, 0x162):
            continue
          raw = bytes(packet.dat)
          if len(raw) == 32 and int.from_bytes(raw[:2], 'little') == self.checksum(packet.address, None, raw):
            self.frames[int(packet.address)] = (stamp, _decode_oem_surround(int(packet.address), raw))
    targets = {}
    # Prefer the wider CCNC classes; do not infer a type from 1EA colour.
    for address in (0x1EA, 0x162):
      stamp, values = self.frames.get(address, (0.0, {}))
      if 0 <= now-stamp <= .2:
        targets.update(values)
    targets = {side:target for side,target in targets.items() if target is not None}
    cm = None
    comfort = 0
    if fresh('lateralPlan') and fresh('carControl') and sm['carControl'].latActive and sm['lateralPlan'].mpcSolutionValid:
      cm = _oem_margin_status(str(sm['lateralPlan'].latDebugText))
    if fresh('lateralPlan') and fresh('longitudinalPlan') and fresh('carControl') and sm['carControl'].longActive:
      comfort = _oem_comfort_status(str(sm['lateralPlan'].latDebugText), time.monotonic_ns())
    if not targets and cm is None and not comfort:
      return
    width, height = 235., 350.
    x, y = rect.x+rect.width-width-24., rect.y+rect.height-height-100.
    rl.draw_rectangle_rounded(rl.Rectangle(x, y, width, height), .12, 8, rl.Color(10,20,30,185))
    draw_text_ui_style('순정 주변', x+width/2, y+22, 20, rl.WHITE, align='center', shadow_offset=0)
    cx, cy = x+width/2, y+height*.60
    rl.draw_rectangle_lines_ex(rl.Rectangle(cx-11,cy-20,22,40), 2, rl.SKYBLUE)
    for side, target in targets.items():
      dx = min(84., max(38., target['lateral']*19.)) * (-1 if side.startswith('L') else 1)
      dy = min(104. if side.endswith('F') else 48., max(28., target['distance']*2.4)) * (-1 if side.endswith('F') else 1)
      tx, ty = cx+dx, cy+dy
      colour = rl.WHITE if target['white'] else rl.Color(165,175,185,255)
      truck = target['class'] in (5,6)
      rl.draw_rectangle_lines_ex(rl.Rectangle(tx-12,ty-15,24,30 if truck else 23), 2, colour)
      # Explicit type only exists in CCNC; all 1EA targets stay generic boxes.
      if truck:
        rl.draw_line(int(tx-10),int(ty+4),int(tx+10),int(ty+4),colour)
      draw_text_ui_style(f"{target['distance']:.0f}m",tx,ty+22,17,colour,align='center',shadow_offset=0)
    label = ('거리두기 적용 중 '+('→' if cm > 0 else '←')+f' {abs(cm):.1f}cm') if cm is not None else '거리두기 미적용'
    draw_text_ui_style(label,x+width/2,y+height-54,18,rl.GREEN if cm is not None else rl.LIGHTGRAY,
                       align='center',shadow_offset=0)
    if comfort:
      draw_text_ui_style('부드러운 감속 적용 중',x+width/2,y+height-32,18,rl.SKYBLUE,align='center',shadow_offset=0)
      parts = [name for bit,name in ((1,'타력'),(2,'미리 감속'),(4,'정차')) if comfort & bit]
      draw_text_ui_style(' · '.join(parts),x+width/2,y+height-12,16,rl.WHITE,align='center',shadow_offset=0)


class AugmentedRoadView(CameraView):
  def __init__(self, stream_type: VisionStreamType = VisionStreamType.VISION_STREAM_ROAD):
    super().__init__("camerad", stream_type)
    self._set_placeholder_color(BORDER_COLORS[UIStatus.DISENGAGED])

    self.device_camera: DeviceCameraConfig | None = None
    self.view_from_calib = view_frame_from_device_frame.copy()
    self.view_from_wide_calib = view_frame_from_device_frame.copy()

    self._matrix_cache_key = (0, 0.0, 0.0, stream_type)
    self._cached_matrix: np.ndarray | None = None
    self._content_rect = rl.Rectangle()

    self.model_renderer = ModelRenderer()
    self._hud_renderer = HudRenderer()
    self.alert_renderer = AlertRenderer()
    self.driver_state_renderer = DriverStateRenderer()
    self.oem_surround_overlay = OemSurroundOverlay()

    # debug
    self._pm = messaging.PubMaster(['uiDebug'])



  def _render(self, rect):
    # Only render when system is started to avoid invalid data access
    start_draw = time.monotonic()
    if not ui_state.started:
      return

    self._switch_stream_if_needed(ui_state.sm)

    # Update calibration before rendering
    self._update_calibration()

    # Create inner content area with border padding
    self._content_rect = rl.Rectangle(
      rect.x + UI_BORDER_SIZE,
      rect.y + UI_BORDER_SIZE,
      rect.width - 2 * UI_BORDER_SIZE,
      rect.height - 2 * UI_BORDER_SIZE,
    )

    # Enable scissor mode to clip all rendering within content rectangle boundaries
    # This creates a rendering viewport that prevents graphics from drawing outside the border
    rl.begin_scissor_mode(
      int(self._content_rect.x),
      int(self._content_rect.y),
      int(self._content_rect.width),
      int(self._content_rect.height)
    )

    # Render the base camera view
    super()._render(rect)

    # Draw all UI overlays
    self.model_renderer.render(self._content_rect)
    self._hud_renderer.render(self._content_rect)
    try:
      self.oem_surround_overlay.render(self._content_rect)
    except (AttributeError, KeyError, TypeError, ValueError, OSError) as exc:
      # Optional display failure must not take down the road/alert UI.
      self.oem_surround_overlay.frames.clear()
      if not self.oem_surround_overlay.warned:
        from openpilot.common.swaglog import cloudlog
        cloudlog.warning('OEM surround overlay unavailable: '+type(exc).__name__)
        self.oem_surround_overlay.warned = True
    self.alert_renderer.render(self._content_rect)
    self.driver_state_renderer.render(self._content_rect)

    # Custom UI extension point - add custom overlays here
    # Use self._content_rect for positioning within camera bounds

    # End clipping region
    rl.end_scissor_mode()

    # Draw colored border based on driving state
    self._draw_border_carrot(rect)

    # publish uiDebug
    msg = messaging.new_message('uiDebug')
    msg.uiDebug.drawTimeMillis = (time.monotonic() - start_draw) * 1000
    self._pm.send('uiDebug', msg)

  def _handle_mouse_press(self, _):
    if not self._hud_renderer.user_interacting() and self._click_callback is not None:
      self._click_callback()

  def _handle_mouse_release(self, _):
    # We only call click callback on press if not interacting with HUD
    pass

  def _draw_border(self, rect: rl.Rectangle):
    rl.draw_rectangle_lines_ex(rect, UI_BORDER_SIZE, rl.BLACK)
    border_roundness = 0.12
    border_color = BORDER_COLORS.get(ui_state.status, BORDER_COLORS[UIStatus.DISENGAGED])
    border_rect = rl.Rectangle(rect.x + UI_BORDER_SIZE, rect.y + UI_BORDER_SIZE,
                               rect.width - 2 * UI_BORDER_SIZE, rect.height - 2 * UI_BORDER_SIZE)
    rl.draw_rectangle_rounded_lines_ex(border_rect, border_roundness, 10, UI_BORDER_SIZE, border_color)

  def _switch_stream_if_needed(self, sm):
    if sm['selfdriveState'].experimentalMode and WIDE_CAM in self.available_streams:
      v_ego = sm['carState'].vEgo
      if v_ego < WIDE_CAM_MAX_SPEED:
        target = WIDE_CAM
      elif v_ego > ROAD_CAM_MIN_SPEED:
        target = ROAD_CAM
      else:
        # Hysteresis zone - keep current stream
        target = self.stream_type
    else:
      target = ROAD_CAM

    if self.stream_type != target:
      self.switch_stream(target)

  def _update_calibration(self):
    # Update device camera if not already set
    sm = ui_state.sm
    if not self.device_camera and sm.seen['roadCameraState'] and sm.seen['deviceState']:
      self.device_camera = DEVICE_CAMERAS[(str(sm['deviceState'].deviceType), str(sm['roadCameraState'].sensor))]

    # Check if live calibration data is available and valid
    if not (sm.updated["liveCalibration"] and sm.valid['liveCalibration']):
      return

    calib = sm['liveCalibration']
    if len(calib.rpyCalib) != 3 or calib.calStatus != CALIBRATED:
      return

    # Update view_from_calib matrix
    device_from_calib = rot_from_euler(calib.rpyCalib)
    self.view_from_calib = view_frame_from_device_frame @ device_from_calib

    # Update wide calibration if available
    if hasattr(calib, 'wideFromDeviceEuler') and len(calib.wideFromDeviceEuler) == 3:
      wide_from_device = rot_from_euler(calib.wideFromDeviceEuler)
      self.view_from_wide_calib = view_frame_from_device_frame @ wide_from_device @ device_from_calib

  def _calc_frame_matrix(self, rect: rl.Rectangle) -> np.ndarray:
    # Check if we can use cached matrix
    cache_key = (
      ui_state.sm.recv_frame['liveCalibration'],
      self._content_rect.width,
      self._content_rect.height,
      self.stream_type
    )
    if cache_key == self._matrix_cache_key and self._cached_matrix is not None:
      return self._cached_matrix

    # Get camera configuration
    device_camera = self.device_camera or DEFAULT_DEVICE_CAMERA
    is_wide_camera = self.stream_type == WIDE_CAM
    intrinsic = device_camera.ecam.intrinsics if is_wide_camera else device_camera.fcam.intrinsics
    calibration = self.view_from_wide_calib if is_wide_camera else self.view_from_calib
    zoom = 2.0 if is_wide_camera else 1.1

    # Calculate transforms for vanishing point
    calib_transform = intrinsic @ calibration
    kep = calib_transform @ INF_POINT

    # Calculate center points and dimensions
    x, y = self._content_rect.x, self._content_rect.y
    w, h = self._content_rect.width, self._content_rect.height
    cx, cy = intrinsic[0, 2], intrinsic[1, 2]

    # Calculate max allowed offsets with margins
    margin = 5
    max_x_offset = cx * zoom - w / 2 - margin
    max_y_offset = cy * zoom - h / 2 - margin

    # Calculate and clamp offsets to prevent out-of-bounds issues
    try:
      if abs(kep[2]) > 1e-6:
        x_offset = np.clip((kep[0] / kep[2] - cx) * zoom, -max_x_offset, max_x_offset)
        y_offset = np.clip((kep[1] / kep[2] - cy) * zoom, -max_y_offset, max_y_offset)
      else:
        x_offset, y_offset = 0, 0
    except (ZeroDivisionError, OverflowError):
      x_offset, y_offset = 0, 0

    # Cache the computed transformation matrix to avoid recalculations
    self._matrix_cache_key = cache_key
    self._cached_matrix = np.array([
      [zoom * 2 * cx / w, 0, -x_offset / w * 2],
      [0, zoom * 2 * cy / h, -y_offset / h * 2],
      [0, 0, 1.0]
    ])

    video_transform = np.array([
      [zoom, 0.0, (w / 2 + x - x_offset) - (cx * zoom)],
      [0.0, zoom, (h / 2 + y - y_offset) - (cy * zoom)],
      [0.0, 0.0, 1.0]
    ])
    self.model_renderer.set_transform(video_transform @ calib_transform)

    return self._cached_matrix

  def _get_border_color(self, status: UIStatus) -> rl.Color:
    return BORDER_COLORS.get(status, BORDER_COLORS[UIStatus.DISENGAGED])


  def _draw_border_carrot(self, rect: rl.Rectangle):
    sm = ui_state.sm
    if not sm.alive["carState"]:
      self._draw_border(rect)
      return

    car_state = sm["carState"]

    x = float(rect.x)
    y = float(rect.y)
    w = float(rect.width)
    h = float(rect.height)

    mid_y = y + h / 2.0
    center_gap = 200.0
    gap_half = center_gap / 2.0

    thickness = float(UI_BORDER_SIZE)
    roundness = 0.18
    segments = 10

    # ---------- colors ----------
    if car_state.steeringPressed:
      top_color = self._get_border_color(UIStatus.OVERRIDE)
    elif ui_state.lat_active:
      top_color = self._get_border_color(UIStatus.ENGAGED)
    else:
      top_color = self._get_border_color(UIStatus.DISENGAGED)

    bottom_color = self._get_border_color(ui_state.status)

    left_blink = bool(car_state.leftBlinker)
    right_blink = bool(car_state.rightBlinker)

    # ---------- geometry ----------
    top_h = max(0.0, mid_y - gap_half - y)
    bottom_y = mid_y + gap_half
    bottom_h = max(0.0, y + h - bottom_y)

    # blinker box
    blink_w = UI_BORDER_SIZE
    blink_h = center_gap
    blink_y = mid_y - blink_h / 2.0

    left_blink_rect = rl.Rectangle(
      x,
      blink_y,
      blink_w,
      blink_h
    )
    right_blink_rect = rl.Rectangle(
      x + w - blink_w,
      blink_y,
      blink_w,
      blink_h
    )

    # ---------- top inverted U ----------
    # top horizontal
    rl.draw_rectangle(
      int(x),
      int(y),
      int(w),
      int(thickness),
      top_color
    )

    # top left vertical
    rl.draw_rectangle(
      int(x),
      int(y),
      int(thickness),
      int(top_h),
      top_color
    )

    # top right vertical
    rl.draw_rectangle(
      int(x + w - thickness),
      int(y),
      int(thickness),
      int(top_h),
      top_color
    )

    # ---------- bottom U ----------
    # bottom left vertical
    rl.draw_rectangle(
      int(x),
      int(bottom_y),
      int(thickness),
      int(bottom_h),
      bottom_color
    )

    # bottom right vertical
    rl.draw_rectangle(
      int(x + w - thickness),
      int(bottom_y),
      int(thickness),
      int(bottom_h),
      bottom_color
    )

    # bottom horizontal
    rl.draw_rectangle(
      int(x),
      int(y + h - thickness),
      int(w),
      int(thickness),
      bottom_color
    )

    # ---------- blinkers ----------
    rl.draw_rectangle_rounded(
      left_blink_rect,
      roundness,
      segments,
      rl.ORANGE if left_blink else rl.BLACK
    )
    rl.draw_rectangle_rounded_lines_ex(
      left_blink_rect,
      roundness,
      segments,
      1.0,
      top_color
    )

    rl.draw_rectangle_rounded(
      right_blink_rect,
      roundness,
      segments,
      rl.ORANGE if right_blink else rl.BLACK
    )
    rl.draw_rectangle_rounded_lines_ex(
      right_blink_rect,
      roundness,
      segments,
      1.0,
      top_color
    )

    # ---------- text ----------
    text_margin = 30.0
    font_size = 30.0
    line_margin = 2.0

    top = str(car_state.logCarrot)
    top_left = ""
    top_right = ""
    bottom = ""
    bottom_left = ""
    bottom_right = ""

    car_name = ui_state.params.get("CarName") or ""

    if ui_state.params.get_int("HyundaiCameraSCC") > 0:
      car_name += "(CAMERA SCC)"
    else:
      try:
        if sm.alive["carParams"] and sm["carParams"].openpilotLongitudinalControl:
          car_name += " - OP Long"
      except Exception:
        pass

    nnff_model_name = ui_state.params.get("NNFFModelName") or ""
    if len(nnff_model_name) > 0:
      car_name += ",NNFF"

    top_left = car_name

    try:
      top_right_parts = []

      if sm.alive["liveDelay"]:
        live_delay = sm["liveDelay"]
        top_right_parts.append(
          f"LD[{live_delay.calPerc:.0f}%,{live_delay.lateralDelay:.2f}]"
        )

      if sm.alive["liveTorqueParameters"]:
        ltp = sm["liveTorqueParameters"]
        live_valid = "ON" if ltp.liveValid else "OFF"
        top_right_parts.append(
          f"LT[{ltp.calPerc:.0f}%,{live_valid}]({ltp.latAccelFactorFiltered:.2f}/{ltp.frictionCoefficientFiltered:.2f})"
        )

      if sm.alive["liveParameters"]:
        lp = sm["liveParameters"]
        custom_sr = ui_state.params.get_float("CustomSR") / 10.0
        top_right_parts.append(f"SR({lp.steerRatio:.1f},{custom_sr:.1f})")

      top_right = ", ".join(top_right_parts)
    except Exception:
      print("Error accessing live debug data for top right text")
      top_right = ""

    if sm.alive["lateralPlan"]:
      lat_plan = sm["lateralPlan"]
      bottom = str(lat_plan.latDebugText)

    bottom_left = ui_state.params.get("GitBranch") or ""

    bottom_right = ui_state.params_memory.get("NetworkAddress") or ""

    # text positions
    top_text_y = y + line_margin
    bottom_text_y = bottom_y + bottom_h - font_size - 2
    draw_text_ui_style(top, x + w / 2.0, top_text_y, font_size, rl.WHITE,
                       align="center_top", y_offset=0.0)
    draw_text_ui_style(top_left, x + text_margin, top_text_y, font_size, rl.WHITE,
                       align="left_top", y_offset=0.0)
    draw_text_ui_style(top_right, x + w - text_margin, top_text_y, font_size, rl.WHITE,
                       align="right_top", y_offset=0.0)

    draw_text_ui_style(bottom, x + w / 2.0, bottom_text_y, font_size, rl.WHITE,
                       align="center_top", y_offset=0.0)
    draw_text_ui_style(bottom_left, x + text_margin, bottom_text_y, font_size, rl.WHITE,
                       align="left_top", y_offset=0.0)
    draw_text_ui_style(bottom_right, x + w - text_margin, bottom_text_y, font_size, rl.WHITE,
                       align="right_top", y_offset=0.0)

if __name__ == "__main__":
  gui_app.init_window("OnRoad Camera View")
  road_camera_view = AugmentedRoadView(ROAD_CAM)
  gui_app.push_widget(road_camera_view)
  print("***press space to switch camera view***")
  try:
    for _ in gui_app.render():
      ui_state.update()
      if rl.is_key_released(rl.KeyboardKey.KEY_SPACE):
        if WIDE_CAM in road_camera_view.available_streams:
          stream = ROAD_CAM if road_camera_view.stream_type == WIDE_CAM else WIDE_CAM
          road_camera_view.switch_stream(stream)
  finally:
    road_camera_view.close()
