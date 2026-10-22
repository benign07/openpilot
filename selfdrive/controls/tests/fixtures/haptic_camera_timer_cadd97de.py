"""Frozen timer AST from cadd97de carcontroller SHA256f0a8f7a1c33641f7192bdc6c6ce75cdc2697326a4ac5b47b135544518db46233. PC oracle only."""
def tick(self, hud_control):
    left_lane_warning = right_lane_warning = 0
    active_speed_decel = hud_control.activeCarrot == 3 and self.activeCarrot != 3
    self.activeCarrot = hud_control.activeCarrot
    if active_speed_decel and self.speedCameraHapticEndFrame < 0:
        self.speedCameraHapticEndFrame = self.frame + 8.0 / DT_CTRL
    elif not active_speed_decel:
        self.speedCameraHapticEndFrame = -1
    if 0 <= self.speedCameraHapticEndFrame - self.frame < int(8.0 / DT_CTRL) and self.hapticFeedbackWhenSpeedCamera > 0:
        t = (self.frame - (self.speedCameraHapticEndFrame - int(8.0 / DT_CTRL))) * DT_CTRL
        for start, end in vibrate_intervals:
            if start <= t < end:
                left_lane_warning = right_lane_warning = self.hapticFeedbackWhenSpeedCamera
                break
    if self.frame >= self.speedCameraHapticEndFrame:
        self.speedCameraHapticEndFrame = -1
    return (left_lane_warning, right_lane_warning)
