"""PC-only unqualified departure comparison, never a live control import."""
import math
from types import SimpleNamespace as NS


class DepartureThresholdReview:
  """Unqualified shadow calculation, NEVER a control/stop threshold input.

  Transient-data loss can reassert the old criterion during low-speed motion.
  Keep this candidate diagnostic-only until a complete transition policy and
  physical delay cause are established. No setting can apply it to driving.
  """
  def __init__(self):
    self.reset('off')

  def reset(self, reason):
    self.track = self.previous = None
    self.since = self.opening_since = None
    self.rest_distance = None
    self.active = False
    self.reason = reason

  def observe(self, *args, **kwargs):
    try:
      return self.threshold(*args,**kwargs)
    except Exception:
      # Passive evidence may fail independently of normal planning/control.
      self.reset('review_input_error')
      return args[0]

  def threshold(self, default, radar, *, enabled, eligible, stamp_ns, v_ego,
                standstill, starting_speed, stop_distance, planned_accel):
    if not enabled:
      self.reset('off');return default
    if (not eligible or not all(math.isfinite(v) for v in
        (default,v_ego,starting_speed,stop_distance,planned_accel)) or default<=.25 or
        v_ego>=(max(default,.1) if self.active else max(starting_speed,.1)) or v_ego<0):
      self.reset('inactive_or_moving');return default
    lead=radar.leadOne
    if (not lead.status or not lead.radar or lead.radarTrackId<0 or lead.modelProb<.9 or
        not all(math.isfinite(v) for v in (lead.dRel,lead.vRel,lead.vLead,lead.aLeadK,lead.modelProb)) or
        lead.dRel<3. or lead.vRel<=-.1 or lead.aLeadK<-.3 or
        any(t.status and (not math.isfinite(t.dRel) or
            (t.radarTrackId!=lead.radarTrackId and t.dRel<lead.dRel+2.)) for t in (radar.leadTwo,))):
      self.reset('lead_unqualified');return default
    # Confirm with distinct radar messages. Cached or backwards observations
    # cannot advance opening evidence; discontinuity discards the old latch.
    if self.previous is not None:
      old_stamp,old_distance,old_relative=self.previous
      dt=(stamp_ns-old_stamp)/1e9
      if dt==0:
        if planned_accel<=0.:
          self.active=False;self.reason='waiting_for_positive_plan';return default
        return min(default,.25) if self.active else default
      if not 0<dt<=.2 or abs(lead.dRel-old_distance-old_relative*dt)>1.:
        self.reset('radar_discontinuity')
    if self.track!=lead.radarTrackId:
      self.reset('new_track');self.track=lead.radarTrackId;self.since=stamp_ns
    self.previous=(stamp_ns,lead.dRel,lead.vRel)
    if standstill and v_ego<.1 and not self.active and lead.vRel<.3:
      self.rest_distance=lead.dRel
    opening=(lead.vRel>0.05 and lead.vLead>=.25) if self.active else (lead.vRel>=.3 and lead.vLead>=.3)
    if not opening or planned_accel<=0.:
      self.opening_since=None;self.active=False;self.reason='waiting_for_opening_plan';return default
    if self.opening_since is None:self.opening_since=stamp_ns
    if (not self.active and (self.rest_distance is None or stamp_ns-self.since<1_000_000_000 or
        stamp_ns-self.opening_since<300_000_000 or lead.dRel-self.rest_distance<.3)):
      self.reason='confirming_opening';return default
    self.active=True;self.reason='confirmed_opening_lower_stop_threshold'
    return min(default,.25)


def compare_record(frame, review, *, starting_speed):
  """Same recorded decision only; gaps/absent fields are not interpolated.

  starting_speed must come from that drive's captured CarParams. Results from
  this unqualified alternative never imply a road improvement or actuator ACK.
  """
  try:
    state,output=frame['state'],frame['output'];decision=output['stop_decision']
    default=output['v_ego_stopping'];cycle=int(frame['cycle']);identity=frame['producer']
    services=frame['services'];stamp=int(services['radarState']['mono_ns'])
    # Contract2 records the SAME update-time freshness reference; publication
    # time may be later and must not silently change the200ms eligibility edge.
    if decision['contract']!=2:raise ValueError('unsupported stop decision contract')
    now=int(decision['update_mono_ns']);values=(decision['v_at_action'],decision['v_at_action_plus_1s'])
    if not all(math.isfinite(v) for v in (*values,default,starting_speed)):
      raise ValueError('nonfinite decision input')
    if state['planner_mode']!='acc':return {'status':'unsupported_mode','control_changes':False}
    actual=values[0]<default and values[1]<default
    if actual is not decision['mpc_should_stop']:
      return {'status':'baseline_decision_mismatch','control_changes':False}
    previous=getattr(review,'record_position',None)
    gap=previous is not None and (previous[0]!=identity or cycle!=previous[1]+1)
    if gap:review.reset('recording_gap')
    review.record_position=(identity,cycle)
    leads=[NS(**row) for row in frame['leads']]
    if len(leads)!=2:raise ValueError('two recorded lead slots required')
    eligible=(state['approach_eligible'] and state['long_active'] and state['can_valid'] and
      not(state['can_timeout'] or state['reset_state'] or state['force_decel'] or state['gas'] or
          state['brake'] or state['soft_hold'] or state['cruise_standstill'] or state['fcw']) and
      all(services[k]['valid'] and services[k]['alive'] and 0<=now-int(services[k]['mono_ns'])<=200_000_000
          for k in ('carState','carControl','radarState','modelV2','controlsState','selfdriveState')))
    threshold=review.observe(default,NS(leadOne=leads[0],leadTwo=leads[1]),enabled=True,
      eligible=bool(eligible),stamp_ns=stamp,v_ego=state['v_ego'],standstill=state['standstill'],
      starting_speed=starting_speed,stop_distance=frame['settings']['stop_distance'],planned_accel=state['planned_accel'])
    return {'status':'recording_gap_reset' if gap else 'same_recorded_decision',
      'actual_mpc_should_stop':actual,'candidate_should_stop':values[0]<threshold and values[1]<threshold,
      'candidate_threshold':threshold,'reason':review.reason,'candidate_qualified':False,'control_changes':False}
  except (KeyError,ValueError,TypeError,AttributeError):
    review.reset('missing_or_invalid_record')
    return {'status':'missing_or_invalid_record','candidate_qualified':False,'control_changes':False}
