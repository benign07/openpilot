"""Linux real IPC/schema/production host publishing; no vehicle or CAN TX."""
import time
from types import SimpleNamespace as NS
from cereal import car, log, messaging
from cereal.services import SERVICE_LIST
from openpilot.selfdrive.selfdrived.selfdrived import SelfdriveD
from openpilot.selfdrive.selfdrived.state import StateMachine
from openpilot.selfdrive.selfdrived.events import Events, ET
from openpilot.selfdrive.selfdrived.lx3_engagement import Lx3Engagement, lx3_control_permissions


def main():
  cp = car.CarParams.new_message(carFingerprint='HYUNDAI_PALISADE_LX3_HEV', steerControlType='angle',
                                 openpilotLongitudinalControl=True, alternativeExperience=1,
                                 safetyConfigs=[{'safetyModel': 'hyundaiCanfd', 'safetyParam': 1214}])
  pm = messaging.PubMaster(['pandaStates', 'carState', 'selfdriveState', 'onroadEvents'])
  sm = messaging.SubMaster(['pandaStates', 'carState'])
  output = messaging.sub_sock('selfdriveState', timeout=1000)
  alert = NS(alert_text_1='', alert_text_2='', alert_size='none', alert_status='normal',
             alert_type='', audible_alert='none', visual_alert='none')
  context = NS(CP=cp, sm=sm, pm=pm, lx3_engagement=Lx3Engagement(), state_machine=StateMachine(),
               car_state_fresh=True, enabled=False, active=False, events=Events(), events_prev=[],
               AM=NS(current_alert=alert), experimental_mode=False, personality=log.LongitudinalPersonality.standard,
               distance_traveled=0.0)
  panda = dict(version=1, requested=0, accepted=0, counter=0, generation=1, age=0, phase=0, allowed=False)
  def publish_panda():
    msg = messaging.new_message('pandaStates', 1, valid=True)
    p = msg.pandaStates[0]
    p.safetyModel = 'hyundaiCanfd'; p.safetyParam = 1214; p.alternativeExperience = 1
    p.controlsAllowed = panda['allowed']; p.safetyRxChecksInvalid = False; p.faults = []
    p.lx3PermissionVersion = panda['version']; p.lx3RequestedMode = panda['requested']
    p.lx3AcceptedMode = panda['accepted']; p.lx3PhysicalCounter = panda['counter']
    p.lx3RequestGeneration = panda['generation']; p.lx3RequestAgeMs = panda['age']
    p.lx3ControlsAllowed = panda['allowed']; p.lx3PermissionPhase = panda['phase']
    pm.send('pandaStates', msg)
  for _ in range(8):
    publish_panda(); sm.update(100)
    time.sleep(1 / SERVICE_LIST['pandaStates'].frequency)
  assert sm.all_checks(['pandaStates']), (sm.alive, sm.valid, sm.freq_ok)
  def step(button=None, counter=0, event=None, physical_valid=True):
    time.sleep(1 / SERVICE_LIST['pandaStates'].frequency)
    publish_panda()
    msg = messaging.new_message('carState', valid=True)
    msg.carState.canValid = True
    msg.carState.buttonEvents = [] if button is None else [dict(type=button, pressed=False,
                                      lx3PhysicalCounter=counter, lx3PhysicalValid=physical_valid)]
    pm.send('carState', msg); sm.update(100)
    assert sm.all_checks(['pandaStates']), (sm.alive, sm.valid, sm.freq_ok)
    context.events = Events()
    if event is not None: context.events.add(event)
    SelfdriveD.update_lx3_state(context, sm['carState'])
    # Execute the actual production publisher, including ACK fields and events.
    SelfdriveD.publish_selfdriveState(context, sm['carState'])
    received = messaging.recv_one(output)
    assert received is not None and received.valid
    ss = received.selfdriveState
    assert ss.lx3EngagementMode == context.lx3_engagement.mode
    assert ss.lx3AckValid == context.lx3_engagement.ack_valid
    assert ss.lx3AckGeneration == context.lx3_engagement.ack_generation
    assert ss.lx3AckPhysicalCounter == context.lx3_engagement.ack_counter
    return ss, lx3_control_permissions(ss.lx3EngagementMode, ss.enabled, ss.active, True, True, True)
  panda.update(requested=1, counter=40, generation=2, phase=1)
  ss, permission = step('lfaButton', 40)
  assert permission == (False, False) and ss.enabled and not ss.active
  assert ss.state == 'preEnabled' and ss.lx3AckValid and ss.lx3AckMode == 1 and ss.lx3AckGeneration == 2
  panda.update(accepted=1, phase=2, allowed=True)
  ss, permission = step()
  assert permission == (True, False) and not ss.lx3AckValid and ss.lx3AckGeneration == 0
  panda.update(requested=2, accepted=0, counter=42, generation=3, phase=1, allowed=False)
  ss, permission = step('mainCruise', 42)
  assert permission == (False, False) and not ss.enabled
  assert ET.USER_DISABLE in context.state_machine.current_alert_types
  ss, permission = step()
  assert ss.enabled and not ss.active and ss.lx3AckMode == 2 and ss.lx3AckValid
  panda.update(accepted=2, phase=2, allowed=True)
  ss, permission = step()
  assert permission == (True, True) and not ss.lx3AckValid
  ss, permission = step(event=log.OnroadEvent.EventName.gasPressedOverride)
  assert permission == (True, True) and context.events.contains(ET.OVERRIDE_LONGITUDINAL)
  ss, permission = step('cancel')
  assert permission == (False, False) and not ss.enabled
  panda.update(version=0)
  ss, permission = step('lfaButton', 44)
  assert permission == (False, False) and not ss.enabled and not ss.lx3AckValid
  panda.update(version=1, requested=1, accepted=0, counter=44, generation=4, phase=1, allowed=False)
  ss, permission = step('lfaButton', 44)
  assert ss.enabled and not ss.active and ss.lx3AckGeneration == 4
  ss, permission = step(event=log.OnroadEvent.EventName.tooDistracted)
  assert not ss.enabled and permission == (False, False)
  assert ss.lx3AckValid and ss.lx3AckMode == 0 and ss.lx3AckGeneration == 4
  panda.update(accepted=1, phase=2, allowed=True)
  ss, permission = step()
  assert not ss.enabled and permission == (False, False)
  panda.update(requested=1, accepted=0, counter=46, generation=5, phase=1, allowed=False)
  ss, permission = step('lfaButton', 46, physical_valid=False)
  assert not ss.enabled and permission == (False, False)
  assert not (ss.lx3AckValid and ss.lx3AckMode == 1 and ss.lx3AckGeneration == 5)
  print('PASS real msgq/Capnp/production host publisher: request ACK, PRE_ENABLE, LAT/COMB, upgrade alert, denial, cancel, old firmware/default producer')


if __name__ == '__main__':
  main()
