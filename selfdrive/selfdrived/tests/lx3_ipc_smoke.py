"""Linux real IPC/schema/production host publishing; no vehicle or CAN TX."""
import time
import tempfile
from types import SimpleNamespace as NS
from unittest.mock import patch
from cereal import car, log, messaging
from cereal.services import SERVICE_LIST
from openpilot.selfdrive.selfdrived.selfdrived import SelfdriveD
from openpilot.selfdrive.selfdrived.state import StateMachine
from openpilot.selfdrive.selfdrived.events import Events, ET
from openpilot.selfdrive.selfdrived.alertmanager import AlertManager
from openpilot.selfdrive.selfdrived.lx3_engagement import Lx3Engagement, lx3_control_permissions, lx3_input_ready
from openpilot.selfdrive.selfdrived.lx3_transport import stamp_control_identity, prepare_sendcan
from openpilot.selfdrive.pandad import can_list_to_can_capnp
from openpilot.common.params import Params
from openpilot.common.prefix import OpenpilotPrefix
from openpilot.selfdrive.car.car_specific import CarSpecificEvents


def main():
  cp = car.CarParams.new_message(carFingerprint='HYUNDAI_PALISADE_LX3_HEV', brand='hyundai', steerControlType='angle',
                                 openpilotLongitudinalControl=True, alternativeExperience=1,
                                 safetyConfigs=[{'safetyModel': 'hyundaiCanfd', 'safetyParam': 1214}])
  pm = messaging.PubMaster(['pandaStates', 'carState', 'selfdriveState', 'onroadEvents', 'carControl', 'sendcan'])
  sm = messaging.SubMaster(['pandaStates', 'carState'])
  output = messaging.sub_sock('selfdriveState', timeout=1000)
  control_output = messaging.sub_sock('carControl', timeout=1000)
  sendcan_output = messaging.sub_sock('sendcan', timeout=1000)
  context = NS(CP=cp, sm=sm, pm=pm, lx3_engagement=Lx3Engagement(), state_machine=StateMachine(),
               car_state_fresh=True, enabled=False, active=False, events=Events(), events_prev=[],
               AM=AlertManager(), is_metric=True, experimental_mode=False, personality=log.LongitudinalPersonality.standard,
               distance_traveled=0.0)
  panda = dict(version=2, epoch=0x123456789ABCDEF0, requested=0, accepted=0, counter=0,
               generation=1, age=0, phase=0, allowed=False)
  directory = tempfile.TemporaryDirectory()
  isolated_params = Params(directory.name)
  isolated_params.put_bool('MuteDoor', False)
  isolated_params.put_bool('MuteSeatbelt', False)
  with patch('openpilot.selfdrive.car.car_specific.Params', return_value=isolated_params):
    car_events = CarSpecificEvents(cp)
  car_events.frame = 99  # First update reads the actual isolated Params values.
  previous_cs = car.CarState.new_message(gearShifter='drive', vEgo=15, vCruise=50)
  cc = car.CarControl.new_message()
  def publish_panda():
    msg = messaging.new_message('pandaStates', 1, valid=True)
    p = msg.pandaStates[0]
    p.safetyModel = 'hyundaiCanfd'; p.safetyParam = 1214; p.alternativeExperience = 1
    p.controlsAllowed = panda['allowed']; p.safetyRxChecksInvalid = False; p.faults = []
    p.lx3PermissionVersion = panda['version']; p.lx3RequestedMode = panda['requested']
    p.lx3AcceptedMode = panda['accepted']; p.lx3PhysicalCounter = panda['counter']
    p.lx3RequestGeneration = panda['generation']; p.lx3RequestAgeMs = panda['age']
    p.lx3ControlsAllowed = panda['allowed']; p.lx3PermissionPhase = panda['phase']
    p.lx3TransportEpoch = panda['epoch']
    pm.send('pandaStates', msg)
  for _ in range(8):
    publish_panda(); sm.update(100)
    time.sleep(1 / SERVICE_LIST['pandaStates'].frequency)
  assert sm.all_checks(['pandaStates']), (sm.alive, sm.valid, sm.freq_ok)
  def step(button=None, counter=0, event=None, physical_valid=True, door=False, input_state='ready',
           input_counter_valid=True, buttons=None, reset_count=1):
    nonlocal previous_cs
    time.sleep(1 / SERVICE_LIST['pandaStates'].frequency)
    publish_panda()
    msg = messaging.new_message('carState', valid=True)
    msg.carState.canValid = True
    msg.carState.lx3InputState = input_state
    msg.carState.lx3PhysicalCounter = counter
    msg.carState.lx3PhysicalCounterValid = input_counter_valid
    msg.carState.lx3InputResetCount = reset_count
    msg.carState.gearShifter = 'drive'
    msg.carState.vEgo = 15
    msg.carState.vCruise = 50
    msg.carState.doorOpen = door
    # LFA-only must not depend on the legacy stock SCC availability latch.
    msg.carState.cruiseState.available = False
    msg.carState.buttonEvents = [] if button is None else [dict(type=button, pressed=False,
                                      lx3PhysicalCounter=counter, lx3PhysicalValid=physical_valid)]
    if buttons is not None:
      msg.carState.buttonEvents = [dict(type=name, pressed=pressed, lx3PhysicalCounter=physical_counter,
                                       lx3PhysicalValid=True) for name, pressed, physical_counter in buttons]
    pm.send('carState', msg); sm.update(100)
    assert sm.all_checks(['pandaStates']), (sm.alive, sm.valid, sm.freq_ok)
    context.events.clear()
    stock_events = car_events.update(sm['carState'], previous_cs, cc)
    assert log.OnroadEvent.EventName.wrongCarMode in stock_events.events
    context.events.add_from_msg(stock_events.to_msg())
    previous_cs = sm['carState']
    if event is not None: context.events.add(event)
    SelfdriveD.update_lx3_state(context, sm['carState'])
    assert log.OnroadEvent.EventName.wrongCarMode not in context.events.events
    SelfdriveD.update_alerts(context, sm['carState'])
    # Execute the actual production publisher, including ACK fields and events.
    SelfdriveD.publish_selfdriveState(context, sm['carState'])
    received = messaging.recv_one(output)
    assert received is not None and received.valid
    ss = received.selfdriveState
    assert ss.lx3EngagementMode == context.lx3_engagement.mode
    assert ss.lx3AckValid == context.lx3_engagement.ack_valid
    assert ss.lx3AckGeneration == context.lx3_engagement.ack_generation
    assert ss.lx3AckPhysicalCounter == context.lx3_engagement.ack_counter
    assert ss.lx3AckTransportEpoch == context.lx3_engagement.ack_epoch
    assert ss.lx3AcceptedGeneration == context.lx3_engagement.accepted_generation
    assert ss.lx3AcceptedTransportEpoch == context.lx3_engagement.accepted_epoch
    ready = lx3_input_ready(sm['carState'])
    permission = lx3_control_permissions(ss.lx3EngagementMode, ss.enabled, ss.active, ready, True, True)
    control = messaging.new_message('carControl', valid=True)
    control.carControl.latActive, control.carControl.longActive = permission
    stamp_control_identity(control.carControl, ss, ready)
    pm.send('carControl', control)
    received_control = messaging.recv_one(control_output)
    assert received_control is not None
    origin = received_control.carControl
    frames, identity = prepare_sendcan([(0x161, bytes(32), 0), (0x730, b'\x02\x3e\x80' + bytes(5), 1)], origin)
    pm.send('sendcan', can_list_to_can_capnp(frames, msgtype='sendcan', lx3_identity=identity))
    wire = messaging.recv_one(sendcan_output)
    assert wire is not None
    if identity is None:
      assert [item.address for item in wire.sendcan] == [0x730]
    else:
      owned = wire.sendcan[0]
      assert owned.lx3IdentityValid and owned.lx3Generation == ss.lx3AcceptedGeneration
      assert owned.lx3TransportEpoch == panda['epoch']
      assert not wire.sendcan[1].lx3IdentityValid
    return ss, permission
  panda.update(requested=1, counter=40, generation=2, phase=1)
  ss, permission = step('lfaButton', 40)
  assert permission == (False, False) and ss.enabled and not ss.active
  assert ss.state == 'preEnabled' and ss.lx3AckValid and ss.lx3AckMode == 1 and ss.lx3AckGeneration == 2
  panda.update(accepted=1, phase=2, allowed=True)
  ss, permission = step()
  assert permission == (True, False) and not ss.lx3AckValid and ss.lx3AckGeneration == 0
  assert ss.lx3AcceptedTransportEpoch == panda['epoch'] and ss.lx3AcceptedGeneration == 2
  panda.update(requested=2, accepted=0, counter=42, generation=3, phase=1, allowed=False)
  ss, permission = step('mainCruise', 42)
  assert permission == (False, False) and not ss.enabled
  assert ss.lx3AcceptedTransportEpoch == 0 and ss.lx3AcceptedGeneration == 0
  assert ET.USER_DISABLE in context.state_machine.current_alert_types
  ss, permission = step()
  assert ss.enabled and not ss.active and ss.lx3AckMode == 2 and ss.lx3AckValid
  panda.update(accepted=2, phase=2, allowed=True)
  ss, permission = step()
  assert permission == (True, True) and not ss.lx3AckValid
  ss, permission = step(event=log.OnroadEvent.EventName.gasPressedOverride)
  assert permission == (True, True) and context.events.contains(ET.OVERRIDE_LONGITUDINAL)
  ss, permission = step('cancel', input_state='requalifying', input_counter_valid=False)
  assert permission == (False, False) and not ss.enabled
  assert ss.alertType == 'buttonCancel/userDisable'
  panda.update(version=0)
  ss, permission = step('lfaButton', 44)
  assert permission == (False, False) and not ss.enabled and not ss.lx3AckValid
  panda.update(version=2, requested=1, accepted=0, counter=46, generation=4, phase=1, allowed=False)
  ss, permission = step('lfaButton', 46)
  assert ss.enabled and not ss.active and ss.lx3AckGeneration == 4
  ss, permission = step(event=log.OnroadEvent.EventName.tooDistracted)
  assert not ss.enabled and permission == (False, False)
  assert ss.lx3AckValid and ss.lx3AckMode == 0 and ss.lx3AckGeneration == 4
  panda.update(accepted=1, phase=2, allowed=True)
  ss, permission = step()
  assert not ss.enabled and permission == (False, False)
  panda.update(requested=1, accepted=0, counter=48, generation=5, phase=1, allowed=False)
  ss, permission = step('lfaButton', 48, physical_valid=False)
  assert not ss.enabled and permission == (False, False)
  assert not (ss.lx3AckValid and ss.lx3AckMode == 1 and ss.lx3AckGeneration == 5)
  panda.update(requested=1, accepted=0, counter=50, generation=6, phase=1, allowed=False)
  ss, permission = step('lfaButton', 50, door=True)
  assert log.OnroadEvent.EventName.doorOpen in context.events.events
  assert not ss.enabled and permission == (False, False)
  assert ss.lx3AckValid and ss.lx3AckMode == 0 and ss.lx3AckGeneration == 6
  for generation, state, counter_valid in ((7, 'notApplicable', False), (8, 'warmingUp', False),
                                           (9, 'requalifying', False), (10, 'integrityFault', False),
                                           (11, 'ready', False)):
    panda.update(requested=1, accepted=0, counter=52 + generation, generation=generation, phase=1, allowed=False)
    ss, permission = step('lfaButton', 52 + generation, input_state=state, input_counter_valid=counter_valid)
    assert not ss.enabled and permission == (False, False)
    assert not (ss.lx3AckValid and ss.lx3AckMode != 0)
    assert log.OnroadEvent.EventName.steerUnavailable not in context.events.events
    expected = log.OnroadEvent.EventName.lx3InputFault if state == 'integrityFault' else log.OnroadEvent.EventName.lx3InputNotReady
    assert expected in context.events.events
  panda.update(requested=1, accepted=0, counter=80, generation=12, phase=1, allowed=False)
  ss, permission = step('lfaButton', 80)
  assert ss.enabled and not ss.active and ss.lx3AckMode == 1
  panda.update(accepted=1, phase=2, allowed=True)
  ss, permission = step()
  assert ss.active and permission == (True, False)
  ss, permission = step(input_state='integrityFault', input_counter_valid=False)
  assert not ss.enabled and permission == (False, False)
  assert context.events.contains(ET.IMMEDIATE_DISABLE)
  assert log.OnroadEvent.EventName.steerUnavailable not in context.events.events
  assert ss.alertText2 == '핸들 버튼 데이터 확인 필요'
  ss, permission = step()
  assert not ss.enabled and permission == (False, False)
  # A delayed MAIN-OFF/RES batch must enter a new generation from disabled.
  # Real schema/IPC carry reset serials, individual release anchors and final
  # consumed counters separately; no Panda mode is used as driver intent.
  panda.update(requested=2, accepted=0, counter=6, generation=13, phase=1, allowed=False)
  ss, permission = step('mainCruise', 6)
  assert ss.state == 'preEnabled' and ss.lx3AckGeneration == 13
  panda.update(accepted=2, phase=2, allowed=True)
  ss, permission = step()
  assert permission == (True, True)
  ss, permission = step(counter=34, buttons=[('mainCruise', True, 34)])
  assert permission == (True, True)
  panda.update(requested=2, accepted=0, counter=44, generation=15, phase=1, allowed=False)
  ss, permission = step(counter=40)
  assert not ss.enabled and permission == (False, False)
  assert context.lx3_engagement.replay_base is not None
  ss, permission = step(counter=44, buttons=[('mainCruise', False, 36),
                                           ('accelCruise', True, 42), ('accelCruise', False, 44)])
  assert ss.state == 'preEnabled' and not ss.active and permission == (False, False)
  assert ss.lx3AckValid and ss.lx3AckGeneration == 15 and ss.lx3AckPhysicalCounter == 44
  assert ss.lx3AcceptedGeneration == 0
  panda.update(accepted=2, phase=2, allowed=True)
  ss, permission = step(counter=44)
  assert permission == (True, True) and ss.lx3AcceptedGeneration == 15
  # Exercise the real pending Alert creation delay in30 normal10ms frames,
  # separately from this smoke's deliberately10Hz transport sample steps.
  alert_events, manager = Events(), AlertManager()
  for frame in range(30):
    alert_events.clear()
    alert_events.add(log.OnroadEvent.EventName.lx3PermissionPending)
    alerts = alert_events.create_alerts([ET.PRE_ENABLE])
    if frame < 29: assert not alerts
    manager.add_many(frame, alerts)
    manager.process_alerts(frame, set())
  assert manager.current_alert.alert_text_1 == '주행보조 준비 중'
  assert manager.current_alert.alert_type == 'lx3PermissionPending/preEnable'
  directory.cleanup()
  print('PASS real msgq/Capnp/production host publisher, CarSpecificEvents and AlertManager: stock SCC unavailable LAT/COMB, door barrier, request ACK, PRE_ENABLE, upgrade alert, denial, cancel, old firmware/default producer, delayed MAIN OFF/RES with reset serial; immutable accepted identity through carControl/sendcan IPC')


if __name__ == '__main__':
  with OpenpilotPrefix():
    main()
