"""Built Linux IPC/schema + real state-machine smoke check; no vehicle/CAN TX."""
import time
from types import SimpleNamespace
from cereal import car, log, messaging
from cereal.services import SERVICE_LIST
from openpilot.selfdrive.selfdrived.selfdrived import SelfdriveD
from openpilot.selfdrive.selfdrived.state import StateMachine
from openpilot.selfdrive.selfdrived.events import Events
from openpilot.selfdrive.selfdrived.lx3_engagement import Lx3Engagement, lx3_control_permissions


def main():
  cp=car.CarParams.new_message(carFingerprint='HYUNDAI_PALISADE_LX3_HEV', steerControlType='angle',
                              openpilotLongitudinalControl=True, alternativeExperience=1,
                              safetyConfigs=[{'safetyModel':'hyundaiCanfd','safetyParam':1214}])
  pm=messaging.PubMaster(['pandaStates','carState','selfdriveState'])
  sm=messaging.SubMaster(['pandaStates','carState'])
  output=messaging.sub_sock('selfdriveState',timeout=1000)
  context=SimpleNamespace(CP=cp,sm=sm,lx3_engagement=Lx3Engagement(),state_machine=StateMachine(),
                          car_state_fresh=True,enabled=False,active=False,events=Events())
  def publish_panda(allowed):
    msg=messaging.new_message('pandaStates',1,valid=True)
    p=msg.pandaStates[0]
    p.safetyModel='hyundaiCanfd';p.safetyParam=1214;p.alternativeExperience=1;p.controlsAllowed=allowed
    p.safetyRxChecksInvalid=False;p.faults=[]
    pm.send('pandaStates',msg)
  for _ in range(8):
    publish_panda(True);sm.update(100)
    time.sleep(1 / SERVICE_LIST['pandaStates'].frequency)
  assert sm.all_checks(['pandaStates']), (sm.alive,sm.valid,sm.freq_ok)
  def step(button=None,gas_override=False):
    publish_panda(True)
    msg=messaging.new_message('carState',valid=True)
    msg.carState.canValid=True
    msg.carState.buttonEvents=[] if button is None else [{'type':button,'pressed':False}]
    pm.send('carState',msg);sm.update(100)
    context.events=Events()
    if gas_override:context.events.add(log.OnroadEvent.EventName.gasPressedOverride)
    SelfdriveD.update_lx3_state(context,sm['carState'])
    state=messaging.new_message('selfdriveState',valid=True)
    state.selfdriveState.enabled=context.enabled;state.selfdriveState.active=context.active
    state.selfdriveState.lx3EngagementMode=int(context.lx3_engagement.mode)
    pm.send('selfdriveState',state)
    received=messaging.recv_one(output)
    assert received is not None and received.valid
    assert received.selfdriveState.lx3EngagementMode==context.lx3_engagement.mode
    return lx3_control_permissions(context.lx3_engagement.mode,context.enabled,context.active,True,True,True)
  assert step('lfaButton')==(True,False)
  assert step('mainCruise')==(True,True)
  assert step(gas_override=True)==(True,True)  # Event suspends long actuation later in controlsd.
  assert context.events.contains('overrideLongitudinal')
  assert step('cancel')==(False,False)
  print('PASS real msgq/Capnp/StateMachine: LFA lateral-only, SCC combined, gas override event, cancel')


if __name__=='__main__':main()
