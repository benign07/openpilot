"""Exercise manager's actual startup and onroad process selection without starting processes."""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from openpilot.cereal import car
import openpilot.system.manager.manager as manager


@pytest.mark.parametrize('lite', (False, True))
@pytest.mark.parametrize('blocked', ('', 'loggerd'))
def test_lite_keeps_route_recording_and_explicit_block(monkeypatch, lite, blocked):
  values = {'HardwareC3xLite': lite, 'RecordAudio': True}
  params = SimpleNamespace(get=lambda k: 'test-dongle', get_bool=lambda k: values.get(k, False),
                           put_bool=lambda k, v: values.__setitem__(k, v), clear_all=lambda flag: None)
  cp = car.CarParams.new_message()
  cp.notCar = False

  class SM:
    def update(self, timeout): pass
    def __getitem__(self, key):
      return {'deviceState': SimpleNamespace(started=True), 'carParams': cp, 'pandaStates': []}[key]

  monkeypatch.setenv('BLOCK', blocked)
  monkeypatch.delenv('NOBOARD', raising=False)
  monkeypatch.setattr(manager, 'Params', lambda: params)
  monkeypatch.setattr(manager, 'cloudlog', Mock())
  monkeypatch.setattr(manager.messaging, 'SubMaster', lambda *a, **kw: SM())
  monkeypatch.setattr(manager.messaging, 'PubMaster', lambda *a, **kw: None)
  monkeypatch.setattr(manager, 'write_onroad_params', lambda *a: None)
  monkeypatch.setattr(manager, 'XiaogeStartupGate', lambda: SimpleNamespace(update=lambda *a: True))

  class ObservedOnroad(Exception): pass

  observed = []
  def observe(processes, started, params, CP, not_run):
    observed.append(started)
    assert ('loggerd' in not_run) == bool(blocked)
    assert ('micd' in not_run) == lite
    assert ('soundd' in not_run) == lite
    assert 'encoderd' not in not_run
    assert values['RecordAudio'] == (not lite)
    assert manager.managed_processes['loggerd'].should_run(started, params, CP) == started
    if started:
      raise ObservedOnroad

  monkeypatch.setattr(manager, 'ensure_running', observe)
  with pytest.raises(ObservedOnroad):
    manager.manager_thread(None)
  assert observed == [False, True]
