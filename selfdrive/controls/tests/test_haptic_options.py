"""Event selection and bounded binary motor-request patterns, no vehicle TX."""
from types import SimpleNamespace
import pytest
from opendbc.car.hyundai.lx3_haptic_notifications import HapticNotifications, HAPTIC_EVENT_OPTIONS, HAPTIC_PATTERN_KEY


@pytest.mark.parametrize('pattern,expected',[(0,80),(1,60),(2,60)])
def test_pattern_never_extends_deadline_or_changes_strength(pattern,expected):
  h=HapticNotifications();h.configure(pattern=pattern)
  values=[h.update('engage' if f==0 else 'none',eligible=True,frame=f) for f in range(250)]
  assert sum(values)==expected
  assert not any(values[80:])
  assert all(type(x) is bool for x in values)


@pytest.mark.parametrize('key,default,names,title',HAPTIC_EVENT_OPTIONS)
def test_disabling_each_group_consumes_held_event_without_replay(key,default,names,title):
  h=HapticNotifications();h.configure(h.ROUTINE-set(names))
  assert not h.update(names[0],eligible=True,frame=0)
  h.configure()
  assert not h.update(names[0],eligible=True,frame=1)
  assert not h.update('none',eligible=True,frame=2)
  assert h.update(names[0],eligible=True,frame=3)


@pytest.mark.parametrize('pattern',range(3))
@pytest.mark.parametrize('cancel',['ineligible','critical','unknown','settings','clock_rollback'])
def test_every_pattern_cancels_and_cannot_resume_a_partial_request(pattern,cancel):
  h=HapticNotifications();h.configure(pattern=pattern)
  assert h.update('engage',eligible=True,frame=100)
  if cancel=='settings':h.configure((),pattern)
  sound='warningImmediate' if cancel=='critical' else 'notAnAlert' if cancel=='unknown' else 'none'
  assert not h.update(sound,eligible=cancel!='ineligible',frame=90 if cancel=='clock_rollback' else 110)
  assert not h.update('none',eligible=True,frame=120)


@pytest.mark.parametrize('value',[-1,3,999,None,'2',True,0.5])
def test_unknown_pattern_values_fall_back_to_verified_single_pulse(value):
  h=HapticNotifications();h.configure(pattern=value)
  assert h.pattern==0
  assert h.update('engage',eligible=True,frame=0)
  assert h.update('none',eligible=True,frame=40)


def test_new_registry_selects_events_and_legacy_registry_keeps_previous_defaults():
  h=HapticNotifications()
  legacy=SimpleNamespace(check_key=lambda key:False)
  h.configure_from_params(legacy)
  assert h.enabled_events==h.ROUTINE and h.pattern==0
  current=SimpleNamespace(check_key=lambda key:True,
    get_bool=lambda key:key=='C3xLiteHapticEngage',get_int=lambda key:2)
  h.configure_from_params(current)
  assert h.enabled_events==set(HAPTIC_EVENT_OPTIONS[0][2]) and h.pattern==2
  assert set().union(*(set(x[2]) for x in HAPTIC_EVENT_OPTIONS))==h.ROUTINE


def test_setting_refresh_without_change_preserves_request_and_sparse_updates_do_not_replay():
  h=HapticNotifications();h.configure(pattern=1)
  assert h.update('engage',eligible=True,frame=0)
  h.configure(pattern=1)
  assert h.update('none',eligible=True,frame=5)
  assert not h.update('engage2',eligible=True,frame=80)
  assert not h.update('engage2',eligible=True,frame=101)
  assert not h.update('none',eligible=True,frame=102)
  assert h.update('engage2',eligible=True,frame=103)
