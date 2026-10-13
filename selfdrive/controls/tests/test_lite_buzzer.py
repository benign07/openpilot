import ast
from pathlib import Path
from types import SimpleNamespace as NS
import pytest
from openpilot.selfdrive.controls import beep


@pytest.fixture
def buzzer(monkeypatch):
  p=NS(mute=False,volume=100)
  p.get_bool=lambda k:p.mute
  p.get_int=lambda k:p.volume
  monkeypatch.setattr(beep,'Params',lambda:p)
  monkeypatch.setattr(beep.Beepd,'enable_gpio',lambda self:None)
  writes=[]
  monkeypatch.setattr(beep.Beepd,'_write_gpio',lambda self,on:writes.append(on))
  return beep.Beepd(),p,writes


def test_routine_mute_and_live_mute_cuts_running_optional(buzzer):
  b,p,out=buzzer
  b.update_alert(beep.AudibleAlert.engage,now=0);b.tick(0);assert out[-1]
  p.mute=True;b.tick(.5);assert out[-1] is False
  b.update_alert(beep.AudibleAlert.laneChange,now=1);b.tick(1);assert not b.pattern


@pytest.mark.parametrize('name',['disengage','refuse','prompt','promptRepeat','promptDistracted',
  'warningImmediate','warningSoft','bsdWarning','trafficError','stopStop','speedDown','disengage2','audio1','audio10'])
def test_protected_sound_ignores_routine_mute_and_zero_volume(buzzer,name):
  b,p,out=buzzer;p.mute=True;p.volume=0
  b.update_alert(getattr(beep.AudibleAlert,name),now=2);b.tick(2)
  assert out[-1] is True
  b.tick(3);assert out[-1] is False


def test_warning_preempts_and_optional_cannot_stop_it(buzzer):
  b,p,out=buzzer
  b.update_alert(beep.AudibleAlert.engage,now=0);b.tick(0)
  b.update_alert(beep.AudibleAlert.warningImmediate,now=.01);b.tick(.01)
  start=b.pattern_start
  b.update_alert(beep.AudibleAlert.engage2,now=.015)
  assert b.protected and b.pattern_start==start


def test_persistent_prompt_repeats_but_engage_does_not(buzzer):
  b,p,out=buzzer
  b.update_alert(beep.AudibleAlert.promptRepeat,now=0);b.tick(0)
  for t in (.01,.02,.03,.04,.05,.06):b.tick(t)
  b.update_alert(beep.AudibleAlert.promptRepeat,now=1);b.tick(1);assert out[-1]
  for t in (1.01,1.02,1.03,1.04,1.05,1.06):b.tick(t)
  b.update_alert(beep.AudibleAlert.engage,now=2);b.tick(3)
  b.tick(3.1)
  b.update_alert(beep.AudibleAlert.engage,now=4);b.tick(4);assert out[-1] is False


def test_delayed_tick_does_not_skip_entire_short_warning(buzzer):
  b,p,out=buzzer
  b.update_alert(beep.AudibleAlert.warningImmediate,now=0)
  b.tick(.5);assert out[-1] is True
  b.update_alert(beep.AudibleAlert.warningImmediate,now=2)
  b.tick(2);assert out[-1] is False  # progress, no same-warning restart loop
  b.tick(3);assert out[-1] is True


def test_optional_whitelist_contains_no_critical_or_unknown_enum():
  critical=['disengage','refuse','prompt','promptRepeat','promptDistracted','warningImmediate','warningSoft',
            'bsdWarning','trafficError','stopStop','speedDown','disengage2','longDisengaged',
            *['audio'+str(i) for i in range(1,11)]]
  for name in critical:assert getattr(beep.AudibleAlert,name) not in beep.OPTIONAL_ALERTS


@pytest.mark.parametrize('failure',['ownership','open'])
def test_gpio_initialization_failure_uses_legacy_writer(monkeypatch,failure):
  import subprocess
  b=beep.Beepd.__new__(beep.Beepd);b.gpio=None;b.gpio_fallback_logged=False
  writes=[]
  def run(args,**kw):
    if isinstance(args,list) and 'chown' in args and failure=='ownership':
      raise subprocess.CalledProcessError(1,args)
    if 'input' in kw:writes.append(kw['input'])
    return NS(returncode=0)
  monkeypatch.setattr(beep.subprocess,'run',run)
  monkeypatch.setattr(beep.os,'getuid',lambda:1000,raising=False)
  monkeypatch.setattr(beep.os,'getgid',lambda:1000,raising=False)
  def fail_open(*a,**kw):raise OSError('test permission failure')
  monkeypatch.setattr('builtins.open',fail_open)
  b.enable_gpio();b._write_gpio(True);b._write_gpio(False)
  assert b.gpio is None and b.gpio_fallback_logged and writes==['1','0']


def test_persistent_gpio_writer_has_no_per_edge_subprocess(monkeypatch):
  import io
  b=beep.Beepd.__new__(beep.Beepd);b.gpio=io.StringIO()
  def forbidden(*a,**kw):raise AssertionError('Per-edge subprocess')
  monkeypatch.setattr(beep.subprocess,'run',forbidden)
  b._write_gpio(True);assert b.gpio.getvalue()=='1'
  b._write_gpio(False);assert b.gpio.getvalue()=='0'


def soundd_functions():
  path=Path(__file__).resolve().parents[2]/'ui/soundd.py'
  tree=ast.parse(path.read_text(encoding='utf-8'))
  tree.body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('main','check_selfdrive_timeout_alert')]
  return tree


def test_stock_timeout_window_under_mute(buzzer):
  b,p,out=buzzer;p.mute=True
  clock=NS(monotonic=lambda:6.)
  ns=dict(time=clock,SELFDRIVE_STATE_TIMEOUT=5)
  exec(compile(soundd_functions(),'soundd.py','exec'),ns)
  class SM(dict):pass
  sm=SM(selfdriveState=NS(enabled=True));sm.recv_time={'selfdriveState':0};sm.updated={'selfdriveState':False}
  assert ns['check_selfdrive_timeout_alert'](sm)
  b.get_audible_alert(sm,ns['check_selfdrive_timeout_alert']);assert b.protected
  clock.monotonic=lambda:16.
  assert not ns['check_selfdrive_timeout_alert'](sm)
  sm['selfdriveState'].enabled=False;clock.monotonic=lambda:6.
  assert not ns['check_selfdrive_timeout_alert'](sm)


def test_countdown_protected_but_not_fabricated_without_new_state(buzzer):
  b,p,out=buzzer;p.mute=True
  class SM(dict):pass
  sm=SM(selfdriveState=NS(alertSound=NS(raw=beep.AudibleAlert.none)),carrotMan=NS(leftSec=2))
  sm.updated={'selfdriveState':True}
  b.get_audible_alert(sm,lambda s:False)
  assert b.current_alert==beep.AudibleAlert.audio2 and b.protected


def test_nonlite_main_keeps_speaker_and_lite_routes_gpio(monkeypatch):
  calls=[];flag=0
  ns=dict(Params=lambda:NS(get_int=lambda k:flag),Soundd=lambda:NS(soundd_thread=lambda:calls.append('speaker')),
          check_selfdrive_timeout_alert=object())
  exec(compile(soundd_functions(),'soundd.py','exec'),ns)
  monkeypatch.setattr(beep,'Beepd',lambda:NS(beepd_thread=lambda cb:calls.append('gpio')))
  ns['main']();assert calls==['speaker']
  flag=1;ns['main']();assert calls==['speaker','gpio']
