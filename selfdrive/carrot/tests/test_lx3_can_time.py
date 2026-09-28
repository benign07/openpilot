"""Offline calendar/parser/clock recovery tests. No sockets, CAN writes or date command."""
import ast
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
import datetime
from functools import cache
import math
import numbers
import os
from pathlib import Path
import re
import runpy
import time
from types import SimpleNamespace as NS
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[3]
OP = ROOT / 'opendbc_repo/opendbc'
DBC_FILE = OP / 'dbc/generator/hyundai/hyundai_canfd_lx3_hev.dbc'
CLOCK = runpy.run_path(str(OP / 'car/hyundai/lx3_time.py'))
Lx3Clock = CLOCK['Lx3Clock']


def definitions(path, env, names=None):
  tree = ast.parse(path.read_text(encoding='utf-8'))
  nodes = [n for n in tree.body if not isinstance(n,(ast.Import,ast.ImportFrom))]
  if names is not None:
    nodes = [n for n in nodes if getattr(n,'name',None) in names]
  exec(compile(ast.Module(body=nodes,type_ignores=[]),str(path),'exec'),env)
  return env


def parser_environment():
  env=dict(globals(), DBC_PATH=str(OP/'dbc'), carlog=NS(warning=lambda *a:None))
  env['CRC16_XMODEM']=runpy.run_path(str(OP/'car/crc.py'))['CRC16_XMODEM']
  definitions(OP/'car/hyundai/hyundaicanfd.py',env,{'hkg_can_fd_checksum'})
  definitions(OP/'can/dbc.py',env)
  definitions(OP/'can/parser.py',env)
  factory_env=dict(env, Bus=NS(pt=0,cam=2,alt=1), CAR=NS(HYUNDAI_PALISADE_LX3_HEV='lx3'),
             HyundaiFlags=NS(CANFD_ALT_BUTTONS=1), CanBus=lambda _:NS(ECAN=0,CAM=2,ACAN=1),
             DBC={'lx3':{0:str(DBC_FILE)}}, LX3_TIME_MESSAGE='LX3_LOCAL_TIME', LX3_UTC_MESSAGE='LX3_UTC_SNAPSHOT')
  node=next(n for n in ast.walk(ast.parse((OP/'car/hyundai/carstate.py').read_text(encoding='utf-8')))
            if isinstance(n,ast.FunctionDef) and n.name=='get_can_parsers_canfd')
  exec(compile(ast.Module(body=[node],type_ignores=[]),'carstate-parser-factory','exec'),factory_env)
  env['get_can_parsers_canfd']=factory_env['get_can_parsers_canfd']
  return env


ENV = parser_environment()


def payload(stamp, counter=0, utc=False):
  if utc:
    stamp=stamp.astimezone(datetime.timezone.utc)
    d=bytearray([255]*32)
    d[2]=counter
    d[8:10]=stamp.year.to_bytes(2,'little')
    d[10:15]=bytes((stamp.month,stamp.day,stamp.hour,stamp.minute,stamp.second))
    d[:2]=ENV['hkg_can_fd_checksum'](0x417,None,d).to_bytes(2,'little')
    return bytes(d)
  d=bytearray([255]*16)
  d[2]=counter
  d[8]=0x4a
  d[9:12]=bytes((stamp.hour,stamp.minute,stamp.second))
  d[12]=0x43 | (stamp.month<<2)
  d[13:15]=bytes((stamp.year-2000,stamp.day))
  d[15]=0xfe
  d[:2]=ENV['hkg_can_fd_checksum'](0x41b,None,d).to_bytes(2,'little')
  return bytes(d)


class CalendarTests(unittest.TestCase):
  def setUp(self):
    self.parser=ENV['get_can_parsers_canfd'](None,NS(carFingerprint='lx3',flags=1))[0]
    self.clock=Lx3Clock()
    self.date=datetime.datetime(2026,4,29,8,7,58,tzinfo=CLOCK['KST'])

  def feed(self,seconds,stamp=None,utc_offset=0,counter=None,corrupt=False):
    ns=int((10+seconds)*1e9)
    stamp=stamp or self.date+datetime.timedelta(seconds=seconds)
    d=bytearray(payload(stamp,(int(seconds)*17)%256 if counter is None else counter))
    if corrupt: d[10]^=1
    utc_data=payload(stamp+datetime.timedelta(seconds=utc_offset),utc=True)
    self.parser.update([[ns,[(0x41b,bytes(d),0),(0x417,utc_data,0)]]])
    return self.clock.update(self.parser.vl['LX3_LOCAL_TIME'],self.parser.ts_nanos['LX3_LOCAL_TIME']['SECONDS'],ns,
                             CLOCK['utc_snapshot_millis'](self.parser.vl['LX3_UTC_SNAPSHOT']),ns)

  def test_dbc_crc_is_bound_only_to_optional_time(self):
    state=self.parser.message_states[0x41b]
    self.assertTrue(state.ignore_alive)
    self.assertIsNotNone(next(s.calc_checksum for s in state.signals if s.name=='CHECKSUM'))
    self.assertFalse(any(s.type==1 for s in state.signals))

  def test_two_advances_and_minute_rollover(self):
    self.assertEqual(self.feed(0),0)
    self.assertEqual(self.feed(1),0)
    self.assertEqual(self.feed(2),int((self.date+datetime.timedelta(seconds=2)).timestamp()*1000))
    self.assertTrue(self.parser.can_valid)

  def test_midnight_month_and_year_rollovers(self):
    for stamp in (datetime.datetime(2026,4,30,23,59,58,tzinfo=CLOCK['KST']),
                  datetime.datetime(2026,12,31,23,59,58,tzinfo=CLOCK['KST'])):
      self.setUp()
      self.date=stamp
      for i in range(4): result=self.feed(i)
      self.assertEqual(result,int((stamp+datetime.timedelta(seconds=3)).timestamp()*1000))

  def test_corrupt_crc_cannot_seed_or_refresh_clock(self):
    for i in range(3): self.assertEqual(self.feed(i,corrupt=True),0)
    for i in range(3,6): self.feed(i)
    self.assertGreater(self.feed(6),0)
    for i in range(7,10): result=self.feed(i,corrupt=True)
    self.assertEqual(result,0)
    self.assertTrue(self.parser.can_valid)

  def test_missing_clock_and_counter_jumps_do_not_break_driving_validity(self):
    self.parser.update([[10_000_000_000,[(123,b'\0'*8,0)]]])
    self.assertTrue(self.parser.can_valid)
    for i in range(10): self.feed(i,counter=(i*59)%256)
    self.assertTrue(self.parser.can_valid)
    self.assertEqual(self.parser.message_states[0x41b].counter_fail,0)

  def test_wrong_timezone_never_produces_a_time(self):
    for i in range(5): self.assertEqual(self.feed(i,utc_offset=3600),0)

  def test_frozen_fresh_messages_expire(self):
    for i in range(3): self.feed(i)
    frozen=self.date+datetime.timedelta(seconds=2)
    for i in range(3,7): result=self.feed(i,stamp=frozen)
    self.assertEqual(result,0)

  def test_cached_missing_frames_expire(self):
    for i in range(3): self.feed(i)
    self.assertEqual(self.clock.update(self.parser.vl['LX3_LOCAL_TIME'],12_000_000_000,15_000_000_000),0)

  def test_missing_or_stale_utc_cannot_confirm_timezone(self):
    for i in range(4):
      self.feed(i,utc_offset=600)
    self.assertEqual(self.clock.update(self.parser.vl['LX3_LOCAL_TIME'],13_000_000_000,13_000_000_000,
                                      self.clock.millis,1_000_000_000),0)

  def test_later_frozen_utc_does_not_freeze_verified_local_clock(self):
    for i in range(3): self.feed(i)
    self.assertGreater(self.feed(3,utc_offset=-180),0)

  def test_backward_and_large_forward_jumps_need_new_validation(self):
    for delta in (-10,3600):
      self.setUp()
      for i in range(3): self.feed(i)
      self.assertEqual(self.feed(3,stamp=self.date+datetime.timedelta(seconds=delta)),0)

  def test_invalid_calendar_and_reserved_values_rejected(self):
    base=dict(YEAR=26,MONTH=4,DATE=30,HOURS=19,MINUTES=59,SECONDS=59)
    for k,v in (('MONTH',0),('MONTH',13),('DATE',31),('HOURS',24),('MINUTES',60),('SECONDS',255),('YEAR',255)):
      self.assertEqual(CLOCK['calendar_millis'](base|{k:v}),0)


class SystemClockTests(unittest.TestCase):
  def setUp(self):
    self.target=datetime.datetime(2026,9,28,12,0)
    self.cs=NS(datetime=int(self.target.replace(tzinfo=datetime.timezone.utc).timestamp()*1000),
               canValid=True,canTimeout=False,gearShifter='park',standstill=True,vEgo=0.)
    self.cc=NS(enabled=False,latActive=False,longActive=False)
    self.ss=NS(enabled=False,active=False)
    class Sm(dict):
      pass
    self.sm=Sm(carState=self.cs,carControl=self.cc,selfdriveState=self.ss,
               gpsLocationExternal=NS(hasFix=False,unixTimestampMillis=self.cs.datetime))
    self.sm.valid={s:True for s in self.sm}
    self.sm.logMonoTime={s:100_000_000_000 for s in self.sm}
    self.sm.updated={s:True for s in self.sm}
    self.env=dict(datetime=datetime,min_date=lambda:datetime.datetime(2025,1,1),MAX_DATE=datetime.datetime(2035,1,1))
    definitions(ROOT/'system/timed.py',self.env,{'parked_can_time'})

  def candidate(self):
    return self.env['parked_can_time'](self.sm,100_000_000_000)

  def test_parked_fresh_calendar_is_utc(self):
    self.assertEqual(self.candidate(),self.target)

  def test_motion_gear_control_and_invalid_can_block_correction(self):
    for object_name,field_name,value in [('cs','vEgo',.1),('cs','vEgo',float('nan')),('cs','gearShifter','drive'),
        ('cs','standstill',False),('cs','canValid',False),('cs','canTimeout',True),('cc','enabled',True),
        ('cc','latActive',True),('cc','longActive',True),('ss','active',True),('ss','enabled',True)]:
      self.setUp()
      setattr(getattr(self,object_name),field_name,value)
      self.assertIsNone(self.candidate(),field_name)

  def test_stale_future_invalid_telemetry_blocks_correction(self):
    for service in ('carState','carControl','selfdriveState'):
      for offset in (-600_000_000,1):
        self.setUp()
        self.sm.logMonoTime[service]+=offset
        self.assertIsNone(self.candidate())
      self.setUp()
      self.sm.valid[service]=False
      self.assertIsNone(self.candidate())

  def test_zero_or_out_of_bounds_calendar_rejected(self):
    for ms in (0,1,4102444800000):
      self.cs.datetime=ms
      self.assertIsNone(self.candidate())

  def main_once(self,valid_clock=False,ntp=False,gps=False,lx3=True,iterations=1):
    self.sm.update=Mock(side_effect=[None]*iterations+[StopIteration])
    self.sm['gpsLocationExternal'].hasFix=gps
    env=dict(self.env,Params=lambda:None,get_gps_location_service=lambda _: 'gpsLocationExternal',
             messaging=NS(PubMaster=lambda _:NS(send=lambda *a:None),SubMaster=lambda _:self.sm,
                          new_message=lambda _:NS(clocks=NS())),
             time=NS(monotonic=lambda:100,monotonic_ns=lambda:100_000_000_000,time_ns=lambda:0,sleep=lambda _:None),
             system_time_valid=lambda:valid_clock,Path=lambda _:NS(exists=lambda:ntp),
             has_lx3_calendar=lambda _:lx3,NoReturn=object,set_time=Mock(return_value=True),cloudlog=NS(info=lambda _:None))
    definitions(ROOT/'system/timed.py',env,{'main'})
    with self.assertRaises(StopIteration): env['main']()
    return env['set_time']

  def test_invalid_boot_clock_recovers(self):
    self.main_once(iterations=3).assert_called_once_with(self.target)

  def test_valid_clock_ntp_and_other_vehicles_are_never_overridden(self):
    for kwargs in ({'valid_clock':True},{'ntp':True},{'lx3':False}):
      self.setUp()
      self.main_once(**kwargs).assert_not_called()

  def test_gps_remains_primary_and_is_interpreted_as_utc(self):
    self.main_once(gps=True).assert_called_once_with(self.target)

  def test_invalid_gps_date_does_not_block_valid_car_fallback(self):
    self.sm['gpsLocationExternal'].unixTimestampMillis=0
    self.main_once(gps=True).assert_called_once_with(self.target)


if __name__=='__main__':
  unittest.main()
