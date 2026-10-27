"""Keep schema generations and actual controller output distinct in passive logs."""
from types import SimpleNamespace as NS

from selfdrive.carrot.can_diagnostics.automatic_runtime import FIELDS, selected_fields, vehicle_capture_context, panda_summary


def test_current_radar_error_struct_is_not_silently_missing():
  radar = NS(leadOne=None, leadTwo=None, radarErrors=NS(to_dict=lambda: {'canError': True, 'radarFault': False}))
  row = selected_fields(radar, FIELDS['radarState'])
  assert row['radarErrors'] == {'canError': True, 'radarFault': False}
  assert row['errors'] is None


def test_legacy_radar_error_list_remains_readable():
  row = selected_fields(NS(leadOne=None, leadTwo=None, errors=['canError']), FIELDS['radarState'])
  assert row['errors'] == ['canError'] and row['radarErrors'] is None


def test_actuator_output_is_captured_separately_from_requested_actuators():
  out = NS(actuatorsOutput=NS(to_dict=lambda: {'accel': .36, 'torqueOutputCan': 125.}))
  assert selected_fields(out, FIELDS['carOutput']) == {
    'actuatorsOutput': {'accel': .36, 'torqueOutputCan': 125.}}


def test_longitudinal_parameters_record_observed_values_without_inventing_defaults():
  cp = NS(passive=False, dashcamOnly=False, safetyConfigs=[], startingState=False, startAccel=1.,
          stopAccel=-2., vEgoStarting=.1, openpilotLongitudinalControl=True)
  row = vehicle_capture_context(cp)
  assert row['longitudinal_control']['startingState'] is False
  assert row['longitudinal_control']['startAccel'] == 1.
  assert row['longitudinal_control']['longitudinalActuatorDelay'] is None
  assert row['scope'] == 'host_CarParams_not_live_Panda_mode_or_physical_relay_verification'


def test_bus_startup_reset_and_transmit_errors_are_not_lost_in_spi_evidence():
  bus = NS(totalRxCnt=50, totalRxLostCnt=0, totalErrorCnt=4, busOff=False,
           totalTxCnt=61, totalTxLostCnt=8, canCoreResetCnt=11, transmitErrorCnt=123,
           receiveErrorCnt=0, errorWarning=True)
  p = NS(canState0=bus, canState1=bus, canState2=bus, safetyModel='hyundaiCanfd', safetyParam=2238,
         controlsAllowed=False, safetyTxBlocked=0, rxBufferOverflow=0, txBufferOverflow=0,
         spiChecksumErrorCount=1, safetyRxChecksInvalid=False, faults=[])
  row = panda_summary(p)['buses'][1]
  assert (row['tx'], row['tx_lost'], row['core_resets'], row['tx_errors'], row['error_warning']) == (61,8,11,123,True)
