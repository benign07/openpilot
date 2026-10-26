"""Regression for the published planner clock units without native solver imports."""
import ast
from pathlib import Path
from types import SimpleNamespace as NS

import pytest


def production_publish():
  source = Path(__file__).parents[1] / "lib" / "longitudinal_planner.py"
  tree = ast.parse(source.read_text(encoding="utf-8"))
  cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "LongitudinalPlanner")
  method = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "publish")
  module = ast.Module(body=[method], type_ignores=[])
  return compile(ast.fix_missing_locations(module), str(source), "exec")


@pytest.mark.parametrize("elapsed_ns", [20_000_000, 50_000_000, -10_000_000])
def test_published_delay_uses_seconds_and_preserves_control_output(elapsed_ns):
  # Use a long-running monotonic clock; the previous seconds-minus-ns formula
  # produced a huge negative value even for an ordinary 20/50 ms delay.
  model_ns = 3_500_000_000_000
  message = NS(logMonoTime=model_ns + elapsed_ns, longitudinalPlan=NS())
  namespace = {"messaging": NS(new_message=lambda name: message)}
  exec(production_publish(), namespace)
  array = lambda values: NS(tolist=lambda: values)
  planner = NS(
    mpc=NS(solve_time=0.004, source="cruise", t_follow=1.5, desired_distance=30.0),
    v_desired_trajectory=array([15.0, 15.1]), a_desired_trajectory=array([0.1, 0.2]),
    j_desired_trajectory=array([0.3, 0.4]), fcw=False, output_a_target=0.2,
    output_v_target_now=15.0, output_j_target_now=0.3, output_should_stop=False,
    allow_throttle=True, v_cruise_kph=60.0, long_evidence_pending=None,
  )
  class State(dict):
    logMonoTime = {"modelV2": model_ns}
    def all_checks(self, service_list):
      return True
  state = State(radarState=NS(leadOne=NS(status=True)))
  carrot = NS(xState=NS(value=0), trafficState=NS(value=1),
              events=NS(to_msg=lambda: []), myDrivingMode=NS(value=2))
  sent = []
  namespace["publish"](planner, state, NS(send=lambda name, msg: sent.append((name, msg))), carrot)
  plan = message.longitudinalPlan
  assert sent == [("longitudinalPlan", message)]
  assert message.valid
  assert plan.modelMonoTime == model_ns
  assert plan.processingDelay == pytest.approx(elapsed_ns / 1_000_000_000)
  assert plan.speeds == [15.0, 15.1] and plan.accels == [0.1, 0.2]
  assert plan.jerks == [0.3, 0.4]
  assert (plan.aTarget, plan.vTargetNow, plan.jTargetNow) == (0.2, 15.0, 0.3)
  assert plan.solverExecutionTime == 0.004
