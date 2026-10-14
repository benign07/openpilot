"""PC-only single-problem comparison; never a road/comfort counterfactual."""
import copy


def compare(frame):
  p = frame.get('problem') or {}
  if (p.get('mode') != 'acc' or p.get('reset_state') or p.get('reference_modifiers_active')
      or p.get('v_cruise', 0) <= 0 or p.get('solution_status') != 0):
    return {'status': 'unsupported_problem', 'control_changes': False}
  try:
    import numpy as np
    from openpilot.selfdrive.controls.lib.longitudinal_mpc_lib.long_mpc import (
      LongitudinalMpc, N, T_IDXS, T_DIFFS, get_safe_obstacle_distance)
    from openpilot.selfdrive.controls.lib.drive_helpers import get_accel_from_plan, CONTROL_N
    from openpilot.selfdrive.modeld.constants import ModelConstants
  except (ImportError, OSError) as exc:
    return {'status': 'native_solver_unavailable', 'detail': type(exc).__name__}
  required = {'params': (N+1, 8), 'yref': (N+1, 6), 'x0': (3,),
              'warm_x': (N+1, 3), 'warm_u': (N, 1), 'output_x': (N+1, 3), 'obstacles': (N+1, 4)}
  try:
    a = {key: np.array(p[key], dtype=float) for key in required}
    if any(x.shape != required[k] or not np.isfinite(x).all() for k,x in a.items()):
      raise ValueError('invalid dimensions or nonfinite inputs')
    if not np.array_equal(p['t_idxs'], T_IDXS) or not np.array_equal(p['t_diffs'], T_DIFFS):
      raise ValueError('different solver horizon')
    if np.any(a['yref'][:, 0] != 0.):
      raise ValueError('comfort reference needs a separate replay contract')
    def solve(cap=None):
      m = LongitudinalMpc()
      m.set_cost_weights(p['weights'], p['constraint_weights'])
      m.x0 = a['x0'].copy()
      m.params = a['params'].copy()
      if cap is not None:
        m.params[:, 1] = max(0., cap)
        lower = m.x0[1] + T_IDXS * p['cruise_min_a'] * 1.05
        upper = m.x0[1] + T_IDXS * cap * 1.05
        vc = np.clip(np.full(N+1, p['v_cruise']), lower, upper)
        obstacles = a['obstacles'].copy()
        obstacles[:, 2] = np.cumsum(T_DIFFS * vc) + get_safe_obstacle_distance(
          vc, m.params[:,4], m.params[:,6], m.params[:,7])
        m.params[:,2] = np.min(obstacles, axis=1)
      for i in range(N+1):
        m.solver.set(i, 'x', a['warm_x'][i].copy())
        m.solver.set(i, 'yref', a['yref'][i] if i < N else a['yref'][i,:5])
      for i in range(N):
        m.solver.set(i, 'u', a['warm_u'][i].copy())
      m.run()
      if m.solution_status != 0:
        raise ValueError('native solve failed')
      ts = ModelConstants.T_IDXS[:CONTROL_N]
      v = np.interp(ts, T_IDXS, m.v_solution)
      accel = np.interp(ts, T_IDXS, m.a_solution)
      target, stop, _, _ = get_accel_from_plan(v, accel, ts,
        action_t=frame['output']['actuator_delay']+frame['dt'], vEgoStopping=frame['output']['v_ego_stopping'])
      return m.x_sol.copy(), float(target), bool(stop)
    baseline_x, baseline_target, baseline_stop = solve()
    error = float(np.max(np.abs(baseline_x - a['output_x'])))
    target_error = abs(baseline_target-frame['output']['a_target'])
    if error > .02 or target_error > .02 or baseline_stop != frame['output']['should_stop']:
      return {'status': 'baseline_mismatch', 'state_max_error': error, 'target_error': target_error,
              'reason': 'previous primal solutions do not reproduce internal warm/dual state'}
    x30, target30, stop30 = solve(frame['comparison']['30']['applied_ceiling'])
    x40, target40, stop40 = solve(frame['comparison']['40']['applied_ceiling'])
    return {'status': 'qualified_single_problem', 'baseline_state_error': error, 'baseline_target_error': target_error,
            'a_target30': target30, 'a_target40': target40, 'delta_a_target': target30-target40,
            'should_stop30': stop30, 'should_stop40': stop40,
            'max_horizon_accel_difference': float(np.max(np.abs(x30[:,2]-x40[:,2]))),
            'scope': 'same_recorded_problem_previous_primal_initialization_not_closed_loop_or_placebo_proof',
            'control_changes': False}
  except (ValueError, KeyError, TypeError, RuntimeError) as exc:
    return {'status': 'invalid_or_unreplayable_problem', 'detail': str(exc)[:160]}
