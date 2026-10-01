"""Exercise built ELF MPC extensions from a copied installation tree, no CAN I/O.

Run after SCons on Linux. An absolute DT_NEEDED is rejected even if the old
build directory still exists, so it cannot hide a packaging regression.
"""
import argparse
import importlib.util
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile


def load_solver(tree, model):
  import numpy as np

  folder = 'lateral' if model == 'lat' else 'longitudinal'
  extension = tree / f'selfdrive/controls/lib/{folder}_mpc_lib/c_generated_code/acados_ocp_solver_pyx.so'
  spec = importlib.util.spec_from_file_location('acados_ocp_solver_pyx', extension)
  module = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(module)
  solver = module.AcadosOcpSolverCython(model, 'SQP_RTI', 32 if model == 'lat' else 12)
  solver.reset()
  expected_dim = 4 if model == 'lat' else 3
  state = solver.get(0, 'x')
  assert state.shape == (expected_dim,) and np.isfinite(state).all(), state
  solver.set(0, 'x', np.zeros(expected_dim))
  assert np.array_equal(solver.get(0, 'x'), np.zeros(expected_dim))
  native_prefixes = ('libacados_ocp_solver_', 'libacados.', 'libhpipm.', 'libblasfeo.', 'libqpOASES_e.')
  mapped = set()
  for line in Path('/proc/self/maps').read_text().splitlines():
    parts = line.split(maxsplit=5)
    if len(parts) < 6:
      continue
    path = Path(parts[5])
    if path.name.startswith(native_prefixes):
      assert path.resolve().is_relative_to(tree.resolve()), ('original-tree library used', path)
      mapped.add(path.name)
  assert {f'libacados_ocp_solver_{model}.so', 'libacados.so', 'libhpipm.so', 'libblasfeo.so'} <= mapped, mapped
  print(f'PASS relocated {model} Cython import, native solver creation/reset/get/set', flush=True)


def dynamic_metadata(artifact):
  dynamic = subprocess.check_output(['readelf', '-d', str(artifact)], text=True)
  needed = re.findall(r'\(NEEDED\).*?\[(.*?)\]', dynamic)
  paths = [entry for path in re.findall(r'\((?:RPATH|RUNPATH)\).*?\[(.*?)\]', dynamic) for entry in path.split(':')]
  assert needed and all('/' not in value for value in needed), (artifact, needed)
  assert all(entry == '$ORIGIN' or entry.startswith('$ORIGIN/') for entry in paths), (artifact, paths)
  return paths


def check_vendored(repo, arch):
  libraries = sorted((repo / f'third_party/acados/{arch}/lib').glob('*.so'))
  assert libraries, arch
  for artifact in libraries:
    dynamic_metadata(artifact)
  print(f'PASS {arch} vendored acados ELF dependency/path metadata (static inspection only)', flush=True)


def check_relocation(repo):
  assert sys.platform.startswith('linux'), 'This checks ELF artifacts on Linux'
  arch = os.uname().machine
  if arch == 'aarch64' and Path('/TICI').is_file():
    arch = 'larch64'
  assert (repo / f'third_party/acados/{arch}/lib').is_dir(), arch
  check_vendored(repo, arch)
  with tempfile.TemporaryDirectory(prefix='mpc-relocation-') as temporary:
    installed = Path(temporary) / 'installed'
    for folder in ('lateral', 'longitudinal'):
      relative = Path(f'selfdrive/controls/lib/{folder}_mpc_lib/c_generated_code')
      original = repo / relative
      target = installed / relative
      target.mkdir(parents=True)
      for name in ('acados_ocp_solver_pyx.so', f'libacados_ocp_solver_{"lat" if folder == "lateral" else "long"}.so'):
        artifact = original / name
        paths = dynamic_metadata(artifact)
        expected = '$ORIGIN' if name == 'acados_ocp_solver_pyx.so' else f'$ORIGIN/../../../../../third_party/acados/{arch}/lib'
        assert expected in paths, (artifact, paths, expected)
        shutil.copy2(artifact, target / name)
    relative_libs = Path(f'third_party/acados/{arch}/lib')
    shutil.copytree(repo / relative_libs, installed / relative_libs)
    env = dict(os.environ)
    env.pop('LD_LIBRARY_PATH', None)
    # Import the extension in a fresh process; earlier imports cannot satisfy
    # the dependency from the original tree's already loaded native libraries.
    for model in ('lat', 'long'):
      subprocess.run([sys.executable, str(Path(__file__).resolve()), '--load', model, '--tree', str(installed)],
                     cwd=installed, env=env, check=True, timeout=30)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--load', choices=('lat', 'long'))
  parser.add_argument('--static-vendored-arch', help='Inspect ELF dependencies without loading foreign architecture libraries')
  parser.add_argument('--tree', type=Path, default=Path(__file__).resolve().parents[3])
  args = parser.parse_args()
  if args.load:
    load_solver(args.tree, args.load)
  elif args.static_vendored_arch:
    check_vendored(args.tree.resolve(), args.static_vendored_arch)
  else:
    check_relocation(args.tree.resolve())
