# Run only in a prepared original-native profile, never in the live checkout.
import os
import sysconfig
from pathlib import Path
import numpy as np
import json
import hashlib

AddOption('--minimal',action='store_false',dest='extras',default=True)
root=Path(Dir('#').abspath)
sdk=Path(ARGUMENTS['sdk']).resolve()
arch='larch64'
profile=json.loads((root/'profile.json').read_text())
source_sha=hashlib.sha256((root/'selfdrive/pandad/spi.cc').read_bytes()).hexdigest()
env=Environment(tools=['default','cython'],toolpath=[str(root/'site_scons/site_tools')],
  ENV={**os.environ,'PATH':str(sdk/'capnproto/install/bin')+':'+os.environ['PATH']},
  CC='clang',CXX='clang++',
  CCFLAGS=['-g','-fPIC','-O2','-Wunused','-Werror','-Wshadow','-D__TICI__','-mcpu=cortex-a57',
    '-Wno-unknown-warning-option','-Wno-inconsistent-missing-override','-Wno-c99-designator',
    '-Wno-reorder-init-list','-Wno-vla-cxx-extension'],
  CFLAGS=['-std=gnu11'],CXXFLAGS=['-std=c++1z'],
  CPPPATH=['#','#msgq','#third_party','#third_party/json11','#third_party/linux/include',
    '#third_party/catch2/include',str(sdk/'capnproto/install/include'),str(sdk/'zeromq/install/include')],
  LIBPATH=['#common','#msgq_repo','#third_party','#selfdrive/pandad',str(sdk/'capnproto/install/lib'),str(sdk/'zeromq/install/lib')],
  LINKFLAGS=['-Wl,--as-needed','-Wl,--no-undefined'],RPATH=[],CYTHONCFILESUFFIX='.cpp')
envCython=env.Clone();envCython['CPPPATH'] += [sysconfig.get_paths()['include'],np.get_include()]
if profile['profile']!='baseline':
  env.Append(CPPDEFINES=[('LX3_SPI_READY_PROFILE',profile['spi_ready_profile']),
    ('LX3_SPI_SOURCE_SHA','\\"'+source_sha+'\\"')])
envCython['CCFLAGS'].remove('-Werror');envCython['LIBS']=[];envCython['LINKFLAGS']=['-pthread','-shared']
np_version=Value(np.__version__);Export('env','envCython','arch','np_version')
SConscript('common/SConscript');Import('_common');common=[_common,'json11','zmq'];Export('common')
env_swaglog=env.Clone();env_swaglog['CXXFLAGS'].append('-DSWAGLOG="\\"common/swaglog.h\\""')
SConscript('msgq_repo/SConscript',exports={'env':env_swaglog});Import('msgq')
SConscript('cereal/SConscript');Import('socketmaster');messaging=[socketmaster,msgq,'capnp','kj'];Export('messaging')
# Only the original native target's JSON library is required, not GPU/model code.
json_env=env.Clone()
# Original json11 intentionally uses move through its std using-declaration.
# Keep its sealed bytes; this newer Clang style warning is not a runtime fault.
json_env.Append(CXXFLAGS=['-Wno-unqualified-std-cast-call'])
json_env.Library('third_party/json11', ['third_party/json11/json11.cpp'])
SConscript('selfdrive/pandad/SConscript')
Default('selfdrive/pandad/pandad','selfdrive/pandad/tests/test_pandad_canprotocol')
