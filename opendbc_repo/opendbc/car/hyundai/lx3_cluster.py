"""LX3 display ownership. This never grants actuator permission."""


class Lx3ClusterTransport:
  SOURCES = {
    'LFAHDA_CLUSTER': 'lfahda_cluster',
    'ADRV_0x161': 'adrv_0x161',
    'CCNC_0x162': 'ccnc_0x162',
    'ADRV_0x1ea': 'adrv_0x1ea',
    'ADRV_0x200': 'adrv_0x200',
  }
  MAX_AGE_NS = 100_000_000

  def __init__(self):
    self.used = {}
    self.parser = None
    self.cs = None
    self.now_ns = 0
    self.active = False

  def begin(self, cs, now_ns, active):
    parser = getattr(cs, 'cp_cam', None)
    reset = self.parser is not None and parser is not self.parser or now_ns < self.now_ns
    self.cs, self.parser, self.now_ns = cs, parser, now_ns
    self.active = bool(active) and parser is not None
    if reset:
      self.used.clear()
    if not self.active or reset:
      # An already-forwarded inactive publication must not be replayed on
      # engage. A new parser/clock epoch also needs a new original publication.
      for name in self.SOURCES:
        self.used[name] = self._publication(name)

  def _publication(self, name):
    if self.parser is None:
      return None
    values = self.parser.vl.get(name)
    stamp = self.parser.ts_nanos.get(name, {}).get('CHECKSUM', 0)
    if values is None or stamp <= 0 or values is not getattr(self.cs, self.SOURCES[name], None):
      return None
    return stamp, values.get('COUNTER')

  def claim(self, name):
    publication = self._publication(name)
    if not self.active or publication is None or publication == self.used.get(name):
      return False
    age_ns = self.now_ns - publication[0]
    if not 0 <= age_ns <= self.MAX_AGE_NS:
      return False
    self.used[name] = publication
    return True
