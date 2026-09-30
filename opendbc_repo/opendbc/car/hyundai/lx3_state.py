"""LX3 camera health input for engagement, independent of cluster output."""


def lateral_fault(values, received_ns, now_ns):
  # 0x162 is observed at 20 Hz. A cached healthy frame is not a live ACK.
  if values is None or received_ns <= 0 or not 0 <= now_ns - received_ns <= 250_000_000:
    return True
  return any(values.get(name, 1) != 0 for name in ('FAULT_LSS', 'FAULT_LFA', 'FAULT_DAS'))
