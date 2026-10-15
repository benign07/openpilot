"""PC-only read-only Tailscale retrieval of completed local-record video segments."""
import gzip
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import shutil
import time
from urllib.parse import urlparse, quote
from urllib.request import Request, build_opener, HTTPRedirectHandler

SEGMENT = re.compile(r'^[A-Za-z0-9_-]{1,120}--[0-9]{1,6}$')


class NoRedirect(HTTPRedirectHandler):
  def redirect_request(self,*args,**kwargs):return None


OPENER=build_opener(NoRedirect())


def checked_base(value):
  u = urlparse(value)
  if (u.scheme != 'http' or not u.hostname or ipaddress.ip_address(u.hostname) not in ipaddress.ip_network('100.64.0.0/10')
      or not u.port or u.username or u.password or u.path or u.query or u.fragment):
    raise ValueError('Only the configured private Tailscale device endpoint is supported')
  return value


def fetch(base, path):
  with OPENER.open(Request(base+path), timeout=6) as r:
    if r.url != base+path: raise ValueError('Redirect rejected')
    raw=r.read(2*1024*1024+1)
    if len(raw)>2*1024*1024:raise ValueError('Oversized media catalog')
    return json.loads(raw)


def completed_segments(route, rows, current_route):
  clean = sorted({s for s in rows if isinstance(s,str) and SEGMENT.fullmatch(s) and s.rsplit('--',1)[0] == route},key=lambda s:int(s.rsplit('--',1)[1]))
  # Never read the current route's newest segment while the encoder may write it.
  return clean[:-1] if route == current_route else clean


def cached(target, manifest):
  if not target.is_file() or not manifest.is_file():return False
  try:
    m=json.loads(manifest.read_text(encoding='utf-8'))
    return target.stat().st_size==m['bytes'] and hashlib.sha256(target.read_bytes()).hexdigest()==m['sha256']
  except (OSError,ValueError,KeyError):return False


def retrieve(base, segment, kind, directory, root, deadline):
  names=('qcamera.ts',) if kind=='qcamera' else ('qlog.zst','qlog.bz2')
  for name in names:
    if cached(directory/name,directory/(name+'.source.json')):return False
  url=base+'/api/dashcam/download/'+quote(segment,safe='')+'/'+kind
  partial=directory/(kind+'.partial')
  with OPENER.open(Request(url),timeout=15) as response:
    if response.url!=url or response.status!=200:raise ValueError('Media source changed')
    length=int(response.headers.get('Content-Length','0'))
    disposition=response.headers.get('Content-Disposition','')
    name=next((n for n in names if disposition.endswith('--'+n+'"')),None)
    if not name or not 0<length<=32*1024*1024:raise ValueError('Unsupported media length/format')
    if shutil.disk_usage(root).free<3*1024**3+length:raise OSError('Secondary media reserve reached')
    h=hashlib.sha256();received=0
    with partial.open('wb') as output:
      while block:=response.read(65536):
        if time.monotonic()>deadline:raise TimeoutError('Media pass budget reached; retry next pass')
        received+=len(block)
        if received>length:raise ValueError('Media grew beyond closed response size')
        output.write(block);h.update(block)
    if received!=length:raise ValueError('Incomplete media remains partial; retry next sync')
  target=directory/name;partial.replace(target)
  manifest=directory/(name+'.source.json');temp=manifest.with_suffix('.tmp')
  temp.write_text(json.dumps({'bytes':length,'sha256':h.hexdigest(),'segment':segment,'kind':kind}),encoding='utf-8');temp.replace(manifest)
  return True


def sync(base, root, maximum=4):
  base=checked_base(base);routes={}
  for path in root.glob('*.jsonl.gz'):
    with gzip.open(path,'rt',encoding='utf-8') as stream:
      header=json.loads(stream.readline(2*1024*1024))
      if header.get('kind')=='header' and isinstance(header.get('route'),str):
        routes.setdefault(header['route'],set()).add(header.get('device_dongle_id'))
  state=fetch(base,'/api/params_bulk?names=CurrentRoute,DongleId')
  values=state.get('values',{});current=values.get('CurrentRoute');dongle=values.get('DongleId')
  if not current or not dongle:raise ValueError('Current route/device identity unavailable')
  result={'downloaded':[],'pending_identity':[],
          'scope':'completed_qcamera_and_qlog_same_device_local_hash_not_remote_signed_hash'}
  deadline=time.monotonic()+30
  for route in sorted(routes,reverse=True)[:5]:
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,110}',route):continue
    if routes[route]!={dongle}:
      result['pending_identity'].append(route);continue
    data=fetch(base,'/api/dashcam/segments/'+quote(route,safe='')+'?limit=2000')
    if not data.get('ok') or data.get('route')!=route:continue
    for segment in completed_segments(route,data.get('segments',[]),current):
      directory=root/'video_segments'/segment;directory.mkdir(parents=True,exist_ok=True)
      if len(result['downloaded'])>=maximum or time.monotonic()>=deadline:return result
      # Both artifacts are necessary. Retry qlog even if its video was cached.
      changed=retrieve(base,segment,'qlog',directory,root,deadline)
      changed=retrieve(base,segment,'qcamera',directory,root,deadline) or changed
      if changed:result['downloaded'].append(segment)
  return result
