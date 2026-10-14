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


def sync(base, root, maximum=4):
  base = checked_base(base)
  routes=set()
  for path in root.glob('*.jsonl.gz'):
    with gzip.open(path,'rt',encoding='utf-8') as stream:
      header=json.loads(stream.readline(2*1024*1024))
      if header.get('kind')=='header' and isinstance(header.get('route'),str): routes.add(header['route'])
  state=fetch(base,'/api/params_bulk?names=CurrentRoute')
  current=state.get('values',{}).get('CurrentRoute')
  if not current: raise ValueError('Current route identity unavailable; do not guess the active encoder')
  result={'downloaded':[],'scope':'completed_qcamera_only_local_hash_not_remote_signed_hash'}
  deadline=time.monotonic()+30
  for route in sorted(routes,reverse=True)[:5]:
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,110}',route):continue
    data=fetch(base,'/api/dashcam/segments/'+quote(route,safe='')+'?limit=2000')
    if not data.get('ok') or data.get('route') != route:continue
    for segment in completed_segments(route,data.get('segments',[]),current):
      directory=root/'video_segments'/segment;directory.mkdir(parents=True,exist_ok=True)
      target=directory/'qcamera.ts';manifest=directory/'qcamera.source.json'
      if target.is_file() and manifest.is_file():
        m=json.loads(manifest.read_text())
        if target.stat().st_size==m['bytes'] and hashlib.sha256(target.read_bytes()).hexdigest()==m['sha256']:continue
      if len(result['downloaded'])>=maximum or time.monotonic()>=deadline:return result
      path='/api/dashcam/download/'+quote(segment,safe='')+'/qcamera';url=base+path
      partial=directory/'qcamera.partial'
      with OPENER.open(Request(url),timeout=15) as response:
        if response.url!=url or response.status!=200:raise ValueError('Video source changed')
        length=int(response.headers.get('Content-Length','0'))
        filename=response.headers.get('Content-Disposition','')
        if not 0<length<=32*1024*1024 or not filename.endswith('--qcamera.ts"'):
          raise ValueError('Unsupported video length/format')
        if shutil.disk_usage(root).free<2*1024**3+length:raise OSError('PC recording reserve reached')
        h=hashlib.sha256();received=0
        with partial.open('wb') as output:
          while block:=response.read(65536):
            if time.monotonic()>deadline:raise TimeoutError('Video pass budget reached; resume report without new video')
            received+=len(block)
            if received>length:raise ValueError('Video grew beyond closed response size')
            output.write(block);h.update(block)
        if received!=length:raise ValueError('Incomplete video remains partial; retry on next sync')
      partial.replace(target)
      temp=manifest.with_suffix('.tmp');temp.write_text(json.dumps({'bytes':length,'sha256':h.hexdigest(),'segment':segment}),encoding='utf-8');temp.replace(manifest)
      result['downloaded'].append(segment)
  return result
