"""Read-only localhost report server with byte-range video seeking."""
import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import re


class Handler(SimpleHTTPRequestHandler):
  def send_head(self):
    path=Path(self.translate_path(self.path)).resolve()
    if not path.is_relative_to(Path(self.directory).resolve()):
      self.send_error(403);return None
    if path.is_dir():
      path=path/'index.html'
    try:stream=path.open('rb')
    except OSError:self.send_error(404);return None
    size=path.stat().st_size;start,end=0,size-1
    request=self.headers.get('Range')
    if request:
      match=re.fullmatch(r'bytes=(\d*)-(\d*)',request)
      try:
        if not match or not any(match.groups()):raise ValueError()
        a,b=match.groups()
        if a:start=int(a);end=min(int(b),size-1) if b else size-1
        else:start=max(0,size-int(b))
        if not 0<=start<=end<size:raise ValueError()
      except ValueError:
        stream.close();self.send_response(416);self.send_header('Content-Range',f'bytes */{size}');self.send_header('Content-Length','0');self.end_headers();return None
    self.send_response(206 if request else 200)
    self.send_header('Content-Type',self.guess_type(str(path)))
    self.send_header('Accept-Ranges','bytes')
    self.send_header('Content-Length',str(end-start+1))
    self.send_header('Cache-Control','no-store')
    if request:self.send_header('Content-Range',f'bytes {start}-{end}/{size}')
    self.end_headers();stream.seek(start);self.remaining=end-start+1
    return stream

  def copyfile(self,source,outputfile):
    left=self.remaining
    try:
      while left:
        block=source.read(min(left,65536))
        if not block:break
        outputfile.write(block);left-=len(block)
    except (BrokenPipeError,ConnectionResetError):pass


def main():
  p=argparse.ArgumentParser(description=__doc__);p.add_argument('--out',type=Path,required=True);p.add_argument('--port',type=int,default=8769);a=p.parse_args()
  root=a.out.resolve();assert (root/'index.html').is_file()
  server=ThreadingHTTPServer(('127.0.0.1',a.port),partial(Handler,directory=str(root)))
  print(f'http://127.0.0.1:{a.port}/index.html',flush=True)
  server.serve_forever()


if __name__=='__main__':main()
