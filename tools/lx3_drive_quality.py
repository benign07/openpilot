"""Offline log/video evaluation. Never contacts a car or changes parameters.

python tools/lx3_drive_quality.py --input D:/carrot-records --out D:/quality --video
Optional pycapnp/zstandard read rlogs. ffmpeg/ffprobe remux local qcamera to MP4.
Native comparison, when installed on the PC, is baseline-qualified one problem
at a time, not a closed-loop simulation or a detector of subjective placebo.
"""
import argparse
import bisect
from collections import defaultdict
import gzip
import hashlib
import html
import json
import math
from pathlib import Path
import statistics
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
KEY = 'lx3_longitudinal_evidence'


def sha(path):
  return hashlib.sha256(path.read_bytes()).hexdigest()


def unwrap(record):
  if record.get('kind') == 'longitudinal_evidence':
    return record
  msg = record.get('msg')
  return msg.get(KEY) if isinstance(msg, dict) else None


def percentile(values, fraction):
  if not values:
    return None
  v = sorted(values)
  x = (len(v)-1)*fraction
  i = int(x)
  return v[i] + (v[min(i+1,len(v)-1)]-v[i])*(x-i)


def frame_integrity(rows):
  reasons = []
  if not rows:
    return ['missing_detailed_evidence']
  identity = {(r['producer'], r.get('settings_generation')) for r in rows}
  if len(identity) != 1:
    reasons.append('producer_or_settings_changed')
  for a,b in zip(rows, rows[1:]):
    if b['sequence'] != a['sequence']+1 or b['cycle'] != a['cycle']+1 or b['mono_ns']-a['mono_ns'] > 100_000_000:
      reasons.append('planner_evidence_gap')
      break
  if any(r.get('producer_errors',0) for r in rows):
    reasons.append('producer_error_observed')
  for r in rows:
    if set(r.get('services',{})) != {'carState','carControl','controlsState','radarState','modelV2','selfdriveState'}:
      reasons.append('missing_input_validity')
    for service in r.get('services',{}).values():
      age = r['mono_ns']-service.get('mono_ns',0)
      if not service.get('valid') or not service.get('alive') or not 0 <= age < 500_000_000:
        reasons.append('stale_or_invalid_input')
        break
  return sorted(set(reasons))


def summarize_frames(rows, native=False):
  rows = sorted({(r['producer'],r['sequence']):r for r in rows}.values(), key=lambda r:r['mono_ns'])
  gaps = frame_integrity(rows)
  varying = [r for r in rows if abs(r['comparison']['30']['applied_ceiling']-r['comparison']['40']['applied_ceiling']) > 1e-9]
  result = {'frames':len(rows), 'integrity_issues':gaps,
            'different_positive_ceiling_frames':len(varying),
            'max_ceiling_difference':max((abs(r['comparison']['30']['applied_ceiling']-r['comparison']['40']['applied_ceiling']) for r in varying),default=0.),
            'classification':'unknown' if gaps else 'different_ceiling_not_yet_proven_command_effect' if varying else 'same_ceiling_on_recorded_inputs',
            'placebo_determined':False, 'native_comparisons':[]}
  if native:
    from tools.replay_lx3_longitudinal_problem import compare
    for row in varying[:16]:
      result['native_comparisons'].append({'sequence':row['sequence'], **compare(row)})
  return result


def collect(inputs, schema_dir):
  streams = defaultdict(lambda:defaultdict(list))
  frames, videos, indexes, offsets, hashes, errors = [], {}, defaultdict(list), {}, {}, []
  paths = sorted({p for root in inputs for p in root.rglob('*') if p.is_file()})
  for path in paths:
    if path.name.endswith('.jsonl.gz'):
      try:
        # Standard phone/PC manifests are mandatory if present. Bulk collectors
        # may instead provide manifest.json; all actual bytes are hashed below.
        manifest = path.with_name(path.name.split('.')[0]+'.manifest.json')
        if manifest.exists():
          m = json.loads(manifest.read_text(encoding='utf-8'))
          if m['sha256'] != sha(path) or m['bytes'] != path.stat().st_size:
            raise ValueError('chunk manifest mismatch')
        rows = [json.loads(line) for line in gzip.open(path,'rt',encoding='utf-8')]
        if not rows or rows[0].get('kind') != 'header':
          raise ValueError('missing header')
        header = rows[0]
        route = header.get('route')
        if route and isinstance(header.get('native_clock_offset_ns'),int):
          value=header['native_clock_offset_ns']
          previous=offsets.get(route,value)
          offsets[route]=value if previous is not None and abs(previous-value)<=2_000_000 else None
        hashes[str(path)] = sha(path)
        for row in rows[1:]:
          f = unwrap(row)
          if f:
            frames.append({**f, 'route':route, 'boot_id':header.get('boot_id')})
          if row.get('kind') == 'sample' and route:
            for k,v in row.get('services',{}).items():
              streams[route][k].append({'t':v.get('mono_ns',row['mono_ns']), 'valid':v.get('valid'), **v.get('data',{})})
          if row.get('kind') == 'event' and route:
            streams[route]['events'].append({'t':row.get('mono_ns',0), **row})
      except (OSError,ValueError,KeyError,EOFError) as exc:
        errors.append({'path':str(path),'error':str(exc)[:180]})
    if path.name == 'qcamera.ts':
      try:
        route,seg = path.parent.name.rsplit('--',1)
        videos[route,int(seg)] = path
      except ValueError:
        pass
  rlogs = [p for p in paths if p.name in ('rlog.zst','rlog.bz2')]
  if rlogs:
    import capnp
    import zstandard
    capnp.remove_import_hook()
    event_type = capnp.load(str(schema_dir/'log.capnp'),imports=[str(schema_dir)]).Event
    for path in rlogs:
      route,seg = path.parent.name.rsplit('--',1)
      try:
        raw = path.read_bytes()
        hashes[str(path)] = hashlib.sha256(raw).hexdigest()
        if path.suffix == '.zst':
          with zstandard.ZstdDecompressor().stream_reader(raw) as z:
            raw = z.read()
        else:
          import bz2
          raw = bz2.decompress(raw)
        for e in event_type.read_multiple_bytes(raw,traversal_limit_in_words=2**29):
          k,t = e.which(),int(e.logMonoTime)
          row = {'t':t,'valid':bool(e.valid)}
          if k == 'carState':
            c=e.carState
            row.update({name:getattr(c,name) for name in ('vEgo','aEgo','brakePressed','gasPressed','steeringPressed','standstill','leftBlinker','rightBlinker','canValid','canTimeout')})
            row['cruiseState']=c.cruiseState.to_dict()
          elif k == 'carControl':
            c=e.carControl
            row.update(latActive=bool(c.latActive),longActive=bool(c.longActive),accel=float(c.actuators.accel))
          elif k == 'radarState':
            row.update(leadOne=e.radarState.leadOne.to_dict(),leadTwo=e.radarState.leadTwo.to_dict())
          elif k == 'longitudinalPlan':
            p=e.longitudinalPlan
            row.update(shouldStop=bool(p.shouldStop),aTarget=float(p.aTarget),source=str(p.longitudinalPlanSource))
          elif k == 'controlsState':
            row.update(longControlState=str(e.controlsState.longControlState),forceDecel=bool(e.controlsState.forceDecel))
          elif k == 'selfdriveState':
            p=e.selfdriveState
            row.update(alertText1=str(p.alertText1),alertText2=str(p.alertText2),alertType=str(p.alertType))
          elif k == 'modelV2':
            p=e.modelV2
            row.update(frameId=int(p.frameId),timestampEof=int(p.timestampEof))
            center=None
            if len(p.laneLines)>=3 and len(p.laneLineProbs)>=3 and min(p.laneLineProbs[1],p.laneLineProbs[2])>=.8:
              left,right=p.laneLines[1],p.laneLines[2]
              if len(left.x) and len(right.x) and abs(left.x[0])<.1 and abs(right.x[0])<.1:
                if 2.5<=abs(right.y[0]-left.y[0])<=4.5:
                  center=(left.y[0]+right.y[0])/2
            row['lane_center_y_m']=center
          elif k == 'qRoadEncodeIdx':
            p=e.qRoadEncodeIdx
            indexes[route,int(p.segmentNum)].append({'native_sof':int(p.timestampSof),'native_eof':int(p.timestampEof),'received_t':t,'frameId':int(p.frameId),'segmentId':int(p.segmentId)})
            continue
          elif k == 'logMessage':
            text=str(e.logMessage)
            if KEY in text:
              f=unwrap(json.loads(text))
              if f:
                frames.append({**f,'route':route})
            continue
          else:
            continue
          streams[route][k].append(row)
        print(json.dumps({'parsed':path.parent.name}),flush=True)
      except (OSError,ValueError,KeyError,RuntimeError) as exc:
        errors.append({'path':str(path),'error':str(exc)[:180]})
  for route,ss in streams.items():
    for k,rows in ss.items():
      ss[k]=sorted({r['t']:r for r in rows}.values(),key=lambda r:r['t'])
  frames=sorted({(r['producer'],r['sequence']):r for r in frames}.values(),key=lambda r:(r.get('route') or '',r['mono_ns']))
  for row in frames:
    if isinstance(row.get('native_clock_offset_ns'),int) and row.get('route'):
      route=row['route'];value=row['native_clock_offset_ns'];previous=offsets.get(route,value)
      offsets[route]=value if previous is not None and abs(previous-value)<=2_000_000 else None
  return streams,frames,videos,indexes,offsets,hashes,errors


def evaluate_route(route, ss, frames, native=False):
  times={k:[r['t'] for r in rows] for k,rows in ss.items()}
  def at(k,t):
    i=bisect.bisect_right(times.get(k,[]),t)-1
    r=ss[k][i] if i>=0 else None
    return r if r and r.get('valid',True) and 0<=t-r['t']<=300_000_000 else None
  cs=ss.get('carState',[])
  events,jerks,headways,centers,plot=[],[],[],[],[]
  bins=defaultdict(list)
  for c in cs:
    cc=at('carControl',c['t'])
    if c.get('canValid') and not c.get('canTimeout') and c.get('valid') and cc and cc.get('longActive') and not c.get('brakePressed') and not c.get('gasPressed'):
      bins[c['t']//100_000_000].append(c)
  previous=None
  for key,rows in sorted(bins.items()):
    c=min(rows,key=lambda r:abs(r['t']-key*100_000_000))
    t=c['t'];a=statistics.median(r['aEgo'] for r in rows);v=c['vEgo']
    rd=at('radarState',t);lead=(rd or {}).get('leadOne',{})
    gap=lead.get('dRel') if lead.get('status') else None
    if v>=2 and gap is not None:
      headways.append(gap/v)
    jerk=None
    if previous and .075<=(t-previous[0])/1e9<=.2:
      jerk=(a-previous[1])/((t-previous[0])/1e9)
      jerks.append(abs(jerk))
      if abs(jerk)>2.5 and (not events or t-events[-1]['t']>2e9):
        events.append({'route':route,'t':t,'kind':'가감속 변화 검토','value':round(jerk,3),'status':'heuristic_candidate'})
    previous=t,a
    plot.append({'t':t,'speed':v*3.6,'accel':a,'gap':gap,'jerk':jerk})
  lane_last=-1e20
  for m in ss.get('modelV2',[]):
    cc=at('carControl',m['t']);c=at('carState',m['t'])
    y=m.get('lane_center_y_m')
    if (y is not None and cc and cc.get('latActive') and c and c.get('canValid') and not c.get('steeringPressed')
        and not c.get('leftBlinker') and not c.get('rightBlinker')):
      centers.append(abs(y))
      if abs(y)>.35 and m['t']-lane_last>5e9:
        lane_last=m['t'];events.append({'route':route,'t':m['t'],'kind':'차선 중앙 추정 편차 검토','value':round(y,3),'status':'model_estimate_not_ground_truth'})
  starts=[];stopped=None;last_stop=None
  for c in cs:
    cc=at('carControl',c['t'])
    if not (cc and cc.get('longActive') and c.get('canValid') and not c.get('gasPressed') and not c.get('brakePressed')):
      stopped=last_stop=None;continue
    if c['vEgo']<=.15:
      stopped=c['t'] if stopped is None else stopped;last_stop=c['t']
    elif c['vEgo']>.5 and stopped is not None:
      if last_stop-stopped>=1e9:
        rd=at('radarState',c['t']);lead=(rd or {}).get('leadOne',{});track=lead.get('radarTrackId')
        leadrows=[r for r in ss.get('radarState',[]) if stopped<=r['t']<=c['t'] and r['leadOne'].get('status') and r['leadOne'].get('radarTrackId')==track]
        lt=next((r['t'] for r in leadrows if r['leadOne'].get('vLead',0)>.3),None)
        end=next((r['t'] for r in cs if c['t']<=r['t']<=c['t']+8e9 and r['vEgo']>=10/3.6),None)
        lo=lt if lt else c['t']
        window=[r for r in frames if lo-500_000_000<=r['mono_ns']<=(end if end else c['t']+3e9)]
        reasons=[]
        interval=[r for r in cs if lo<=r['t']<=(end or c['t']+3e9)]
        if any(r.get('gasPressed') or r.get('brakePressed') for r in interval):reasons.append('pedal_intervention')
        if any(not r['leadOne'].get('status') or r['leadOne'].get('radarTrackId')!=track for r in ss.get('radarState',[]) if lo<=r['t']<=(end or c['t'])):reasons.append('lead_identity_changed')
        if lt is None or end is None:reasons.append('incomplete_launch_window')
        if any(not r.get('longActive') for r in ss.get('carControl',[]) if lo<=r['t']<=(end or c['t'])):reasons.append('control_interrupted')
        pair=summarize_frames(window,native)
        if (not window or window[0]['mono_ns']>lo-250_000_000
            or window[-1]['mono_ns']<(end or c['t']+3e9)-100_000_000):
          reasons.append('incomplete_detailed_window')
        pair['integrity_issues']=sorted(set(pair['integrity_issues']+reasons))
        if pair['integrity_issues']:pair['classification']='unknown'
        release=next((r['t'] for r in ss.get('longitudinalPlan',[]) if lo<=r['t']<=c['t'] and not r.get('shouldStop',True)),None)
        command=next((r['t'] for r in ss.get('carControl',[]) if lo<=r['t']<=c['t'] and r.get('accel',r.get('actuators',{}).get('accel',0))>.1),None)
        starts.append({'lead_detection_ns':lt,'planner_release_ns':release,'positive_command_ns':command,'ego_moving_ns':c['t'],'ego10_ns':end,
                       'gap_at_move':lead.get('dRel'),'comparison':pair})
        events.append({'route':route,'t':lo,'kind':'재출발 단계 검토','value':round((c['t']-lt)/1e9,3) if lt else None,'status':pair['classification']})
      stopped=last_stop=None
  last=None
  for r in ss.get('selfdriveState',[]):
    text=(r.get('alertText1'),r.get('alertText2'),r.get('alertType'))
    if text!=last and any(text[:2]):
      events.append({'route':route,'t':r['t'],'kind':'오파 경고','value':' / '.join(x for x in text[:2] if x),'status':'host_alert_not_dashboard_camera'})
    last=text
  return {'route':route,'metrics':{'jerk_abs_p95_mps3':percentile(jerks,.95),'headway_p10_s':percentile(headways,.1),
      'model_center_abs_p95_m':percentile(centers,.95),'jerk_samples':len(jerks),'center_samples':len(centers)},
      'launches':starts,'events':sorted(events,key=lambda x:x['t']),'plot':plot}


def media_prepare(videos,indexes,offsets,out):
  media={};errors=[]
  (out/'media').mkdir(exist_ok=True)
  for (route,segment),path in sorted(videos.items()):
    target=out/'media'/(hashlib.sha256(route.encode()).hexdigest()[:12]+'-'+str(segment)+'.mp4')
    try:
      subprocess.run(['ffmpeg','-nostdin','-v','error','-y','-i',str(path),'-an','-c:v','copy','-movflags','+faststart',str(target)],check=True,capture_output=True,timeout=60)
      p=subprocess.run(['ffprobe','-v','error','-select_streams','v:0','-show_entries','frame=best_effort_timestamp_time','-of','json',str(target)],check=True,capture_output=True,text=True,timeout=60)
      pts=[float(x['best_effort_timestamp_time']) for x in json.loads(p.stdout)['frames']]
      idx=sorted({x['segmentId']:x for x in indexes.get((route,segment),[])}.values(),key=lambda x:x['segmentId'])
      if not idx or len(idx)!=len(pts) or [x['segmentId'] for x in idx]!=list(range(len(pts))):
        raise ValueError('encoder index/video frame coverage mismatch')
      if offsets.get(route) is None:
        raise ValueError('missing camera BOOTTIME to host MONOTONIC mapping')
      media[route,segment]=[{'t':x['native_sof']-offsets[route],'pts':pts[i],'file':target.relative_to(out).as_posix(),'frameId':x['frameId']} for i,x in enumerate(idx)]
    except (OSError,ValueError,KeyError,subprocess.SubprocessError) as exc:
      errors.append({'video':str(path),'error':str(exc)[:200]})
  return media,errors


def match_video(event, media):
  rows=[r for (route,_),rr in media.items() if route==event['route'] for r in rr]
  if not rows:return None
  row=min(rows,key=lambda r:abs(r['t']-event['t']))
  error=abs(row['t']-event['t'])/1e6
  return {**row,'match_error_ms':error} if error<=100 else None


def render(result,out):
  data=json.dumps(result,ensure_ascii=False,allow_nan=False).replace('<','\\u003c')
  page='''<!doctype html><html lang="ko"><meta charset="utf-8"><title>LX3 주행 품질 검토</title>
<style>body{background:#101820;color:#e7edf1;font:16px system-ui;margin:24px}video{width:100%;max-height:55vh;background:black}button,select{font:inherit;padding:8px;margin:4px}table{border-collapse:collapse;width:100%}td,th{padding:8px;border-bottom:1px solid #456;text-align:left}canvas{width:100%;height:180px;background:#192b38}small{color:#b8cad5}.grid{display:grid;grid-template-columns:1.4fr 1fr;gap:20px}@media(max-width:900px){.grid{display:block}}#events{max-height:62vh;overflow:auto}</style>
<h1>LX3 주행 품질 검토</h1><p>로그 수치와 영상을 함께 확인합니다. 자동 점수·설정 변경은 하지 않습니다.</p>
<div id="summary"></div><div class="grid"><div><video id="video" controls></video><p id="context"></p><canvas id="chart" width="1000" height="180"></canvas>
<small>선: 속도(파랑), 가속도(주황). 각 축은 보기용 정규화이며 수치는 선택 항목에 표시됩니다.</small><p><button onclick="rate('편안함')">편안함</button><button onclick="rate('불편함')">불편함</button><button onclick="rate('운전자 개입')">운전자 개입</button><button onclick="save()">평가 저장</button></p></div><div id="events"></div></div>
<p>차선 편차는 모델 추정이며 실제 차선 위치의 정답이 아닙니다. 가속도 미분은 센서 잡음의 영향을 받습니다. 설정별 상한 차이는 실제 명령 차이나 플라시보의 증명이 아닙니다. 영상 연결이 없거나 기록이 끊긴 항목은 미확인으로 남깁니다.</p>
<script>const report=__DATA__;let selected=null;const ratings=[];const video=document.getElementById('video');
const events=report.routes.flatMap(r=>r.events).sort((a,b)=>a.route.localeCompare(b.route)||a.t-b.t);
document.getElementById('summary').textContent=report.routes.map(r=>`${r.route}: 재출발 ${r.launches.length}회, |jerk|95% ${r.metrics.jerk_abs_p95_mps3?.toFixed(2)??'미확인'}, 중앙편차 추정95% ${r.metrics.model_center_abs_p95_m?.toFixed(2)??'미확인'}m`).join(' / ');
const box=document.getElementById('events');events.forEach((e,i)=>{const b=document.createElement('button');b.textContent=`${i+1}. ${e.kind} ${e.value??''} ${e.video?'▶':'영상 미연결'}`;b.onclick=()=>choose(i);box.append(b,document.createElement('br'))});
function choose(i){selected=i;const e=events[i];document.getElementById('context').textContent=`${e.kind} / ${e.status} / ${e.value??''}${e.video?' / 영상 오차 '+e.video.match_error_ms.toFixed(1)+'ms':''}`;
if(e.video){if(video.getAttribute('src')!==e.video.file){video.src=e.video.file;video.onloadedmetadata=()=>video.currentTime=e.video.pts}else video.currentTime=e.video.pts}draw(e)}
function draw(e){const c=document.getElementById('chart'),x=c.getContext('2d');x.clearRect(0,0,c.width,c.height);const rows=report.routes.find(r=>r.route===e.route).plot.filter(r=>Math.abs(r.t-e.t)<8e9);for(const [field,color,scale,base] of [['speed','#5dcfff',100,150],['accel','#ffbd69',5,90]]){x.strokeStyle=color;x.beginPath();rows.forEach((r,i)=>{const xx=500+(r.t-e.t)/8e9*500,yy=base-r[field]/scale*100;i?x.lineTo(xx,yy):x.moveTo(xx,yy)});x.stroke()}x.strokeStyle='#fff';x.beginPath();x.moveTo(500,0);x.lineTo(500,180);x.stroke()}
function rate(value){if(selected===null)return;ratings.push({route:events[selected].route,mono_ns:events[selected].t,rating:value});document.getElementById('context').textContent+=' / 평가: '+value}
function save(){const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([JSON.stringify({schema:1,ratings},null,2)],{type:'application/json'}));a.download='drive_feedback.json';a.click();URL.revokeObjectURL(a.href)}
if(events.length)choose(0);</script></html>'''.replace('__DATA__',data)
  (out/'index.html').write_text(page,encoding='utf-8')


def main():
  p=argparse.ArgumentParser(description=__doc__)
  p.add_argument('--input',type=Path,action='append',required=True)
  p.add_argument('--out',type=Path,required=True)
  p.add_argument('--schema-dir',type=Path,default=ROOT/'cereal')
  p.add_argument('--video',action='store_true')
  p.add_argument('--native-pair',action='store_true')
  args=p.parse_args();args.out.mkdir(parents=True,exist_ok=True)
  streams,frames,videos,indexes,offsets,hashes,errors=collect(args.input,args.schema_dir)
  routes=[evaluate_route(k,v,[r for r in frames if r.get('route')==k],args.native_pair) for k,v in streams.items()]
  media,video_errors=media_prepare(videos,indexes,offsets,args.out) if args.video else ({},[])
  for r in routes:
    for event in r['events']:event['video']=match_video(event,media)
  result={'schema':1,'routes':routes,'source_sha256':hashes,'read_errors':errors,'video_errors':video_errors,
          'detailed_evidence_frames':len(frames),'video_segments_matched':len(media),
          'evaluation_scope':'observational_metrics_and_video_review_not_ground_truth_or_placebo_detector',
          'control_changes':False,'raw_data_uploaded':False}
  (args.out/'report.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
  render(result,args.out)
  print(json.dumps({'report':str(args.out/'index.html'),'routes':len(routes),'frames':len(frames),'videos':len(media),'read_errors':len(errors),'video_errors':len(video_errors)}),flush=True)


if __name__=='__main__':main()
