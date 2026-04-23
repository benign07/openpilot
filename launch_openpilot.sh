#!/usr/bin/env bash
export API_HOST="https://api.konik.ai/"
export ATHENA_HOST="wss://athena.konik.ai"

# Konik 업로드 활성화
echo -n "2" > /data/params/d/EnableConnect

# 부팅 시 주요 설정값 덤프
DUMP="/data/media/0/params_dump.txt"
echo "=== Params Dump $(date) ===" > $DUMP
for key in HyundaiCameraSCC CanfdHDA2 CanfdDebug SpeedFromPCM AutoCruiseControl EnableCornerRadar CustomSteerMax CustomSteerDeltaUp CustomSteerDeltaDown LongitudinalPersonality IsLdwsCar MaxAngleFrames LfaButtonMode CancelButtonMode PaddleMode; do
  val=$(cat /data/params/d/$key 2>/dev/null)
  echo "$key=$val" >> $DUMP
done

exec ./launch_chffrplus.sh