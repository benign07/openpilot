#!/usr/bin/env python3
"""
Apply carrot user params snapshot to /data/params/d/

Usage:
  ./apply_carrot_user_params.py <json_file>            # apply all params (matching default = skip)
  ./apply_carrot_user_params.py <json_file> --all      # apply all (incl. matching default)
  ./apply_carrot_user_params.py <json_file> --dry-run  # preview only

Designed for cross-device sync (device A → device B).
"""
import json, os, sys, argparse

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('json_file', help='snapshot JSON file path')
    ap.add_argument('--all', action='store_true', help='apply matching-default params too')
    ap.add_argument('--dry-run', action='store_true', help='preview only, do not write')
    ap.add_argument('--params-dir', default='/data/params/d', help='target params dir')
    args = ap.parse_args()

    with open(args.json_file, encoding='utf-8') as f:
        data = json.load(f)

    meta = data.get('capture_metadata', {})
    print(f'Source: device={meta.get("device")} branch={meta.get("branch")} commit={meta.get("commit")}')
    print(f'Captured: {meta.get("captured_at")}')
    print(f'Total: {meta.get("total_params")} | modified: {meta.get("modified_from_default")}')
    print()

    applied = 0
    skipped = 0
    errors = 0
    for name, info in data.get('params', {}).items():
        v = info.get('value')
        d = info.get('default')
        if v is None:
            continue
        is_modified = str(v) != str(d)
        if not args.all and not is_modified:
            skipped += 1
            continue
        target = os.path.join(args.params_dir, name)
        if args.dry_run:
            mark = 'M' if is_modified else '='
            print(f'  [{mark}] {name}: {v}')
            applied += 1
            continue
        try:
            os.makedirs(args.params_dir, exist_ok=True)
            with open(target, 'wb') as f:
                f.write(str(v).encode('utf-8'))
            applied += 1
            mark = 'M' if is_modified else '='
            print(f'  [{mark}] {name}: {v}')
        except Exception as ex:
            errors += 1
            print(f'  [E] {name}: {ex}', file=sys.stderr)

    print()
    print(f'Result: applied={applied} skipped(matching_default)={skipped} errors={errors}')
    if not args.dry_run:
        print('Reboot or manager restart required for effect.')

if __name__ == '__main__':
    main()
