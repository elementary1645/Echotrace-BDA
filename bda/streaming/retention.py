"""Delete only explicitly retired run roots; never prune an active file sink in place."""
import argparse
import shutil
import time
from pathlib import Path


def candidates(parent, keep_days, now=None):
    cutoff = (time.time() if now is None else now) - keep_days * 86400
    return sorted(path for path in Path(parent).iterdir() if path.is_dir() and
                  not path.is_symlink() and (path / '.retired').is_file() and
                  (path / '.retired').stat().st_mtime < cutoff)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--parent',required=True)
    p.add_argument('--keep-days',type=int,default=30)
    p.add_argument('--execute',action='store_true')
    args = p.parse_args()
    if args.keep_days < 1:
        p.error('keep-days must be positive')
    for path in candidates(args.parent,args.keep_days):
        print(('DELETE ' if args.execute else 'WOULD DELETE ') + str(path))
        if args.execute:
            shutil.rmtree(path)


if __name__ == '__main__':
    main()
