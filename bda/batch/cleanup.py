"""Only remove a Phase 3 artifact subtree after explicit review; dry-run by default."""
import argparse
from pathlib import Path
import shutil


def checked(path):
    root = Path(__file__).resolve().parents[2]/'artifacts'/'phase3'
    candidate = Path(path).absolute()
    if candidate.is_symlink() or root.resolve() not in candidate.resolve().parents:
        raise ValueError('cleanup requires a child of repository artifacts/phase3; root/symlinks refused')
    return candidate


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('path')
    p.add_argument('--execute',action='store_true')
    args = p.parse_args()
    target = checked(args.path)
    print(('DELETE ' if args.execute else 'WOULD DELETE ')+str(target))
    if args.execute:
        shutil.rmtree(target)
