"""Local-volume ownership and checkpoint compatibility guards."""
import fcntl
import json
from pathlib import Path


def claim(root, stage, configuration):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    locks = []
    try:
        for name in (['ingest','events','windows','anomalies'] if stage == 'all' else [stage]):
            handle = (root / f'.{name}.lock').open('a')
            locks.append(handle)
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        path = root / 'configuration.json'
        with (root / '.configuration.lock').open('a') as guard:
            fcntl.flock(guard, fcntl.LOCK_EX)
            if path.exists():
                if json.loads(path.read_text()) != configuration:
                    raise ValueError('root configuration differs: use a fresh root for changed source/state settings')
            else:
                import os
                temporary = root / 'configuration.json.tmp'
                with temporary.open('w') as out:
                    json.dump(configuration, out, sort_keys=True)
                    out.flush()
                    os.fsync(out.fileno())
                os.replace(temporary, path)
        return locks
    except BaseException:
        for handle in locks:
            handle.close()
        raise
