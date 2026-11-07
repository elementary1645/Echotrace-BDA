"""Refuse to recreate legacy Kafka containers without persistent broker storage."""
import json
import subprocess

result = subprocess.run(['docker','inspect','echotrace-bda-kafka'],capture_output=True,text=True)
if result.returncode == 0:
    container = json.loads(result.stdout)[0]
    if not any(m['Destination'] == '/var/lib/kafka/data' for m in container['Mounts']):
        raise SystemExit('Legacy Kafka container has no /var/lib/kafka/data mount. Export its records before recreation; see bda/PHASE2.md.')
