"""Persist each progress event immediately, including offsets and state-store metrics."""
import json
from pathlib import Path
from pyspark.sql.streaming import StreamingQueryListener


class ProgressLog(StreamingQueryListener):
    def __init__(self, root):
        super().__init__()
        from bda.streaming.telemetry import Telemetry
        self.telemetry = Telemetry(root)
        self.path = Path(root) / 'progress.jsonl'

    def onQueryStarted(self, event):
        self.telemetry.started(event.name,event.id)

    def onQueryProgress(self, event):
        with self.path.open('a') as stream:
            stream.write(event.progress.json + '\n')
        self.telemetry.progress(json.loads(event.progress.json))

    def onQueryTerminated(self, event):
        self.telemetry.terminated(event.id,event.exception)
        with self.path.open('a') as stream:
            stream.write(json.dumps({'terminated':str(event.id),'exception':event.exception}) + '\n')

    def onQueryIdle(self, event):
        pass
