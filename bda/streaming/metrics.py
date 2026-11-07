"""Persist each progress event immediately, including offsets and state-store metrics."""
import json
from pathlib import Path
from pyspark.sql.streaming import StreamingQueryListener


class ProgressLog(StreamingQueryListener):
    def __init__(self, root):
        super().__init__()
        self.path = Path(root) / 'progress.jsonl'

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        with self.path.open('a') as stream:
            stream.write(event.progress.json + '\n')

    def onQueryTerminated(self, event):
        with self.path.open('a') as stream:
            stream.write(json.dumps({'terminated':str(event.id),'exception':event.exception}) + '\n')

    def onQueryIdle(self, event):
        pass
