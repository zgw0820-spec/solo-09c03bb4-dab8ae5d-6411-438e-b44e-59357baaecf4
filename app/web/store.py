"""Tiny JSON-file state store guarded by a lock (single coordinator process)."""
import json
import os
import threading


class Store:
    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        self.data = {"files": {}, "workers": {}, "jobs": {}, "order": []}
        self._load()

    def _load(self):
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
            for k in self.data:
                if k in data:
                    self.data[k] = data[k]
        except (OSError, ValueError):
            pass

    def save(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f)
        os.replace(tmp, self.path)
