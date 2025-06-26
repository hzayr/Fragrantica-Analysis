from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time, subprocess

class Watcher(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path.endswith("fra_perfumes.csv"):
            print("CSV updated, running pipeline...")
            subprocess.run(["python", "pipeline.py"])

observer = Observer()
observer.schedule(Watcher(), path=".", recursive=False)
observer.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()
observer.join()
