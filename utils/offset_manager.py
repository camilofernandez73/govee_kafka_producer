import sqlite3
import threading
import time
import logging

logging.basicConfig(level=logging.INFO)

class OffsetManager:
    def __init__(self, db_file):
        self.db_file = db_file
        self.offsets = {}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.offset_saver_thread = None
        self.create_offset_table()

    def create_offset_table(self):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS offsets
                          (filename TEXT PRIMARY KEY, offset INTEGER, entry_ts TEXT)''')
        conn.commit()
        conn.close()
        logging.debug("Offset table created")

    def load_offsets(self):
        offsets = {}
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute('SELECT filename, offset, entry_ts FROM offsets')
        rows = cursor.fetchall()
        conn.close()
        for row in rows:
            offsets[row[0]] = {'offset': row[1], 'entry_ts': row[2]}
        logging.debug("Offsets loaded from the database")
        return offsets

    def update_offset(self, filename, offset, entry_ts):
        with self.lock:
            self.offsets[filename] = {'offset': offset, 'entry_ts': entry_ts}
        logging.debug(f"Updated offset for {filename}: {offset}")

    def save_offsets(self):
        with self.lock:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            for filename, offset_data in self.offsets.items():
                cursor.execute('INSERT OR REPLACE INTO offsets (filename, offset, entry_ts) VALUES (?, ?, ?)',
                               (filename, offset_data['offset'], offset_data['entry_ts']))
            conn.commit()
            conn.close()
            logging.debug("Offsets saved to the database")

    def save_offsets_periodically(self, interval):
        while not self.stop_event.is_set():
            logging.debug(f"Saving offsets every {interval} seconds")
            self.save_offsets()
            time.sleep(interval)

    def start_async_save(self, interval):
        if self.offset_saver_thread is None:
            self.stop_event.clear()
            self.offset_saver_thread = threading.Thread(target=self.save_offsets_periodically, args=(interval,))
            self.offset_saver_thread.start()
            logging.debug("Asynchronous offset saving started")

    def stop_async_save(self):
        if self.offset_saver_thread is not None:
            self.stop_event.set()
            self.offset_saver_thread.join()
            self.offset_saver_thread = None
            logging.debug("Asynchronous offset saving stopped")

    def close(self):
        self.stop_async_save()
        pass