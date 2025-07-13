import sys
import os
import json
import time
from pathlib import Path
from threading import Event
from PySide6.QtCore import Qt, QThread, Signal, QTimer
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTableWidget, QTableWidgetItem, QProgressBar, QPushButton, QFileDialog,
    QSlider, QLabel, QHeaderView, QAbstractItemView, QComboBox
)
from PySide6.QtGui import QIcon

# Store data file in current working directory
data_file = Path.cwd() / "file_checker_data.json"
AUTO_SAVE_INTERVAL_MS = 2000  # Save every 2 seconds

def get_running_path(relative_path):
    if '_internal' in os.listdir():
        return os.path.join('_internal', relative_path)
    else:
        return relative_path

class FileReadWorker(QThread):
    progress = Signal(str, int)
    current_speed = Signal(str, float)
    min_speed = Signal(str, float)
    max_wait = Signal(str, float)
    verdict = Signal(str, bool)
    finished_all = Signal()

    # Threshold: fail if chunk speed too low
    MIN_SPEED_MB_S = 0.1  # MB/s

    def __init__(self, tasks, chunk_size_mb, speed_limit_mb_s, stop_event):
        super().__init__()
        self.tasks = tasks
        self.chunk_size = int(chunk_size_mb * 1024 * 1024)
        self.speed_limit = speed_limit_mb_s
        self.stop_event = stop_event

    def run(self):
        for path, start in self.tasks:
            if self.stop_event.is_set():
                break
            try:
                file_size = path.stat().st_size
                read = start
                min_speed_val = float('inf')
                max_wait_val = 0
                completed = False
                with open(path, 'rb') as f:
                    f.seek(read)
                    while read < file_size:
                        if self.stop_event.is_set():
                            break
                        t0 = time.time()
                        data = f.read(self.chunk_size)
                        if not data:
                            break
                        elapsed = time.time() - t0
                        wait = max(0, (len(data)/1024/1024/self.speed_limit) - elapsed)
                        time.sleep(wait)
                        block_time = time.time() - t0
                        speed_val = len(data)/1024/1024/block_time
                        if speed_val < self.MIN_SPEED_MB_S:
                            self.verdict.emit(str(path), False)
                            break
                        self.current_speed.emit(str(path), speed_val)
                        min_speed_val = min(min_speed_val, speed_val)
                        max_wait_val = max(max_wait_val, wait)
                        read += len(data)
                        pct = int(read * 100 / file_size)
                        self.progress.emit(str(path), pct)
                        self.min_speed.emit(str(path), min_speed_val)
                        self.max_wait.emit(str(path), max_wait_val)
                    else:
                        completed = True
                if completed:
                    self.verdict.emit(str(path), True)
            except Exception:
                self.verdict.emit(str(path), False)
        self.finished_all.emit()

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("pyFileIntegrityChecker v" + open(get_running_path('version.txt')).read())
        self.setWindowIcon(QIcon(get_running_path('icon.ico')))
        self.resize(800, 600)
        self.data = {}
        self.folder = None
        self.worker = None
        self.stop_event = Event()

        self._setup_ui()
        self._load_data()

        self.autosave_timer = QTimer(self)
        self.autosave_timer.timeout.connect(self.save_data)
        self.autosave_timer.start(AUTO_SAVE_INTERVAL_MS)

    def _setup_ui(self):
        widget = QWidget()
        vbox = QVBoxLayout(widget)

        # Folder selection
        h1 = QHBoxLayout()
        self.folder_label = QLabel("No folder selected")
        btn_browse = QPushButton("Browse...")
        btn_browse.clicked.connect(self.browse_folder)
        h1.addWidget(self.folder_label)
        h1.addWidget(btn_browse)
        vbox.addLayout(h1)

        # Speed slider
        h2 = QHBoxLayout()
        h2.addWidget(QLabel("Speed (MB/s):"))
        self.slider = QSlider(Qt.Horizontal)
        self.slider.setRange(1, 100)
        self.slider.setValue(10)
        self.slider.valueChanged.connect(self.on_speed_change)
        self.speed_label = QLabel(f"{self.slider.value()} MB/s")
        h2.addWidget(self.slider)
        h2.addWidget(self.speed_label)
        vbox.addLayout(h2)

        # Filter dropdown for verdict
        h_filter = QHBoxLayout()
        h_filter.addWidget(QLabel("Filter:"))
        self.filter_combo = QComboBox()
        self.filter_combo.addItems(["All", "OK", "BAD", "EMPTY"])
        self.filter_combo.currentTextChanged.connect(self.apply_filter)
        h_filter.addWidget(self.filter_combo)
        vbox.addLayout(h_filter)

        # Status label
        self.status = QLabel("Stopped")
        self.status.setAlignment(Qt.AlignCenter)
        vbox.addWidget(self.status)

        # Files table
        self.table = QTableWidget(0, 7)
        headers = ["File", "Size", "Progress", "Cur Speed", "Min Speed", "Max Wait", "Verdict"]
        self.table.setHorizontalHeaderLabels(headers)
        # Light blue selection
        self.table.setStyleSheet("QTableWidget::item:selected { background-color: lightblue; color: black; }")
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        hdr = self.table.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.Stretch)
        for col in range(1, 7):
            hdr.setSectionResizeMode(col, QHeaderView.ResizeToContents)
        vbox.addWidget(self.table)

        # Counter label
        self.counter_label = QLabel("Entries: 0")
        vbox.addWidget(self.counter_label)

        # Start/Stop buttons
        h3 = QHBoxLayout()
        self.b_start = QPushButton("Start")
        self.b_start.clicked.connect(self.start_scan)
        self.b_stop = QPushButton("Stop")
        self.b_stop.clicked.connect(self.stop_scan)
        self.b_stop.setEnabled(False)
        h3.addWidget(self.b_start)
        h3.addWidget(self.b_stop)
        vbox.addLayout(h3)

        self.setCentralWidget(widget)

    def browse_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Folder")
        if folder:
            self.folder = Path(folder)
            self.folder_label.setText(folder)
            self._populate_table()

    def on_speed_change(self, value):
        self.speed_label.setText(f"{value} MB/s")
        if self.worker:
            self.worker.speed_limit = value

    def apply_filter(self, text):
        for i in range(self.table.rowCount()):
            item = self.table.item(i, 6)
            val = item.text() if item else ""
            if text == "All":
                self.table.setRowHidden(i, False)
            elif text == "EMPTY":
                self.table.setRowHidden(i, bool(val))
            else:
                self.table.setRowHidden(i, val != text)
        # update counter to reflect visible rows
        visible = sum(not self.table.isRowHidden(i) for i in range(self.table.rowCount()))
        self.counter_label.setText(f"Entries: {visible}")

    def _format_size(self, b):
        return f"{b/1024**3:.2f} GB" if b >= 1024**3 else f"{b/1024**2:.2f} MB"

    def _populate_table(self):
        files = [f for f in self.folder.iterdir() if f.is_file()]
        self.table.setRowCount(len(files))
        # update counter
        self.counter_label.setText(f"Entries: {len(files)}")
        self.data = {}
        for i, f in enumerate(files):
            name = f.name
            size = f.stat().st_size
            self.data[str(f)] = {'size': size, 'progress': 0, 'cur_speed': 0.0,
                                  'min_speed': None, 'max_wait': None, 'verdict': ''}
            self.table.setItem(i, 0, QTableWidgetItem(name))
            self.table.setItem(i, 1, QTableWidgetItem(self._format_size(size)))
            self.table.setCellWidget(i, 2, QProgressBar())
            self.table.setItem(i, 3, QTableWidgetItem("0.00"))
            self.table.setItem(i, 4, QTableWidgetItem(""))
            self.table.setItem(i, 5, QTableWidgetItem(""))
            self.table.setItem(i, 6, QTableWidgetItem(""))

    def start_scan(self):
        if not self.folder:
            return
        self.status.setText("Working")
        self.b_start.setEnabled(False)
        self.b_stop.setEnabled(True)
        self.stop_event.clear()
        tasks = []
        for fpath, stats in self.data.items():
            if stats.get('verdict') == 'OK':
                continue
            size = stats['size']
            start_bytes = int(stats['progress'] / 100 * size)
            tasks.append((Path(fpath), start_bytes))
        self.worker = FileReadWorker(tasks, 1, self.slider.value(), self.stop_event)
        self.worker.progress.connect(self.update_progress)
        self.worker.current_speed.connect(self.update_current_speed)
        self.worker.min_speed.connect(self.update_min_speed)
        self.worker.max_wait.connect(self.update_max_wait)
        self.worker.verdict.connect(self.update_verdict)
        self.worker.finished_all.connect(self.on_scan_finished)
        self.worker.start()

    def stop_scan(self):
        self.stop_event.set()

    def on_scan_finished(self):
        self.status.setText("Stopped")
        self.b_start.setEnabled(True)
        self.b_stop.setEnabled(False)

    def update_progress(self, fname, p):
        self.data[fname]['progress'] = p
        for i in range(self.table.rowCount()):
            if self.table.item(i, 0).text() == Path(fname).name:
                # Only update the progress bar widget, not replace it
                self.table.cellWidget(i, 2).setValue(p)
                break

    def update_current_speed(self, fname, speed):
        self.data[fname]['cur_speed'] = speed
        self._set_item(fname, 3, f"{speed:.2f}")

    def update_min_speed(self, fname, s):
        self.data[fname]['min_speed'] = s
        self._set_item(fname, 4, f"{s:.2f}")

    def update_max_wait(self, fname, w):
        self.data[fname]['max_wait'] = w
        self._set_item(fname, 5, f"{w:.3f}")

    def update_verdict(self, fname, ok):
        text = 'OK' if ok else 'BAD'
        self.data[fname]['verdict'] = text
        item = QTableWidgetItem(text)
        item.setBackground(Qt.green if ok else Qt.red)
        self._place_item(fname, 6, item)

    def _set_item(self, fpath, col, val):
        for i in range(self.table.rowCount()):
            if self.table.item(i, 0).text() == Path(fpath).name:
                self.table.setItem(i, col, QTableWidgetItem(val))
                break

    def _place_item(self, fpath, col, item):
        for i in range(self.table.rowCount()):
            if self.table.item(i, 0).text() == Path(fpath).name:
                self.table.setItem(i, col, item)
                break

    def save_data(self):
        try:
            state = {'folder': str(self.folder), 'speed_limit': self.slider.value(), 'files': self.data}
            with open(data_file, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            print(f"Error saving data: {e}")

    def _load_data(self):
        if data_file.exists():
            try:
                state = json.loads(data_file.read_text())
                folder = state.get('folder', '')
                if folder and os.path.isdir(folder):
                    self.folder = Path(folder)
                    self.folder_label.setText(folder)
                    self._populate_table()
                for fpath, stats in state.get('files', {}).items():
                    if fpath in self.data:
                        self.update_progress(fpath, stats.get('progress', 0))
                        self.update_current_speed(fpath, stats.get('cur_speed', 0.0))
                        if stats.get('min_speed') is not None:
                            self.update_min_speed(fpath, stats['min_speed'])
                        if stats.get('max_wait') is not None:
                            self.update_max_wait(fpath, stats['max_wait'])
                        if stats.get('verdict'):
                            self.update_verdict(fpath, stats['verdict'] == 'OK')
                self.slider.setValue(state.get('speed_limit', 10))
            except Exception as e:
                print(f"Error loading data: {e}")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
