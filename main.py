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
    QSlider, QLabel, QHeaderView, QAbstractItemView, QComboBox, QTextEdit,
    QMessageBox, QDialog
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
        # We read in smaller sub-chunks to smooth out the speed.
        SUB_CHUNK_SIZE = 64 * 1024  # 64 KB

        # We only send signals to the GUI at a fixed interval.
        GUI_UPDATE_INTERVAL_S = 0.2  # 5 updates per second

        for path, start in self.tasks:
            if self.stop_event.is_set():
                break

            try:
                file_size = path.stat().st_size
                if file_size == 0:
                    self.verdict.emit(str(path), True)
                    self.progress.emit(str(path), 100)
                    continue

                read = start
                min_speed_val = float('inf')
                max_wait_val = 0
                completed = False

                last_gui_update_time = time.time()

                with open(path, 'rb') as f:
                    f.seek(read)
                    while read < file_size:
                        if self.stop_event.is_set():
                            break

                        chunk_read_start_time = time.time()
                        bytes_read_in_chunk = 0

                        # This inner loop reads one 'self.chunk_size' in smaller pieces
                        while bytes_read_in_chunk < self.chunk_size and read < file_size:
                            if self.stop_event.is_set():
                                break

                            # Calculate sleep time based on the small sub-chunk size
                            # to maintain the target speed limit.
                            bytes_to_read = min(SUB_CHUNK_SIZE, self.chunk_size - bytes_read_in_chunk, file_size - read)

                            # Target time for this sub-chunk
                            target_time_per_sub_chunk = (bytes_to_read / (1024 * 1024)) / self.speed_limit

                            sub_chunk_t0 = time.time()
                            data = f.read(bytes_to_read)
                            if not data:
                                break  # End of file reached unexpectedly

                            elapsed_for_sub_chunk = time.time() - sub_chunk_t0

                            # Sleep to throttle the speed
                            sleep_duration = max(0, target_time_per_sub_chunk - elapsed_for_sub_chunk)
                            time.sleep(sleep_duration)

                            read += len(data)
                            bytes_read_in_chunk += len(data)

                        if not data:  # Break outer loop if EOF was hit
                            break

                        # Now calculate metrics for the whole chunk that was just processed
                        chunk_total_time = time.time() - chunk_read_start_time
                        if chunk_total_time == 0:
                            speed_val = float('inf')
                        else:
                            speed_val = (bytes_read_in_chunk / (1024 * 1024)) / chunk_total_time

                        # Only perform speed check on full chunks to avoid false negatives at EOF
                        if bytes_read_in_chunk == self.chunk_size and speed_val < self.MIN_SPEED_MB_S:
                            self.verdict.emit(str(path), False)
                            break  # Stop processing this file

                        # Check if it's time to send an update to the GUI
                        current_time = time.time()
                        if current_time - last_gui_update_time > GUI_UPDATE_INTERVAL_S:
                            min_speed_val = min(min_speed_val, speed_val)
                            max_wait_val = max(max_wait_val, sleep_duration)

                            pct = int(read * 100 / file_size)

                            self.current_speed.emit(str(path), speed_val)
                            self.progress.emit(str(path), pct)
                            self.min_speed.emit(str(path), min_speed_val)
                            self.max_wait.emit(str(path), max_wait_val)
                            last_gui_update_time = current_time
                    else:
                        # This 'else' block runs only if the 'while' loop completes
                        # without a 'break'. This means the file was read completely.
                        completed = True

                if completed:
                    # For files that process faster than the GUI update interval, no signals
                    # would have been sent. This block ensures a final, complete update is
                    # always sent upon successful completion.
                    final_speed = speed_val
                    final_min_speed = min(min_speed_val, final_speed)
                    final_max_wait = max(max_wait_val, sleep_duration)

                    self.progress.emit(str(path), 100)
                    self.current_speed.emit(str(path), final_speed)
                    self.min_speed.emit(str(path), final_min_speed)
                    self.max_wait.emit(str(path), final_max_wait)
                    self.verdict.emit(str(path), True)

            except Exception as e:
                print(f"Error processing {path}: {e}")  # Good to log errors
                self.verdict.emit(str(path), False)

        self.finished_all.emit()

class AddFilesDialog(QDialog):
    files_added = Signal(list)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add file paths")
        self.resize(500, 300)
        self.setModal(True)          # makes it a top-level dialog, no transparency

        vbox = QVBoxLayout(self)

        self.text = QTextEdit()
        vbox.addWidget(QLabel("Paste one file path per line:"))
        vbox.addWidget(self.text)

        h = QHBoxLayout()
        btn_add = QPushButton("Add")
        btn_cancel = QPushButton("Cancel")
        btn_add.clicked.connect(self._add)
        btn_cancel.clicked.connect(self.reject)
        h.addWidget(btn_add)
        h.addWidget(btn_cancel)
        vbox.addLayout(h)

    def _add(self):
        raw_lines = self.text.toPlainText().splitlines()
        files = [Path(p.strip()) for p in raw_lines if p.strip()]
        existing = [f for f in files if f.is_file()]
        non_existing = [f for f in files if not f.is_file()]

        if non_existing:
            QMessageBox.warning(
                self,
                "Some paths not found",
                "The following files were not added:\n\n" +
                "\n".join(str(f) for f in non_existing)
            )
        if existing:
            self.files_added.emit(existing)
        self.accept()

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

        # Add file paths manually
        self.btn_add_files = QPushButton("Add files…")
        self.btn_add_files.clicked.connect(self.add_files_manually)
        h1.addWidget(self.btn_add_files)

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

        # Clear button
        self.b_clear = QPushButton("Clear all")
        self.b_clear.clicked.connect(self.clear_all)
        h3.addWidget(self.b_clear)

        self.setCentralWidget(widget)

    def add_files_manually(self):
        dlg = AddFilesDialog(self)
        dlg.files_added.connect(self._insert_files)
        dlg.show()

    def _insert_files(self, files):
        # files is a list of pathlib.Path objects that already exist
        row0 = self.table.rowCount()
        self.table.setRowCount(row0 + len(files))

        for offset, f in enumerate(files):
            row = row0 + offset
            self.data[str(f)] = {
                'size': f.stat().st_size,
                'progress': 0,
                'cur_speed': 0.0,
                'min_speed': None,
                'max_wait': None,
                'verdict': ''
            }
            self.table.setItem(row, 0, QTableWidgetItem(f.name))
            self.table.setItem(row, 1, QTableWidgetItem(self._format_size(f.stat().st_size)))
            self.table.setCellWidget(row, 2, QProgressBar(value=0))
            self.table.setItem(row, 3, QTableWidgetItem("0.00"))
            self.table.setItem(row, 4, QTableWidgetItem(""))
            self.table.setItem(row, 5, QTableWidgetItem(""))
            self.table.setItem(row, 6, QTableWidgetItem(""))

        # update the visible-rows counter and re-apply the current filter
        self.counter_label.setText(f"Entries: {self.table.rowCount()}")
        self.apply_filter(self.filter_combo.currentText())

    def browse_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Folder")
        if folder:
            self.folder = Path(folder)
            self.folder_label.setText(folder)
            self._populate_table()

    def clear_all(self):
        """Stop any running scan and remove every entry."""
        # 1. Stop the worker if it’s running
        if self.worker and self.worker.isRunning():
            self.stop_scan()  # set the stop-event
            self.worker.wait()  # block until thread exits

        # 2. Now it’s safe to wipe everything
        self.data.clear()
        self.table.setRowCount(0)
        self.counter_label.setText("Entries: 0")
        self.folder = None
        self.folder_label.setText("No folder selected")

        # 3. Delete persistent file
        try:
            data_file.unlink(missing_ok=True)
        except Exception as e:
            print("Could not delete data file:", e)

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
        # Get all files recursively from all subfolders
        files = [f for f in self.folder.rglob('*') if f.is_file()]
        self.table.setRowCount(len(files))
        # update counter
        self.counter_label.setText(f"Entries: {len(files)}")
        self.data = {}
        for i, f in enumerate(files):
            name = str(f.relative_to(self.folder))  # Show relative path from root folder
            size = f.stat().st_size
            self.data[str(f)] = {'size': size, 'progress': 0, 'cur_speed': 0.0,
                                 'min_speed': None, 'max_wait': None, 'verdict': ''}
            self.table.setItem(i, 0, QTableWidgetItem(name))
            self.table.setItem(i, 1, QTableWidgetItem(self._format_size(size)))
            self.table.setCellWidget(i, 2, QProgressBar(value=0))
            self.table.setItem(i, 3, QTableWidgetItem("0.00"))
            self.table.setItem(i, 4, QTableWidgetItem(""))
            self.table.setItem(i, 5, QTableWidgetItem(""))
            self.table.setItem(i, 6, QTableWidgetItem(""))

    def start_scan(self):
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

        if not tasks:
            self.status.setText("Nothing to scan")
            self.b_start.setEnabled(True)
            self.b_stop.setEnabled(False)
            return

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
        if fname not in self.data:
            return
        self.data[fname]['progress'] = p

        # Convert to relative path for comparison
        if self.folder:
            rel_path = str(Path(fname).relative_to(self.folder))
        else:
            rel_path = Path(fname).name

        for i in range(self.table.rowCount()):
            if self.table.item(i, 0).text() == rel_path:
                # Only update the progress bar widget, not replace it
                self.table.cellWidget(i, 2).setValue(p)
                break

    def update_current_speed(self, fname, speed):
        if fname not in self.data:
            return
        self.data[fname]['cur_speed'] = speed
        self._set_item(fname, 3, f"{speed:.2f}")

    def update_min_speed(self, fname, s):
        if fname not in self.data:
            return
        self.data[fname]['min_speed'] = s
        self._set_item(fname, 4, f"{s:.2f}")

    def update_max_wait(self, fname, w):
        if fname not in self.data:
            return
        self.data[fname]['max_wait'] = w
        self._set_item(fname, 5, f"{w:.3f}")

    def update_verdict(self, fname, ok):
        if fname not in self.data:
            return
        text = 'OK' if ok else 'BAD'
        self.data[fname]['verdict'] = text
        item = QTableWidgetItem(text)
        item.setBackground(Qt.green if ok else Qt.red)
        self._place_item(fname, 6, item)

    def _set_item(self, fpath, col, val):
        # Convert to relative path for comparison
        if self.folder:
            rel_path = str(Path(fpath).relative_to(self.folder))
        else:
            rel_path = Path(fpath).name

        for i in range(self.table.rowCount()):
            if self.table.item(i, 0).text() == rel_path:
                self.table.setItem(i, col, QTableWidgetItem(val))
                break

    def _place_item(self, fpath, col, item):
        # Convert to relative path for comparison
        if self.folder:
            rel_path = str(Path(fpath).relative_to(self.folder))
        else:
            rel_path = Path(fpath).name

        for i in range(self.table.rowCount()):
            if self.table.item(i, 0).text() == rel_path:
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
        if not data_file.exists():
            return
        try:
            state = json.loads(data_file.read_text())

            # restore speed slider
            self.slider.setValue(state.get('speed_limit', 10))

            # restore last folder if it still exists
            folder = state.get('folder', '')
            if folder and os.path.isdir(folder):
                self.folder = Path(folder)
                self.folder_label.setText(folder)

            # restore *all* valid file entries, no matter where they are
            restored_files = 0
            for fpath_str, stats in state.get('files', {}).items():
                fpath = Path(fpath_str)
                if not fpath.is_file():
                    continue  # skip missing files

                self.data[str(fpath)] = {
                    'size': fpath.stat().st_size,
                    'progress': stats.get('progress', 0),
                    'cur_speed': stats.get('cur_speed', 0.0),
                    'min_speed': stats.get('min_speed'),
                    'max_wait': stats.get('max_wait'),
                    'verdict': stats.get('verdict', '')
                }
                restored_files += 1

            # rebuild the table to show all restored entries
            self._repopulate_table_from_data()
            self.apply_filter(self.filter_combo.currentText())

        except Exception as e:
            print(f"Error loading data: {e}")

    # helper: recreate the table from self.data
    def _repopulate_table_from_data(self):
        self.table.setRowCount(0)
        self.table.setRowCount(len(self.data))
        for row, (fpath_str, stats) in enumerate(self.data.items()):
            fpath = Path(fpath_str)

            # Use relative path if we have a folder context, otherwise use filename
            if self.folder and fpath.is_relative_to(self.folder):
                display_name = str(fpath.relative_to(self.folder))
            else:
                display_name = fpath.name

            self.table.setItem(row, 0, QTableWidgetItem(display_name))
            self.table.setItem(row, 1, QTableWidgetItem(self._format_size(stats['size'])))
            bar = QProgressBar(value=stats['progress'])
            self.table.setCellWidget(row, 2, bar)
            self.table.setItem(row, 3, QTableWidgetItem(f"{stats['cur_speed']:.2f}"))
            self.table.setItem(row, 4,
                               QTableWidgetItem("" if stats['min_speed'] is None else f"{stats['min_speed']:.2f}"))
            self.table.setItem(row, 5,
                               QTableWidgetItem("" if stats['max_wait'] is None else f"{stats['max_wait']:.3f}"))
            verdict = stats.get('verdict', '')
            item = QTableWidgetItem(verdict)
            if verdict == 'OK':
                item.setBackground(Qt.green)
            elif verdict == 'BAD':
                item.setBackground(Qt.red)
            self.table.setItem(row, 6, item)

        self.counter_label.setText(f"Entries: {len(self.data)}")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
