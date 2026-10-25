import sys
import os
import ctypes
import json
import time
from collections import deque
from pathlib import Path
from threading import Event
from PySide6.QtCore import Qt, QThread, Signal, QTimer, QMutex, QMutexLocker
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTableWidget, QTableWidgetItem, QProgressBar, QPushButton, QFileDialog,
    QLabel, QHeaderView, QAbstractItemView, QComboBox, QTextEdit,
    QMessageBox, QDialog, QDoubleSpinBox, QSpinBox
)
from PySide6.QtGui import QIcon

# Store data file in current working directory
data_file = Path.cwd() / "file_checker_data.json"

APP_ID = 'AG95.pyFileIntegrityChecker'
ICON_FILE = 'icon.ico'
AUTO_SAVE_INTERVAL_MS = 2000  # Save every 2 seconds

# Table columns
(COL_FILE, COL_SIZE, COL_PROGRESS, COL_DISK_SPEED, COL_MIN_SPEED,
 COL_MAX_READ, COL_SLOW_EVENTS, COL_VERDICT) = range(8)

# --- Read checking parameters ------------------------------------------------------
# Read a chunk, time the read call, and fail the file if the reading speed stays
# below the accepted minimum, or if a single read takes longer than the drive is
# allowed to take to respond.
BYTES_PER_MB = 1024 * 1024
CHUNK_SIZE_MB = 1          # how much is read per measured chunk
GUI_UPDATE_INTERVAL_S = 0.2   # how often the table is refreshed
SPEED_WINDOW_S = 1.0          # sliding window behind the "Disk MB/s" reading
RESPONSE_WARMUP_MB = 1        # checks only start after this much was read from the file,
                              # so a slow start (spin-up, first sector) never fails a file

# Defaults of the GUI parameters
MIN_CHUNK_SPEED_MB_S = 10.0   # "Fail below (MB/s)"
MAX_SLOW_EVENTS = 50          # "Fail after N slow checks"
MAX_RESPONSE_S = 10.0         # "No response for (s)"

def get_running_path(relative_path):
    """Return a bundled file's path in both source and PyInstaller builds."""
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)


def set_windows_app_id():
    """Give the app its own Windows taskbar identity."""
    if sys.platform != 'win32':
        return
    try:
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(APP_ID)
    except (AttributeError, OSError):
        pass


class SaveWorker(QThread):
    """Thread for saving data to avoid GUI freezes"""
    error_occurred = Signal(str)

    def __init__(self, data_file):
        super().__init__()
        self.data_file = data_file
        self.mutex = QMutex()
        self.pending_data = None
        self.shutdown = False

    def schedule_save(self, data):
        """Schedule data to be saved - thread safe"""
        with QMutexLocker(self.mutex):
            self.pending_data = data
        if not self.isRunning():
            self.start()

    def stop_worker(self):
        """Gracefully stop the worker"""
        self.shutdown = True
        if self.isRunning():
            self.wait(5000)  # Wait up to 5 seconds

    def run(self):
        while not self.shutdown:
            # Get pending data thread-safely
            with QMutexLocker(self.mutex):
                data_to_save = self.pending_data
                self.pending_data = None

            if data_to_save is not None:
                try:
                    # Save to temporary file first, then rename (atomic operation)
                    temp_file = self.data_file.with_suffix('.tmp')
                    with open(temp_file, 'w') as f:
                        json.dump(data_to_save, f, indent=2)

                    # Atomic replace
                    if os.name == 'nt':  # Windows
                        # On Windows, we can't atomically replace, so we'll just overwrite
                        if self.data_file.exists():
                            self.data_file.unlink()
                        temp_file.rename(self.data_file)
                    else:  # Unix-like systems
                        temp_file.replace(self.data_file)

                except Exception as e:
                    self.error_occurred.emit(f"Error saving data: {e}")

            # Sleep a bit to prevent busy waiting
            self.msleep(100)

            # If no more data and shutdown requested, exit
            with QMutexLocker(self.mutex):
                if self.shutdown and self.pending_data is None:
                    break


class FileReadWorker(QThread):
    progress = Signal(str, int)
    disk_speed = Signal(str, float)   # throughput of the last SPEED_WINDOW_S seconds
    min_speed = Signal(str, float)    # lowest windowed throughput seen
    max_read = Signal(str, float)     # longest single read (ms)
    slow_events = Signal(str, int)  # checks that were below the minimum speed
    verdict = Signal(str, bool, str)     # path, ok, human readable reason
    finished_all = Signal()

    def __init__(self, tasks, chunk_size_mb, stop_event, min_chunk_speed_mb_s=MIN_CHUNK_SPEED_MB_S,
                 max_slow_events=MAX_SLOW_EVENTS, max_response_s=MAX_RESPONSE_S):
        super().__init__()
        self.tasks = tasks
        self.chunk_size_mb = chunk_size_mb
        self.stop_event = stop_event
        # The 1-second reading speed must stay above this.
        self.min_chunk_speed_mb_s = min_chunk_speed_mb_s
        # ... this many checks in a row below it => BAD (drives may retry now and then).
        self.max_slow_events = max_slow_events
        # A single read slower than this => BAD (the drive is not responding).
        self.max_response_s = max_response_s

    def run(self):
        chunk_bytes = int(self.chunk_size_mb * BYTES_PER_MB)
        warmup_bytes = int(RESPONSE_WARMUP_MB * BYTES_PER_MB)

        for path, start in self.tasks:
            if self.stop_event.is_set():
                break

            try:
                file_size = path.stat().st_size
                if file_size == 0:
                    self.verdict.emit(str(path), True, "empty file")
                    self.progress.emit(str(path), 100)
                    continue

                read_total = start
                completed = False
                slow_event_count = 0
                max_read_s = 0.0
                min_window_mb_s = None

                # Sliding window of the last SPEED_WINDOW_S seconds of reading,
                # so a dip shows up immediately instead of being averaged away.
                window = deque()
                window_time = 0.0
                window_bytes = 0

                start_time = time.perf_counter()
                last_update = start_time
                buffer = bytearray(chunk_bytes)
                view = memoryview(buffer)

                # buffering=0 -> raw reads, no hidden readahead copy, so only the
                # drive itself is inside the measured read duration.
                with open(path, 'rb', buffering=0) as f:
                    f.seek(read_total)

                    while read_total < file_size:
                        if self.stop_event.is_set():
                            break

                        # Bytes already read from this file before this chunk.
                        read_before = read_total - start
                        want = min(chunk_bytes, file_size - read_total)

                        # 1. Read a chunk and time the read call.
                        read_t0 = time.perf_counter()
                        filled = 0
                        while filled < want:
                            got = f.readinto(view[filled:want])
                            if not got:
                                break
                            filled += got
                        read_s = time.perf_counter() - read_t0

                        if filled <= 0:
                            break

                        read_total += filled
                        max_read_s = max(max_read_s, read_s)

                        window.append((read_s, filled))
                        window_time += read_s
                        window_bytes += filled
                        while len(window) > 1 and window_time - window[0][0] >= SPEED_WINDOW_S:
                            dropped_time, dropped_bytes = window.popleft()
                            window_time -= dropped_time
                            window_bytes -= dropped_bytes

                        window_mb_s = (window_bytes / BYTES_PER_MB) / window_time if window_time > 0 else 0.0
                        if min_window_mb_s is None or window_mb_s < min_window_mb_s:
                            min_window_mb_s = window_mb_s

                        # 2. Verdict, judged on the same 1-second throughput that the
                        #    table shows, so "Min MB/s" below the limit means BAD.
                        #    Slow checks are counted cumulatively, not in a row: one
                        #    retry poisons the 1 s window for a whole second, so
                        #    "in a row" would mean something quite different.
                        #    Checks only start after the warm-up, so a slow start
                        #    (spin-up, first sector) never fails a file.
                        failed = False
                        reason = ""
                        if read_before >= warmup_bytes:
                            if read_s > self.max_response_s:
                                failed = True          # the drive stopped responding
                                reason = (f"no response: a {self.chunk_size_mb:g} MB read took "
                                          f"{read_s:.1f} s (limit {self.max_response_s:g} s)")
                            elif window_mb_s < self.min_chunk_speed_mb_s:
                                slow_event_count += 1
                                failed = slow_event_count >= self.max_slow_events
                                reason = (f"{slow_event_count} slow checks below "
                                          f"{self.min_chunk_speed_mb_s:g} MB/s "
                                          f"(last {window_mb_s:.2f} MB/s)")

                        if failed:
                            self.progress.emit(str(path), int(read_total * 100 / file_size))
                            self.disk_speed.emit(str(path), window_mb_s)
                            self.min_speed.emit(str(path), min_window_mb_s or 0.0)
                            self.max_read.emit(str(path), max_read_s * 1000.0)
                            self.slow_events.emit(str(path), slow_event_count)
                            self.verdict.emit(str(path), False, reason)
                            break

                        # 3. Update GUI stats
                        now = time.perf_counter()
                        if now - last_update >= GUI_UPDATE_INTERVAL_S:
                            last_update = now

                            self.progress.emit(str(path), int(read_total * 100 / file_size))
                            self.disk_speed.emit(str(path), window_mb_s)
                            self.min_speed.emit(str(path), min_window_mb_s)
                            self.max_read.emit(str(path), max_read_s * 1000.0)
                            self.slow_events.emit(str(path), slow_event_count)

                        if filled < want:
                            completed = read_total >= file_size
                            break
                    else:
                        completed = True

                if completed:
                    reason = (f"read {read_total / BYTES_PER_MB:.0f} MB, "
                              f"{slow_event_count} slow checks, longest read {max_read_s * 1000:.0f} ms")
                    self.progress.emit(str(path), 100)
                    self.disk_speed.emit(str(path), window_mb_s)
                    self.min_speed.emit(str(path), min_window_mb_s or 0.0)
                    self.max_read.emit(str(path), max_read_s * 1000.0)
                    self.slow_events.emit(str(path), slow_event_count)
                    self.verdict.emit(str(path), True, reason)

            except Exception as e:
                print(f"Error processing {path}: {e}")
                self.verdict.emit(str(path), False, f"read error: {e}")

        self.finished_all.emit()

class AddFilesDialog(QDialog):
    files_added = Signal(list)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add file paths")
        self.resize(500, 300)
        self.setModal(True)  # makes it a top-level dialog, no transparency

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
        self.setWindowIcon(QIcon(get_running_path(ICON_FILE)))
        self.resize(800, 600)
        self.data = {}
        self.folder = None
        self.worker = None
        self.stop_event = Event()

        self._setup_ui()
        self._load_data()

        self._start_save_worker()

        # Timer to trigger saves
        self.autosave_timer = QTimer(self)
        self.autosave_timer.timeout.connect(self._schedule_save)
        self.autosave_timer.start(AUTO_SAVE_INTERVAL_MS)

    def _start_save_worker(self):
        self.save_worker = SaveWorker(data_file)
        self.save_worker.error_occurred.connect(self._on_save_error)

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

        # Minimum acceptable read speed - the one parameter that decides the verdict
        h2 = QHBoxLayout()
        h2.addWidget(QLabel("Fail below (MB/s):"))
        self.min_speed_box = QDoubleSpinBox()
        self.min_speed_box.setRange(0.01, 500.0)
        self.min_speed_box.setDecimals(2)
        self.min_speed_box.setSingleStep(0.5)
        self.min_speed_box.setValue(MIN_CHUNK_SPEED_MB_S)
        self.min_speed_box.setSuffix(" MB/s")
        self.min_speed_box.setToolTip("A file whose reading speed stays below this is marked BAD, "
                                      "but only after the consecutive checks below are reached.")
        self.min_speed_box.valueChanged.connect(self.on_min_speed_change)
        h2.addWidget(self.min_speed_box)
        self.min_speed_label = QLabel()
        h2.addWidget(self.min_speed_label)
        h2.addStretch()
        vbox.addLayout(h2)

        # Tolerances: retries are fine, a drive that stops responding is not
        h2b = QHBoxLayout()
        h2b.addWidget(QLabel("Fail after N slow checks:"))
        self.slow_checks_box = QSpinBox()
        self.slow_checks_box.setRange(1, 1000000)
        self.slow_checks_box.setValue(MAX_SLOW_EVENTS)
        self.slow_checks_box.setToolTip("Total number of slow checks allowed per file. A drive may "
                                         "retry now and then, so this has to be reached before a file "
                                         "is marked BAD.")
        self.slow_checks_box.valueChanged.connect(self.on_slow_checks_change)
        h2b.addWidget(self.slow_checks_box)
        self.slow_checks_label = QLabel()
        h2b.addWidget(self.slow_checks_label)
        h2b.addStretch()
        vbox.addLayout(h2b)

        h2c = QHBoxLayout()
        h2c.addWidget(QLabel("No response for (s):"))
        self.response_box = QDoubleSpinBox()
        self.response_box.setRange(0.1, 3600.0)
        self.response_box.setDecimals(1)
        self.response_box.setValue(MAX_RESPONSE_S)
        self.response_box.setSuffix(" s")
        self.response_box.setToolTip("A single read that takes longer than this means the drive is "
                                     "not responding and the file is marked BAD immediately. "
                                     f"Ignored until {RESPONSE_WARMUP_MB:g} MB has been read from the file.")
        self.response_box.valueChanged.connect(self.on_response_change)
        h2c.addWidget(self.response_box)
        self.response_label = QLabel()
        h2c.addWidget(self.response_label)
        h2c.addStretch()
        vbox.addLayout(h2c)
        self.on_slow_checks_change(self.slow_checks_box.value())
        self.on_response_change(self.response_box.value())
        self.on_min_speed_change(self.min_speed_box.value())

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
        self.table = QTableWidget(0, 8)
        headers = ["File", "Size", "Progress", "Disk MB/s", "Min MB/s",
                   "Max Read (ms)", "Slow Events", "Verdict"]
        self.table.setHorizontalHeaderLabels(headers)
        # Light blue selection
        self.table.setStyleSheet("QTableWidget::item:selected { background-color: lightblue; color: black; }")
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        hdr = self.table.horizontalHeader()
        hdr.setSectionResizeMode(COL_FILE, QHeaderView.Stretch)
        for col in range(COL_SIZE, COL_VERDICT + 1):
            hdr.setSectionResizeMode(col, QHeaderView.ResizeToContents)

        tooltips = {
            COL_DISK_SPEED: "Throughput of the last second of reading. Dips when the drive stalls. "
                            "Can be inflated by the OS file cache.",
            COL_MIN_SPEED: "Lowest 1-second throughput seen for this file. Below the "
                          "'Fail below' setting means the file is marked BAD.",
            COL_MAX_READ: "Longest single read call - this is where the drive dropped to zero.",
            COL_SLOW_EVENTS: "Number of checks that were below the 'Fail below' speed. "
                             "'Fail after N slow checks' of them in a row marks the file BAD.",
        }
        for col, tip in tooltips.items():
            self.table.horizontalHeaderItem(col).setToolTip(tip)

        vbox.addWidget(self.table)

        # Counter label and progress bar
        counter_layout = QHBoxLayout()
        self.counter_label = QLabel("Entries: 0")
        counter_layout.addWidget(self.counter_label)

        # Progress bar for overall scan progress
        self.overall_progress_bar = QProgressBar()
        self.overall_progress_bar.setMinimum(0)
        self.overall_progress_bar.setMaximum(100)
        self.overall_progress_bar.setValue(0)
        self.overall_progress_bar.setFormat("%v/%m (%p%)")  # Shows current/max (percentage)
        self.overall_progress_bar.setVisible(True)
        counter_layout.addWidget(self.overall_progress_bar)

        vbox.addLayout(counter_layout)

        # Start/Stop buttons
        h3 = QHBoxLayout()
        self.b_start = QPushButton("Start")
        self.b_start.clicked.connect(self.start_scan) # Connects to default start

        self.b_retry = QPushButton("Retry Failed")
        self.b_retry.clicked.connect(self.retry_failed)

        self.b_stop = QPushButton("Stop")
        self.b_stop.clicked.connect(self.stop_scan)
        self.b_stop.setEnabled(False)

        h3.addWidget(self.b_start)
        h3.addWidget(self.b_retry)
        h3.addWidget(self.b_stop)
        vbox.addLayout(h3)

        # Clear button
        self.b_clear = QPushButton("Clear all")
        self.b_clear.clicked.connect(self.clear_all)
        h3.addWidget(self.b_clear)

        self.setCentralWidget(widget)

    def _update_overall_progress(self):
        """Update the overall progress bar based on completed files"""
        if not self.data:
            self.overall_progress_bar.setValue(0)
            self.overall_progress_bar.setMaximum(0)
            return

        total_files = len(self.data)
        completed_files = 0

        for stats in self.data.values():
            if stats.get('verdict') in ['OK', 'BAD']:
                completed_files += 1

        self.overall_progress_bar.setMaximum(total_files)
        self.overall_progress_bar.setValue(completed_files)

        # Update tooltip with detailed information
        tooltip = f"Completed: {completed_files}/{total_files} files\n"
        tooltip += f"Remaining: {total_files - completed_files} files"
        self.overall_progress_bar.setToolTip(tooltip)

    def closeEvent(self, event):
        """Ensure save worker is properly stopped on application close"""
        self.save_worker.stop_worker()
        event.accept()

    def _schedule_save(self):
        """Schedule a save operation in the background thread"""
        if self.data:
            state = {
                'folder': str(self.folder) if self.folder else '',
                'min_chunk_speed': self.min_speed_box.value(),
                'max_slow_events': self.slow_checks_box.value(),
                'max_response_s': self.response_box.value(),
                'files': self.data
            }
            self.save_worker.schedule_save(state)

    def _on_save_error(self, error_msg):
        """Called when save operation fails"""
        print(error_msg)  # Log the error
        # Optional: Show a non-intrusive message to the user

    def add_files_manually(self):
        dlg = AddFilesDialog(self)
        dlg.files_added.connect(self._insert_files)
        dlg.show()

    def _insert_files(self, files):
        # files is a list of pathlib.Path objects that already exist
        row0 = self.table.rowCount()
        self.table.setRowCount(row0 + len(files))

        for offset, f in enumerate(files):
            self.data[str(f)] = stats = self._new_stats(f.stat().st_size)
            self._fill_row(row0 + offset, f.name, stats)

        # update the visible-rows counter and re-apply the current filter
        self.counter_label.setText(f"Entries: {self.table.rowCount()}")
        self._update_overall_progress()
        self.apply_filter(self.filter_combo.currentText())

    def browse_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Folder")
        if folder:
            self.folder = Path(folder)
            self.folder_label.setText(folder)
            self._populate_table()

    def clear_all(self):
        """Stop any running scan and remove every entry."""
        # 1. Stop the worker if it's running
        if self.worker and self.worker.isRunning():
            self.stop_scan()  # set the stop-event
            self.worker.wait()  # block until thread exits

        # 2. Stop save worker
        self.save_worker.stop_worker()

        # 3. Now recreate the save worker
        self._start_save_worker()

        # 4. Only now it's safe to wipe everything
        self.data.clear()
        self.table.setRowCount(0)
        self.counter_label.setText("Entries: 0")
        self.overall_progress_bar.setValue(0)
        self.overall_progress_bar.setMaximum(0)
        self.folder = None
        self.folder_label.setText("No folder selected")

        # 5. Delete persistent file
        try:
            data_file.unlink(missing_ok=True)
        except Exception as e:
            print("Could not delete data file:", e)

    def on_min_speed_change(self, value):
        self.min_speed_label.setText(
            f"(the {SPEED_WINDOW_S:g} s reading speed must stay above {value:g} MB/s)")
        if self.worker:
            self.worker.min_chunk_speed_mb_s = value

    def on_slow_checks_change(self, value):
        self.slow_checks_label.setText(
            "(slow checks allowed per file before it is marked BAD)")
        if self.worker:
            self.worker.max_slow_events = value

    def on_response_change(self, value):
        self.response_label.setText(
            f"(ignored for the first {RESPONSE_WARMUP_MB:g} MB of every file)")
        if self.worker:
            self.worker.max_response_s = value

    def apply_filter(self, text):
        for i in range(self.table.rowCount()):
            item = self.table.item(i, COL_VERDICT)
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
        return f"{b / 1024 ** 3:.2f} GB" if b >= 1024 ** 3 else f"{b / 1024 ** 2:.2f} MB"

    @staticmethod
    def _new_stats(size):
        return {'size': size, 'progress': 0, 'disk_speed': 0.0, 'min_speed': None,
                'max_read_ms': 0.0, 'slow_events': 0, 'reason': '', 'verdict': ''}

    def _fill_row(self, row, display_name, stats):
        self.table.setItem(row, COL_FILE, QTableWidgetItem(display_name))
        self.table.setItem(row, COL_SIZE, QTableWidgetItem(self._format_size(stats['size'])))
        self.table.setCellWidget(row, COL_PROGRESS, QProgressBar(value=stats['progress']))
        self.table.setItem(row, COL_DISK_SPEED,
                           QTableWidgetItem("" if not stats['disk_speed'] else f"{stats['disk_speed']:.2f}"))
        self.table.setItem(row, COL_MIN_SPEED,
                           QTableWidgetItem("" if stats['min_speed'] is None else f"{stats['min_speed']:.2f}"))
        self.table.setItem(row, COL_MAX_READ,
                           QTableWidgetItem("" if not stats['max_read_ms'] else f"{stats['max_read_ms']:.0f}"))
        self.table.setItem(row, COL_SLOW_EVENTS, QTableWidgetItem(str(stats['slow_events'])))
        verdict = stats['verdict']
        item = QTableWidgetItem(verdict)
        if verdict == 'OK':
            item.setBackground(Qt.green)
        elif verdict == 'BAD':
            item.setBackground(Qt.red)
        if stats['reason']:
            item.setToolTip(stats['reason'])
        self.table.setItem(row, COL_VERDICT, item)

    def _populate_table(self):
        # Get all files recursively from all subfolders
        files = [f for f in self.folder.rglob('*') if f.is_file()]
        self.table.setRowCount(len(files))
        self.data = {}
        for i, f in enumerate(files):
            # Show relative path from root folder
            self.data[str(f)] = stats = self._new_stats(f.stat().st_size)
            self._fill_row(i, str(f.relative_to(self.folder)), stats)

        self.counter_label.setText(f"Entries: {len(files)}")
        self._update_overall_progress()

    def start_scan(self):
        """Standard start: Checks unchecked files and paused files. Skips OK and BAD."""
        self._initiate_scan(retry_mode=False)

    def retry_failed(self):
        """Retry start: Checks ONLY files marked as BAD."""
        self._initiate_scan(retry_mode=True)

    def _initiate_scan(self, retry_mode=False):
        self.status.setText("Working")
        self.b_start.setEnabled(False)
        self.b_retry.setEnabled(False)  # Disable retry button too
        self.b_stop.setEnabled(True)
        self.stop_event.clear()

        tasks = []

        # Iterate over all files to determine what to add to the queue
        for fpath, stats in self.data.items():
            verdict = stats.get('verdict')

            if retry_mode:
                # RETRY MODE: Only pick up BAD files
                if verdict != 'BAD':
                    continue

                # Reset state for a fresh attempt
                start_bytes = 0
                self._reset_file_state(fpath)
            else:
                # NORMAL MODE: Skip OK and BAD. Pick up Empty (fresh) or Paused (no verdict yet)
                if verdict == 'OK' or verdict == 'BAD':
                    continue

                # Resume from where we left off
                start_bytes = int(stats['progress'] / 100 * stats['size'])

            tasks.append((Path(fpath), start_bytes))

        if not tasks:
            msg = "No failed files to retry" if retry_mode else "Nothing new to scan"
            self.status.setText(msg)
            self.b_start.setEnabled(True)
            self.b_retry.setEnabled(True)
            self.b_stop.setEnabled(False)
            return

        # Start the worker (using the updated Worker class provided previously)
        self.worker = FileReadWorker(tasks, CHUNK_SIZE_MB, self.stop_event,
                                       self.min_speed_box.value(),
                                       self.slow_checks_box.value(),
                                       self.response_box.value())
        self.worker.progress.connect(self.update_progress)
        self.worker.disk_speed.connect(self.update_disk_speed)
        self.worker.min_speed.connect(self.update_min_speed)
        self.worker.max_read.connect(self.update_max_read)
        self.worker.slow_events.connect(self.update_slow_events)
        self.worker.verdict.connect(self.update_verdict)
        self.worker.finished_all.connect(self.on_scan_finished)
        self.worker.start()

    def _reset_file_state(self, fpath):
        """Helper to visually and logically reset a file before retrying"""
        self.data[fpath]['progress'] = 0
        self.data[fpath]['verdict'] = ''
        self.data[fpath]['min_speed'] = None
        self.data[fpath]['max_read_ms'] = 0.0
        self.data[fpath]['slow_events'] = 0

        # Reset UI columns for this row
        self._set_item(fpath, COL_DISK_SPEED, "")
        self._set_item(fpath, COL_MIN_SPEED, "")
        self._set_item(fpath, COL_MAX_READ, "")
        self._set_item(fpath, COL_SLOW_EVENTS, "")

        # Reset Progress Bar and clear the verdict cell (remove the red background)
        row = self._row_for(fpath)
        if row >= 0:
            self.table.cellWidget(row, COL_PROGRESS).setValue(0)
            self.table.setItem(row, COL_VERDICT, QTableWidgetItem(""))

    def stop_scan(self):
        self.stop_event.set()

    def on_scan_finished(self):
        self.status.setText("Stopped")
        self.b_start.setEnabled(True)
        self.b_retry.setEnabled(True)
        self.b_stop.setEnabled(False)
        self._update_overall_progress()

    def update_progress(self, fname, p):
        if fname not in self.data:
            return
        self.data[fname]['progress'] = p

        row = self._row_for(fname)
        if row >= 0:
            # Only update the progress bar widget, not replace it
            self.table.cellWidget(row, COL_PROGRESS).setValue(p)

    def update_disk_speed(self, fname, speed):
        if fname not in self.data:
            return
        self.data[fname]['disk_speed'] = speed
        self._set_item(fname, COL_DISK_SPEED, f"{speed:.2f}")

    def update_min_speed(self, fname, s):
        if fname not in self.data:
            return
        self.data[fname]['min_speed'] = s
        self._set_item(fname, COL_MIN_SPEED, f"{s:.2f}")

    def update_max_read(self, fname, max_ms):
        if fname not in self.data:
            return
        self.data[fname]['max_read_ms'] = max_ms
        self._set_item(fname, COL_MAX_READ, f"{max_ms:.0f}")

    def update_slow_events(self, fname, count):
        if fname not in self.data:
            return
        self.data[fname]['slow_events'] = count
        self._set_item(fname, COL_SLOW_EVENTS, str(count))

    def update_verdict(self, fname, ok, reason):
        if fname not in self.data:
            return
        text = 'OK' if ok else 'BAD'
        self.data[fname]['verdict'] = text
        self.data[fname]['reason'] = reason
        item = QTableWidgetItem(text)
        item.setBackground(Qt.green if ok else Qt.red)
        item.setToolTip(reason)
        self._place_item(fname, COL_VERDICT, item)
        # Update overall progress when a file gets a verdict
        self._update_overall_progress()

    def _row_for(self, fpath):
        """Table row showing fpath, or -1 when the file is not listed"""
        fpath = Path(fpath)
        if self.folder and fpath.is_relative_to(self.folder):
            rel_path = str(fpath.relative_to(self.folder))
        else:
            rel_path = fpath.name

        for i in range(self.table.rowCount()):
            if self.table.item(i, COL_FILE).text() == rel_path:
                return i
        return -1

    def _set_item(self, fpath, col, val):
        self._place_item(fpath, col, QTableWidgetItem(val))

    def _place_item(self, fpath, col, item):
        row = self._row_for(fpath)
        if row >= 0:
            self.table.setItem(row, col, item)

    def _load_data(self):
        if not data_file.exists():
            return
        try:
            state = json.loads(data_file.read_text())

            # restore the checking parameters
            self.min_speed_box.setValue(state.get('min_chunk_speed', MIN_CHUNK_SPEED_MB_S))
            self.slow_checks_box.setValue(state.get('max_slow_events',
                                                     state.get('max_consecutive_slow', MAX_SLOW_EVENTS)))
            self.response_box.setValue(state.get('max_response_s', MAX_RESPONSE_S))

            # restore last folder if it still exists
            folder = state.get('folder', '')
            if folder and os.path.isdir(folder):
                self.folder = Path(folder)
                self.folder_label.setText(folder)

            # restore *all* valid file entries, no matter where they are
            for fpath_str, stats in state.get('files', {}).items():
                fpath = Path(fpath_str)
                if not fpath.is_file():
                    continue  # skip missing files

                self.data[str(fpath)] = {
                    'size': fpath.stat().st_size,
                    'progress': stats.get('progress', 0),
                    'disk_speed': stats.get('disk_speed', 0.0),
                    'min_speed': stats.get('min_speed'),
                    'max_read_ms': stats.get('max_read_ms', stats.get('max_stall_ms', 0.0)),
                    'slow_events': stats.get('slow_events', 0),
                    'reason': stats.get('reason', ''),
                    'verdict': stats.get('verdict', '')
                }

            # rebuild the table to show all restored entries
            self._repopulate_table_from_data()
            self.apply_filter(self.filter_combo.currentText())
            self._update_overall_progress()

        except Exception as e:
            print(f"Error loading data: {e}")

    # helper: recreate the table from self.data
    def _repopulate_table_from_data(self):
        self.table.setRowCount(len(self.data))
        for row, (fpath_str, stats) in enumerate(self.data.items()):
            fpath = Path(fpath_str)

            # Use relative path if we have a folder context, otherwise use filename
            if self.folder and fpath.is_relative_to(self.folder):
                display_name = str(fpath.relative_to(self.folder))
            else:
                display_name = fpath.name

            self._fill_row(row, display_name, stats)

        self.counter_label.setText(f"Entries: {len(self.data)}")

if __name__ == '__main__':
    set_windows_app_id()
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon(get_running_path(ICON_FILE)))
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
