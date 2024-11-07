import sys
import os
import json
import time
from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLabel,
    QLineEdit,
    QListWidget,
    QFileDialog,
    QMessageBox,
    QInputDialog,
    QCheckBox,
    QProgressDialog,
)
from PyQt6.QtCore import Qt
from pathlib import Path
import threading
import queue
from contextlib import contextmanager
import tempfile


class FileTagManager:
    def __init__(self):
        self.db_file = "file_tags.json"
        self.tags_db = self._load_db()
        self._save_queue = queue.Queue()
        self._last_save = 0
        self._save_lock = threading.Lock()
        self._pending_changes = False
        self._start_save_thread()

    def _start_save_thread(self):
        """Start background thread for saving changes"""
        self._save_thread = threading.Thread(target=self._save_worker, daemon=True)
        self._save_thread.start()

    def _save_worker(self):
        """Background worker that handles saving the database"""
        while True:
            try:
                # Wait for changes to save
                self._save_queue.get(timeout=1)

                # Rate limit saves to once per second
                time_since_last_save = time.time() - self._last_save
                if time_since_last_save < 1:
                    time.sleep(1 - time_since_last_save)

                with self._save_lock:
                    if self._pending_changes:
                        self._save_db_with_retry()
                        self._pending_changes = False
                        self._last_save = time.time()

                self._save_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Error in save worker: {e}")

    def _save_db_with_retry(self, max_retries=3, delay=1):
        """Save database with retry mechanism"""
        for attempt in range(max_retries):
            try:
                # Create temporary file in the same directory
                temp_dir = os.path.dirname(os.path.abspath(self.db_file))
                with tempfile.NamedTemporaryFile(
                    mode="w", dir=temp_dir, delete=False, suffix=".tmp"
                ) as temp_file:
                    json.dump(self.tags_db, temp_file, indent=4)
                    temp_file.flush()
                    os.fsync(temp_file.fileno())  # Ensure all data is written

                # Attempt to rename the temporary file
                if os.path.exists(self.db_file):
                    if sys.platform == "win32":
                        try:
                            os.remove(self.db_file)
                        except PermissionError:
                            time.sleep(delay)
                            continue

                os.rename(temp_file.name, self.db_file)
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                time.sleep(delay)
                delay *= 2  # Exponential backoff

    def queue_save(self):
        """Queue a save operation"""
        with self._save_lock:
            self._pending_changes = True
        self._save_queue.put(True)

    def _load_db(self):
        """Load the tags database with proper error handling"""
        if os.path.exists(self.db_file):
            try:
                with open(self.db_file, "r", encoding="utf-8") as f:
                    content = f.read()
                    if not content.strip():
                        return {}
                    return json.loads(content)
            except (json.JSONDecodeError, FileNotFoundError, PermissionError) as e:
                backup_file = f"{self.db_file}.backup"
                try:
                    if (
                        os.path.exists(self.db_file)
                        and os.path.getsize(self.db_file) > 0
                    ):
                        os.rename(self.db_file, backup_file)
                except OSError:
                    pass
                return {}
        return {}

    def add_tags(self, filepath, tags):
        """Add tags with error handling"""
        try:
            filepath = str(Path(filepath))
            if filepath not in self.tags_db:
                self.tags_db[filepath] = []
            for tag in tags:
                tag = tag.strip().lower()
                if tag and tag not in self.tags_db[filepath]:
                    self.tags_db[filepath].append(tag)
            self.queue_save()
        except Exception as e:
            raise RuntimeError(f"Failed to add tags: {e}")

    def add_tags_to_directory(self, directory, tags, progress_callback=None):
        """Add tags to all files in a directory with batched saves"""
        try:
            files = []
            for root, _, filenames in os.walk(directory):
                for filename in filenames:
                    files.append(os.path.join(root, filename))

            total_files = len(files)
            batch_size = 100  # Process files in batches

            for i in range(0, total_files, batch_size):
                batch = files[i : i + batch_size]
                for filepath in batch:
                    if filepath not in self.tags_db:
                        self.tags_db[filepath] = []
                    for tag in tags:
                        tag = tag.strip().lower()
                        if tag and tag not in self.tags_db[filepath]:
                            self.tags_db[filepath].append(tag)

                self.queue_save()  # Save after each batch

                if progress_callback:
                    progress_callback(min(i + batch_size, total_files), total_files)

            return total_files
        except Exception as e:
            raise RuntimeError(f"Failed to add tags to directory: {e}")

    def remove_tag(self, filepath, tag):
        """Remove tag with error handling"""
        try:
            filepath = str(Path(filepath))
            if filepath in self.tags_db and tag in self.tags_db[filepath]:
                self.tags_db[filepath].remove(tag)
                if not self.tags_db[filepath]:
                    del self.tags_db[filepath]
                self.queue_save()
        except Exception as e:
            raise RuntimeError(f"Failed to remove tag: {e}")

    def get_tags(self, filepath):
        """Get tags with error handling"""
        try:
            filepath = str(Path(filepath))
            return self.tags_db.get(filepath, [])
        except Exception as e:
            print(f"Error getting tags: {e}")
            return []

    def search_by_tags(self, search_tags):
        """Search tags with error handling"""
        try:
            search_tags = [tag.strip().lower() for tag in search_tags]
            results = []
            for filepath, tags in self.tags_db.items():
                if all(tag in tags for tag in search_tags):
                    results.append(filepath)
            return results
        except Exception as e:
            print(f"Error searching tags: {e}")
            return []


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        try:
            self.tag_manager = FileTagManager()
            self.current_file = None
            self.setup_ui()
        except Exception as e:
            QMessageBox.critical(
                self, "Error", f"Failed to initialize application: {e}"
            )
            raise

    def setup_ui(self):
        self.setWindowTitle("File Tagger")
        self.setMinimumSize(800, 600)

        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)

        # File/Directory selection area
        file_layout = QHBoxLayout()
        self.file_label = QLabel("No file/directory selected")
        select_file_btn = QPushButton("Select File")
        select_dir_btn = QPushButton("Select Directory")
        select_file_btn.clicked.connect(self.select_file)
        select_dir_btn.clicked.connect(self.select_directory)
        file_layout.addWidget(self.file_label)
        file_layout.addWidget(select_file_btn)
        file_layout.addWidget(select_dir_btn)
        layout.addLayout(file_layout)

        # Tag input area
        tag_layout = QHBoxLayout()
        self.tag_input = QLineEdit()
        self.tag_input.setPlaceholderText("Enter tags (comma-separated)")
        add_tag_btn = QPushButton("Add Tags")
        add_tag_btn.clicked.connect(self.add_tags)
        tag_layout.addWidget(self.tag_input)
        tag_layout.addWidget(add_tag_btn)
        layout.addLayout(tag_layout)

        # Directory options
        self.include_subdirs = QCheckBox("Include subdirectories")
        self.include_subdirs.setChecked(True)
        layout.addWidget(self.include_subdirs)

        # Current tags area
        layout.addWidget(QLabel("Current Tags:"))
        self.tags_list = QListWidget()
        self.tags_list.itemDoubleClicked.connect(self.remove_tag)
        layout.addWidget(self.tags_list)

        # Search area
        search_layout = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search tags (comma-separated)")
        search_btn = QPushButton("Search")
        search_btn.clicked.connect(self.search_files)
        search_layout.addWidget(self.search_input)
        search_layout.addWidget(search_btn)
        layout.addLayout(search_layout)

        # Search results area
        layout.addWidget(QLabel("Search Results (Double-click to open file location):"))
        self.results_list = QListWidget()
        self.results_list.itemDoubleClicked.connect(self.open_file_location)
        layout.addWidget(self.results_list)

    def select_file(self):
        try:
            filepath, _ = QFileDialog.getOpenFileName(self, "Select File")
            if filepath:
                self.current_file = filepath
                self.file_label.setText(os.path.basename(filepath))
                self.update_tags_list()
                self.include_subdirs.setEnabled(False)
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to select file: {e}")

    def select_directory(self):
        try:
            directory = QFileDialog.getExistingDirectory(self, "Select Directory")
            if directory:
                self.current_file = directory
                self.file_label.setText(directory)
                self.update_tags_list()
                self.include_subdirs.setEnabled(True)
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to select directory: {e}")

    def add_tags(self):
        try:
            if not self.current_file:
                QMessageBox.warning(
                    self, "Error", "Please select a file or directory first!"
                )
                return

            tags = [
                tag.strip() for tag in self.tag_input.text().split(",") if tag.strip()
            ]
            if not tags:
                return

            if os.path.isfile(self.current_file):
                self.tag_manager.add_tags(self.current_file, tags)
                self.tag_input.clear()
                self.update_tags_list()
            else:  # Directory
                progress = QProgressDialog(
                    "Adding tags to files...", "Cancel", 0, 100, self
                )
                progress.setWindowModality(Qt.WindowModality.WindowModal)

                def update_progress(current, total):
                    progress.setValue(int(current / total * 100))

                directory = self.current_file
                if self.include_subdirs.isChecked():
                    total_files = self.tag_manager.add_tags_to_directory(
                        directory, tags, update_progress
                    )
                else:
                    # Only process files in the current directory
                    for filename in os.listdir(directory):
                        filepath = os.path.join(directory, filename)
                        if os.path.isfile(filepath):
                            self.tag_manager.add_tags(filepath, tags)

                progress.setValue(100)
                self.tag_input.clear()
                QMessageBox.information(
                    self, "Success", f"Added tags to files in directory"
                )
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to add tags: {e}")

    def update_tags_list(self):
        try:
            self.tags_list.clear()
            if self.current_file and os.path.isfile(self.current_file):
                tags = self.tag_manager.get_tags(self.current_file)
                self.tags_list.addItems(tags)
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to update tags list: {e}")

    def remove_tag(self, item):
        try:
            if self.current_file and os.path.isfile(self.current_file):
                reply = QMessageBox.question(
                    self,
                    "Remove Tag",
                    f"Remove tag '{item.text()}'?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                )

                if reply == QMessageBox.StandardButton.Yes:
                    self.tag_manager.remove_tag(self.current_file, item.text())
                    self.update_tags_list()
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to remove tag: {e}")

    def search_files(self):
        try:
            search_terms = [
                term.strip() for term in self.search_input.text().split(",")
            ]
            if not search_terms or not search_terms[0]:
                return

            results = self.tag_manager.search_by_tags(search_terms)
            self.results_list.clear()
            self.results_list.addItems([os.path.basename(path) for path in results])
            self.results_list.setToolTip("Double-click to open file location")

            # Store full paths for opening files
            self._search_results = results
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to search files: {e}")

    def open_file_location(self, item):
        try:
            index = self.results_list.row(item)
            filepath = self._search_results[index]

            # Open the file's directory and select the file
            directory = os.path.dirname(filepath)
            os.system(f'explorer /select,"{filepath}"')
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to open file location: {e}")


if __name__ == "__main__":
    try:
        app = QApplication(sys.argv)
        window = MainWindow()
        window.show()
        sys.exit(app.exec())
    except Exception as e:
        print(f"Critical error: {e}")
        sys.exit(1)
