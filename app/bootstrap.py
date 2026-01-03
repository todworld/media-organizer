import os
import sys
from PySide6.QtWidgets import QApplication
from ui.main_window import MainWindow
from persistence.db import Database

APP_NAME = "Media Organizer"

def run_app():
    app = QApplication(sys.argv)
    try:
        app.setStyle("Fusion")
    except Exception:
        pass

    app.setStyleSheet("""
        QMainWindow { background-color: #f6f7f9; }
        QWidget { font-size: 12px; color: #111; }

        QLabel { color: #111; }

        QLineEdit, QSpinBox, QTableWidget, QListWidget {
        background: #ffffff;
        color: #111;
        border: 1px solid #dcdfe4;
        border-radius: 8px;
        padding: 6px;
    }

        QGroupBox {
        background: #ffffff;
        border: 1px solid #dcdfe4;
        border-radius: 10px;
        margin-top: 14px;
        color: #111;
    }
        QGroupBox::title {
        subcontrol-origin: margin;
        left: 12px;
        padding: 0 6px;
        color: #111;
    }

        QPushButton {
        background: #ffffff;
        color: #111;
        border: 1px solid #dcdfe4;
        border-radius: 8px;
        padding: 8px 14px;
    }
        QPushButton:hover { background: #eef1f5; }
    """)


    app.setApplicationName(APP_NAME)

    # Default DB location: alongside app (you can change later in UI; runs store artifacts in user-selected path)
    base_dir = os.path.abspath(os.path.dirname(__file__))
    db_path = os.path.join(os.path.dirname(base_dir), "organizer.sqlite")

    db = Database(db_path)
    db.init()

    w = MainWindow(db=db)
    w.resize(1100, 750)
    w.show()

    sys.exit(app.exec())
