import PyQt6
from PyQt6.QtWidgets import QLabel, QComboBox, QToolButton, QHBoxLayout, QWidget, QLineEdit, QCheckBox
from PyQt6.QtCore import Qt

def create_info_btn(tooltip: str):
    btn = QToolButton()
    btn.setText("ℹ")
    btn.setToolTip(tooltip)
    return btn

def create_combo(start=0, end=20, default=0):
    cb = QComboBox()
    for i in range(start, end+1):
        cb.addItem(str(i))
    cb.setCurrentText(str(default))
    return cb
