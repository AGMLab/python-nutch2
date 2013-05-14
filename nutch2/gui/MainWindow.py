#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Copyright 2013, Ahmet Emre Aladağ, AGMLAB

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import sys
from PyQt4.QtCore import SIGNAL, QVariant, QAbstractListModel, QModelIndex, Qt
from PyQt4.QtGui import QApplication, QMainWindow, QGridLayout, QWidget, \
    QListView, QPushButton, QHBoxLayout, QVBoxLayout, QComboBox, QLabel

from nutch2.runner.hadoop import HadoopJobMonitor


class JobListModel(QAbstractListModel):
    def __init__(self, job_monitor, parent=None, *args):
        """ datain: a list where each item is a row
        """
        QAbstractListModel.__init__(self, parent, *args)

        self.job_monitor = job_monitor

    def rowCount(self, parent=QModelIndex()):
        return len(self.job_monitor.job_names)

    def data(self, index, role):
        if index.isValid() and role == Qt.DisplayRole:
            return QVariant(self.job_monitor.job_names[index.row()])
        else:
            return QVariant()

    def get_job_object(self, name):
        return self.job_monitor.get_job_with_name(str(name))


class MainWindow(QMainWindow):
    def __init__(self):
        QMainWindow.__init__(self)
        self.setupGUI()
        self.job_monitor = HadoopJobMonitor()

    def setupGUI(self):
        layout = QGridLayout()
        self.widget = QWidget()
        self.widget.setLayout(layout)
        self.setCentralWidget(self.widget)
        self.setWindowTitle("Nutch Job Service")

        # create active job list
        active_label = QLabel("Active Hadoop Jobs")
        layout.addWidget(active_label, 0, 0)
        self.lv = QListView()
        layout.addWidget(self.lv, 1, 0, 3, 1)

        # Create buttons
        self.stop_button = QPushButton("Stop")
        self.refresh_button = QPushButton("Refresh")
        button_layout = QVBoxLayout()
        button_layout.addWidget(self.stop_button)
        button_layout.addWidget(self.refresh_button)

        layout.addLayout(button_layout, 1, 1)

        # Start Button
        self.new_job_layout = QHBoxLayout()
        self.new_job_combo = QComboBox()
        self.new_job_combo.addItems(["Inject", "Generate", "Fetch", "Parse", "Solr"])
        self.new_job_button = QPushButton("Start New Job")
        self.new_job_layout.addWidget(self.new_job_combo)
        self.new_job_layout.addWidget(self.new_job_button)
        layout.addLayout(self.new_job_layout, 5, 0)

        #self.statusBar()
        self.connectSlots()

    def load_data(self):
        """
        Loads data from the hadoop job list.
        """

        self.showStatusMessage("Fetching Hadoop job list...")
        print "Loading data..."
        self.job_monitor.fetch_hadoop_job_info()
        self.job_list_model = JobListModel(self.job_monitor, self)
        self.lv.setModel(self.job_list_model)
        self.updateStatusBar()

    def start_new_nob(self):
        """
        Starts a new job according to the selected item in combo box.
        """
        current_text = self.new_job_combo.currentText()
        print "Starting %s" % current_text

    def stop_job(self):
        current_item = self.lv.currentIndex().data(0).toString()
        job_object = self.job_list_model.get_job_object(current_item)
        job_object.stop()

    def connectSlots(self):
        """
        Connects signals to slots.
        """
        self.connect(self.refresh_button, SIGNAL('clicked()'), self.load_data)
        self.connect(self.stop_button, SIGNAL('clicked()'), self.stop_job)
        self.connect(self.new_job_button, SIGNAL('clicked()'), self.start_new_nob)

    def showStatusMessage(self, message):
        self.statusBar().showMessage(message)

    def updateStatusBar(self):
        """
        Updates status bar according to the number of active hadoop jobs.
        """
        self.statusBar().showMessage("%s jobs are active." % self.job_monitor.num_jobs)


def main():
    app = QApplication(sys.argv)
    mainWindow = MainWindow()
    mainWindow.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()