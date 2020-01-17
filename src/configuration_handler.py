#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
from watchdog.observers import Observer
from watchdog.events import FileModifiedEvent

logger = logging.getLogger(__name__)

class ConfigurationHandler(object):

    def __init__(self, file="conf/configuration.json"):
        self.file_ = file
        self.file_path = os.path.realpath(self.file_)
        self.file_folder = os.path.dirname(self.file_path)
        self.file_name = os.path.basename(self.file_path)
        self.configuration = {}
        self.callbacks = []
        self.__load_configuration()
        self.__start_watcher()

    def __read_configuration(self):
        with open(self.file_path, 'r') as f:
            return json.load(f)

    def __load_configuration(self):
        with open(self.file_path, 'r') as f:
            self.configuration = json.load(f)

    def __save_configuration(self):
        with open(self.file_path, 'w+') as f:
            f.write(self.configuration)

    def __start_watcher(self):
        event_handler = FileModifiedHandler(self.file_folder, self.file_name, self.__on_modified)   
        observer = Observer()
        observer.schedule(event_handler, self.file_folder)
        observer.start()

    def __on_modified(self, file_path):
        if file_path != self.file_path:
            logger.warn("Notification received from non-configuration file")
            return
        new_configuration = self.__read_configuration()
        if new_configuration == self.configuration:
            logger.debug("Configuration content has not changed")
            return
        self.configuration = new_configuration
        # Notify callbacks
        for callback in self.callbacks:
            callback()

    def add_change_callback(self, callback):
        self.callbacks.append(callback)

    def update_configuration(self, key, value):
        if not key in self.configuration:
            logger.error("'{}' not present in configuration file".format(key))
            return
        if self.configuration[key] == value:
            logger.debug("'{}' value hasn't changed ('{}')".format(key, value))
            return
        self.configuration[key] = value
        self.__save_configuration()


class FileModifiedHandler(FileModifiedEvent):

    def __init__(self, file_folder, file_name, on_modified):
        FileModifiedEvent.__init__(self, file_folder)
        self.file_folder = file_folder
        self.file_name = file_name
        self.on_modified = on_modified

    def dispatch(self, event):
        if event.src_path.split("/")[-1] != self.file_name:
            return
        self.on_modified(event.src_path)