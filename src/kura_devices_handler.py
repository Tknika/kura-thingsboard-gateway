#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import kura_payload_handler
from kura_device import KuraDevice
import logging

logger = logging.getLogger(__name__)


class KuraDevicesHandler(object):

    def __init__(self, kura_prefix, mqtt_connection, filename="conf/registered_devices.json"):
        self.kura_prefix = kura_prefix
        self.kura_birth_topic = "{}/+/+/MQTT/BIRTH".format(self.kura_prefix)
        self.mqtt_connection = mqtt_connection
        self.filename = filename
        self.registered_devices = {}
        self.started_devices = {}
        self.callbacks = []

    def start(self):
        self.__load_registered_devices()
        self.mqtt_connection.message_callback_add(self.kura_birth_topic, self.__birth_handler)
        res = self.mqtt_connection.subscribe("{}".format(self.kura_birth_topic), 0)
        logger.debug("Subscription result: {}".format(res))

    def stop(self):
        pass

    def register_callback(self, callback):
        self.callbacks.append(callback)

    def get_device_data(self, device, channel):
        if not device in self.started_devices:
            logger.warn("Information requested about unknown device: '{}' ('{}')".format(device, channel))
            return None
        return self.started_devices[device].get_channel_value(channel)

    def set_device_data(self, device, channel, value):
        if not device in self.started_devices:
            logger.warn("Action requested about unknown device: '{}' ('{}')".format(device, channel))
            return None
        self.started_devices[device].set_channel_value(channel, value)

    def __birth_handler(self, client, obj, msg):
        logger.debug("New birth message published on topic: {}".format(msg.topic))

        topic = msg.topic.split("/")
        account_name = topic[1]
        client_id = topic[2]
        #message = kura_decoder.decode_message(msg.payload)
        
        logger.info("New client id: {}".format(client_id))
        self.__handle_device(client_id, account_name)

    def __handle_device(self, client_id, account_name):
        self.__register_device(client_id, account_name)
        self.__start_device(client_id, account_name)

    def __register_device(self, client_id, account_name):
        if client_id in self.registered_devices:
            logger.warn("Device '{}' already registered".format(client_id))
            return

        self.registered_devices[client_id] = { "client_id": client_id, "account_name": account_name}

        with open(self.filename, 'w') as f:
            json.dump(self.registered_devices, f)

    def __start_device(self, client_id, account_name):
        if client_id not in self.started_devices:
            device = KuraDevice(self.kura_prefix, client_id, account_name, self.mqtt_connection)
            device.register_callback(self.__callback_handler)
            device.start()
            self.started_devices[client_id] = device
        else:
            self.started_devices[client_id].restart()

    def __load_registered_devices(self):
        try:
            with open(self.filename, 'r') as f:
                file_devices = json.load(f)
        except FileNotFoundError:
            logger.debug("'Registered devices' file not found")
            file_devices = {}
            with open(self.filename, 'w+') as f:
                f.write("{}")

        self.registered_devices = file_devices
        for id, info in self.registered_devices.items():
            self.__handle_device(info["client_id"], info["account_name"])

    def __callback_handler(self, id, event_type, value):
        [callback(id, event_type, value) for callback in self.callbacks]

        