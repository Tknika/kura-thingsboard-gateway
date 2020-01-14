#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from tb_mqtt_client.tb_gateway_mqtt import TBGatewayMqttClient
import time
import threading

logger = logging.getLogger(__name__)

class TbGatewayHandler(object):

    def __init__(self, hostname, key, data_provider, port=1883):
        self.hostname = hostname
        self.port = port
        self.key = key
        self.tb_connection = TBGatewayMqttClient(self.hostname, self.key)
        self.data_provider = data_provider
        self.tb_devices = []

    def is_connected(self):
        return self.tb_connection._TBDeviceMqttClient__is_connected

    def start(self):
        logger.debug("Starting TB gateway connection")
        self.data_provider.register_callback(self.__data_update_handler)
        self.tb_connection.connect(port=self.port)
        while not self.is_connected():
            time.sleep(0.1)
        self.tb_connection.gw_set_server_side_rpc_request_handler(self.__rpc_request_handler)
        logger.debug("TB gateway connected")
        
    def stop(self):
        logger.debug("Stopping TB gateway connection")
        if self.is_connected():
            for device in self.tb_devices:
                self.tb_connection.gw_disconnect_device(device)
        self.tb_connection.disconnect()

    def __data_update_handler(self, device_id, event_type, value):
        logger.debug("New value for event '{}' from '{}': {}".format(event_type, device_id, value))
        if event_type == "status_changed":
            if value == "started":
                self.__connect_device(device_id)
            elif value == "stopped":
                self.__disconnect_device(device_id)
            else:
                logger.warn("Known status state for device '{}': {}".format(device_id, event_type))
        elif event_type == "attribute_changed":
            self.__send_attribute_data(device_id, value)
        elif event_type == "telemetry_changed":
            self.__send_telemetry_data(device_id, value)

    def __connect_device(self, name):
        if name not in self.tb_devices:
            self.tb_connection.gw_connect_device(name)
            self.tb_devices.append(name)
        else:
            logger.warning("Device '{}' already connected".format(name))

    def __disconnect_device(self, name):
        if name in self.tb_devices:
            self.tb_connection.gw_disconnect_device(name)
            self.tb_devices.remove(name)
        else:
            logger.warning("Device '{}' not connected".format(name))

    def __send_telemetry_data(self, name, values, ts=None):
        if name not in self.tb_devices:
            logger.warning("Device '{}' not connected".format(name))
            return
        if ts is None:
            ts = int(round(time.time() * 1000))
        self.tb_connection.gw_send_telemetry(name, { "ts": ts, "values": values})

    def __send_attribute_data(self, name, values):
        if name not in self.tb_devices:
            logger.warning("Device '{}' not connected".format(name))
            return
        self.tb_connection.gw_send_attributes(name, values)

    def __rpc_request_handler(self, content):
        req_id = content["data"]["id"]
        device_id = content["device"]
        try:
            method = content["data"]["method"]
            action = method.split(".")[0]
            channel = method.split(".")[1]
        except Exception as e:
            logger.error("Error detected: {}".format(e))
        if action == "setValue":
            new_value = content["data"]["params"]
            logger.debug("We need to write the vale '{}' in the '{}' channel of '{}' device".format(new_value, channel, device_id))
        elif action == "getValue":
            data = self.data_provider.get_device_data(device_id, channel)
            self.tb_connection.gw_send_rpc_reply(device_id, req_id, data)
        else:
            logger.warn("Unknown action received")