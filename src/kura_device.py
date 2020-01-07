#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import kura_payload_handler
import logging
import time
import uuid

logger = logging.getLogger(__name__)

class KuraDevice(object):

    def __init__(self, prefix, id, account, mqtt_connection):
        self.prefix = prefix
        self.id = id
        self.account = account
        self.telemetry_topic = "{}/{}/#".format(self.account, self.id)
        self.requester_id = "{}-{}-requester".format(self.account, self.id)
        self.assets = {}
        self.channels = {}
        self.callback = None
        self.mqtt_connection = mqtt_connection

    def start(self):
        logger.debug("Starting device '{}'".format(self.id))
        self.mqtt_connection.message_callback_add(self.telemetry_topic, self.__telemetry_topic_handler)
        self.mqtt_connection.subscribe("{}".format(self.telemetry_topic), 0)
        self.callback(self.id, "status_changed", "started")
        self.__request_assets()
        self.__request_asset_values()

    def stop(self):
        logger.debug("Stopping device '{}'".format(self.id))
        self.mqtt_connection.unsubscribe(self.telemetry_topic)
        self.callback(self.id, "status_changed", "stopped")

    def restart(self):
        self.__request_assets()
        self.__request_asset_values()

    def register_callback(self, callback):
        if self.callback is None:
            self.callback = callback 
    
    def get_channel_value(self, channel, req_asset=None):
        if channel in self.channels:
            return self.channels[channel]["value"]
        logger.warn("Channel '{}' not available for device '{}'".format(channel, self.id))
        return None

    def set_channel_value(self, channel, value):
        pass

    def __request_assets(self):
        logger.debug("Sending device '{}' assets request".format(self.id))
        app_id = "ASSET-V1"
        resource_id = "GET/assets"
        request_id = uuid.uuid4().hex

        pub_topic = "{}/{}/{}/{}/{}".format(self.prefix, self.account, self.id, app_id, resource_id)
        sub_topic = "{}/{}/{}/{}/REPLY/{}".format(self.prefix, self.account, self.requester_id, app_id, request_id)
        self.mqtt_connection.message_callback_add(sub_topic, self.__assets_request_handler)
        self.mqtt_connection.subscribe(sub_topic, 0)

        metrics = { "request.id": request_id, "requester.client.id": self.requester_id}
        payload = kura_payload_handler.create_payload(metrics)

        self.mqtt_connection.publish(pub_topic, payload)


    def __assets_request_handler(self, client, obj, msg):
        logger.debug("Getting device '{}' assets response".format(self.id))
        self.mqtt_connection.unsubscribe(msg.topic)
        message = kura_payload_handler.decode_message(msg.payload)
        body_string = message.body.decode("utf-8")
        body = json.loads(body_string)

        for asset in body:
            asset_name = asset["name"]
            self.assets[asset_name] = {}
            for channel in asset["channels"]:
                channel_name = channel["name"]
                channel_type = channel["type"]
                channel_mode = channel["mode"]
                self.assets[asset_name][channel_name] = { "type": channel_type, "mode": channel_mode, "value": None }
                self.channels[channel_name] = { "asset": asset_name, "type": channel_type, "mode": channel_mode, "value": None }

    def __request_asset_values(self, asset=None, channels=None):
        app_id = "ASSET-V1"
        resource_id = "EXEC/read"
        request_id = uuid.uuid4().hex

        pub_topic = "{}/{}/{}/{}/{}".format(self.prefix, self.account, self.id, app_id, resource_id)
        sub_topic = "{}/{}/{}/{}/REPLY/{}".format(self.prefix, self.account, self.requester_id, app_id, request_id)
        self.mqtt_connection.message_callback_add(sub_topic, self.__asset_values_request_handler)
        self.mqtt_connection.subscribe(sub_topic, 0)

        metrics = { "request.id": request_id, "requester.client.id": self.requester_id}
        payload = kura_payload_handler.create_payload(metrics)

        self.mqtt_connection.publish(pub_topic, payload)

    def __asset_values_request_handler(self, client, obj, msg):
        logger.debug("Getting device '{}' asset values response".format(self.id))
        self.mqtt_connection.unsubscribe(msg.topic)
        message = kura_payload_handler.decode_message(msg.payload)
        body_string = message.body.decode("utf-8")
        body = json.loads(body_string)

        for asset in body:
            if not "name" in asset:
                break
            asset_name = asset["name"]
            for channel in asset["channels"]:
                if not "name" in channel or not "value" in channel:
                    continue
                channel_name = channel["name"]
                channel_value = channel["value"]
                if self.channels[channel_name]["mode"] != "READ":
                    self.channels[channel_name]["value"] = channel_value
                    self.callback(self.id, "attribute_changed", { channel_name: channel_value})              

    def __write_channel_value(self, asset=None, channel=None, values=None):
        app_id = "ASSET-V1"
        resource_id = "EXEC/write"
        request_id = uuid.uuid4().hex

        pub_topic = "{}/{}/{}/{}/{}".format(self.prefix, self.account, self.id, app_id, resource_id)

        if asset is None:
            asset = self.__get_channel_asset(channel)
        
        metrics = { "request.id": request_id, "requester.client.id": self.requester_id}
        payload = kura_payload_handler.create_payload(metrics)

        self.mqtt_connection.publish(pub_topic, payload)

    def __telemetry_topic_handler(self, client, obj, msg):
        logger.debug("New telemetry message published on '{}':".format(msg.topic))
        message = kura_payload_handler.decode_message(msg.payload)
        values = self.__extract_metrics_values(message)
        try:
            ts = message.timestamp
        except AttributeError:
            ts = None
        telemetry_values = self.__filter_telemetry_values(values)
        attribute_values = self.__filter_attribute_values(values)
        if telemetry_values:
            logger.debug("New telemetry value: '{}' ('{}')".format(telemetry_values, self.id))
            for channel, value in telemetry_values.items():
                self.channels[channel]["value"] = value
            self.callback(self.id, "telemetry_changed", telemetry_values)
        if attribute_values:
            logger.debug("New attribute value:'{}' ('{}')".format(attribute_values, self.id))
            for channel, value in attribute_values.items():
                self.channels[channel]["value"] = value
            self.callback(self.id, "attribute_changed", attribute_values)
        
    def __extract_metrics_values(self, message):
        type_mapper = { 0: "double_value", #DOUBLE
                        1: "float_value", #FLOAT
                        2: "long_value", #INT64
                        3: "int_value", #INT32
                        4: "bool_value", #BOOL
                        5: "string_value", #STRING
                        6: "bytes_value" #BYTES
                        }
        filtered = [m for m in message.metric if m.name != "assetName"]
        values = { m.name: getattr(m, type_mapper[m.type]) for m in filtered }
        return values

    def __filter_telemetry_values(self, values):
        telemetry_values = {}
        for key, value in values.items():
            if key in self.channels:
                if self.channels[key]["mode"] == "READ":
                    telemetry_values[key] = value
            else:
                logger.error("'{}' not in device channels, assets should be queried again".format(key))
        return telemetry_values

    def __filter_attribute_values(self, values):
        attribute_values = {}
        for key, value in values.items():
            if key in self.channels:
                if self.channels[key]["mode"] != "READ":
                    attribute_values[key] = value
            else:
                logger.error("'{}' not in device channels, assets should be queried again".format(key))
        return attribute_values

    def __get_channel_asset(self, channel):
        if channel in self.channels:
            return self.channels[channel]["asset"]
        return None

