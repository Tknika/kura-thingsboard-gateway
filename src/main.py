#!/usr/bin/env python
# -*- coding: utf-8 -*-

from configuration_handler import ConfigurationHandler
from kura_devices_handler import KuraDevicesHandler
import logging
import paho.mqtt.client as mqtt_client
import signal
from tb_gateway_handler import TbGatewayHandler

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(name)-20s  - %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)


def signal_handler(sig, frame):
    client.loop_stop()
    tb_gateway.stop()
    kura_devices_handler.stop()

def on_configuration_changed():
    logger.debug("We need to restart the modules")
    restart_modules()

def restart_modules():
    global client, tb_gateway, kura_devices_handler
    client.loop_stop()
    tb_gateway.stop()
    kura_devices_handler.stop()

    client.reinitialise(configuration_handler.configuration["MQTT_CLIENT_ID"])
    client.username_pw_set(configuration_handler.configuration["MQTT_USERNAME"], configuration_handler.configuration["MQTT_PASSWORD"])
    client.connect(configuration_handler.configuration["MQTT_HOST"], configuration_handler.configuration["MQTT_PORT"], 60)
    client.loop_start()

    kura_devices_handler = KuraDevicesHandler(configuration_handler.configuration["KURA_PREFIX"], client)
    tb_gateway = TbGatewayHandler(configuration_handler.configuration["THINGSBOARD_HOST"], configuration_handler.configuration["THINGSBOARD_KEY"], kura_devices_handler)

    tb_gateway.start()
    kura_devices_handler.start()


if __name__ == "__main__":
    logger.debug("Starting program...")

    configuration_handler = ConfigurationHandler()
    configuration_handler.add_change_callback(on_configuration_changed)
    
    client = mqtt_client.Client(configuration_handler.configuration["MQTT_CLIENT_ID"])
    client.username_pw_set(configuration_handler.configuration["MQTT_USERNAME"], configuration_handler.configuration["MQTT_PASSWORD"])
    client.connect(configuration_handler.configuration["MQTT_HOST"], configuration_handler.configuration["MQTT_PORT"], 60)
    client.loop_start()

    kura_devices_handler = KuraDevicesHandler(configuration_handler.configuration["KURA_PREFIX"], client)
    tb_gateway = TbGatewayHandler(configuration_handler.configuration["THINGSBOARD_HOST"], configuration_handler.configuration["THINGSBOARD_KEY"], 
                                    kura_devices_handler, configuration_handler.configuration["THINGSBOARD_PORT"])
    
    tb_gateway.start()
    kura_devices_handler.start()

    signal.signal(signal.SIGINT, signal_handler)
    signal.pause()