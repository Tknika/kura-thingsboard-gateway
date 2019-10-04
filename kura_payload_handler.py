#!/usr/bin/env python
# -*- coding: utf-8 -*-

import google.protobuf
import kurapayload_pb2 as kura_payload
import logging
import zlib

logger = logging.getLogger(__name__)

payload_decoder = kura_payload.KuraPayload()

def decode_message(message):
    ungziped = decode_gzip(message)
    unprotobuffed = decode_protobuf(ungziped)
    return unprotobuffed

def decode_gzip(message):
    try:
        message = zlib.decompress(message, 16+zlib.MAX_WBITS)
    except zlib.error:
        logger.debug("Message is not gzip encoded")
    return message

def decode_protobuf(message):
    try:
        payload_decoder.ParseFromString(message)
        return payload_decoder
    except google.protobuf.message.DecodeError:
        logger.error("Message is not protobuffered")
    return None

def create_payload(metrics):
    payload = kura_payload.KuraPayload()
    for key, value in metrics.items():
        m = kura_payload.KuraPayload.KuraMetric()
        m.name = key
        m.type = m.STRING
        m.string_value = value
        payload.metric.extend([m])

    return payload.SerializeToString()