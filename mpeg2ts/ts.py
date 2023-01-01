#!/usr/bin/env python3

PACKET_SIZE = 188
HEADER_SIZE = 4
SYNC_BYTE = b'\x47'
STUFFING_BYTE = b'\xff'
PCR_CYCLE = 2 ** 33
HZ = 90000

def transport_error_indicator(packet):
  return (packet[1] & 0x80) != 0

def payload_unit_start_indicator(packet):
  return (packet[1] & 0x40) != 0

def transport_priority(packet):
  return (packet[1] & 0x20) != 0

def pid(packet):
  return ((packet[1] & 0x1F) << 8) | packet[2]

def transport_scrambling_control(packet):
  return (packet[3] & 0xC0) >> 6

def has_adaptation_field(packet):
  return (packet[3] & 0x20) != 0

def has_payload(packet):
  return (packet[3] & 0x10) != 0

def continuity_counter(packet):
  return packet[3] & 0x0F

def adaptation_field_length(packet):
  return packet[4] if has_adaptation_field(packet) else 0

def pointer_field(packet):
  return packet[HEADER_SIZE + (1 + adaptation_field_length(packet) if has_adaptation_field(packet) else 0)]

def has_pcr(packet):
  return has_adaptation_field(packet) and adaptation_field_length(packet) > 0 and (packet[HEADER_SIZE + 1] & 0x10) != 0

def pcr(packet):
  if not has_pcr(packet): return None

  pcr_base = 0
  pcr_base = (pcr_base << 8) | ((packet[HEADER_SIZE + 1 + 1] & 0xFF) >> 0)
  pcr_base = (pcr_base << 8) | ((packet[HEADER_SIZE + 1 + 2] & 0xFF) >> 0)
  pcr_base = (pcr_base << 8) | ((packet[HEADER_SIZE + 1 + 3] & 0xFF) >> 0)
  pcr_base = (pcr_base << 8) | ((packet[HEADER_SIZE + 1 + 4] & 0xFF) >> 0)
  pcr_base = (pcr_base << 1) | ((packet[HEADER_SIZE + 1 + 5] & 0x80) >> 7)
  return pcr_base
