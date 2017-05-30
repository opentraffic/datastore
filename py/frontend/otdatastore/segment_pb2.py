# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: segment.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='segment.proto',
  package='io.opentraffic.datastore',
  serialized_pb=_b('\n\rsegment.proto\x12\x18io.opentraffic.datastore\"p\n\nTimeBucket\x12?\n\x04size\x18\x01 \x01(\x0e\x32).io.opentraffic.datastore.TimeBucket.Size:\x06HOURLY\x12\r\n\x05index\x18\x02 \x01(\x03\"\x12\n\x04Size\x12\n\n\x06HOURLY\x10\x00\"\x8d\x02\n\x0bMeasurement\x12\x41\n\x0cvehicle_type\x18\x01 \x01(\x0e\x32%.io.opentraffic.datastore.VehicleType:\x04\x41UTO\x12\x12\n\nsegment_id\x18\x02 \x01(\x06\x12&\n\x0fnext_segment_id\x18\x03 \x01(\x06:\r4398046511103\x12\x0e\n\x06length\x18\x04 \x01(\r\x12\x39\n\x0btime_bucket\x18\x05 \x01(\x0b\x32$.io.opentraffic.datastore.TimeBucket\x12\x10\n\x08\x64uration\x18\x06 \x01(\x05\x12\x10\n\x05\x63ount\x18\x07 \x01(\x05:\x01\x31\x12\x10\n\x08provider\x18\x08 \x01(\t*\x17\n\x0bVehicleType\x12\x08\n\x04\x41UTO\x10\x00')
)
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

_VEHICLETYPE = _descriptor.EnumDescriptor(
  name='VehicleType',
  full_name='io.opentraffic.datastore.VehicleType',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='AUTO', index=0, number=0,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=429,
  serialized_end=452,
)
_sym_db.RegisterEnumDescriptor(_VEHICLETYPE)

VehicleType = enum_type_wrapper.EnumTypeWrapper(_VEHICLETYPE)
AUTO = 0


_TIMEBUCKET_SIZE = _descriptor.EnumDescriptor(
  name='Size',
  full_name='io.opentraffic.datastore.TimeBucket.Size',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='HOURLY', index=0, number=0,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=137,
  serialized_end=155,
)
_sym_db.RegisterEnumDescriptor(_TIMEBUCKET_SIZE)


_TIMEBUCKET = _descriptor.Descriptor(
  name='TimeBucket',
  full_name='io.opentraffic.datastore.TimeBucket',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='size', full_name='io.opentraffic.datastore.TimeBucket.size', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=True, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='index', full_name='io.opentraffic.datastore.TimeBucket.index', index=1,
      number=2, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _TIMEBUCKET_SIZE,
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=43,
  serialized_end=155,
)


_MEASUREMENT = _descriptor.Descriptor(
  name='Measurement',
  full_name='io.opentraffic.datastore.Measurement',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='vehicle_type', full_name='io.opentraffic.datastore.Measurement.vehicle_type', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=True, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='segment_id', full_name='io.opentraffic.datastore.Measurement.segment_id', index=1,
      number=2, type=6, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='next_segment_id', full_name='io.opentraffic.datastore.Measurement.next_segment_id', index=2,
      number=3, type=6, cpp_type=4, label=1,
      has_default_value=True, default_value=4398046511103,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='length', full_name='io.opentraffic.datastore.Measurement.length', index=3,
      number=4, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='time_bucket', full_name='io.opentraffic.datastore.Measurement.time_bucket', index=4,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='duration', full_name='io.opentraffic.datastore.Measurement.duration', index=5,
      number=6, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='count', full_name='io.opentraffic.datastore.Measurement.count', index=6,
      number=7, type=5, cpp_type=1, label=1,
      has_default_value=True, default_value=1,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='provider', full_name='io.opentraffic.datastore.Measurement.provider', index=7,
      number=8, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=158,
  serialized_end=427,
)

_TIMEBUCKET.fields_by_name['size'].enum_type = _TIMEBUCKET_SIZE
_TIMEBUCKET_SIZE.containing_type = _TIMEBUCKET
_MEASUREMENT.fields_by_name['vehicle_type'].enum_type = _VEHICLETYPE
_MEASUREMENT.fields_by_name['time_bucket'].message_type = _TIMEBUCKET
DESCRIPTOR.message_types_by_name['TimeBucket'] = _TIMEBUCKET
DESCRIPTOR.message_types_by_name['Measurement'] = _MEASUREMENT
DESCRIPTOR.enum_types_by_name['VehicleType'] = _VEHICLETYPE

TimeBucket = _reflection.GeneratedProtocolMessageType('TimeBucket', (_message.Message,), dict(
  DESCRIPTOR = _TIMEBUCKET,
  __module__ = 'segment_pb2'
  # @@protoc_insertion_point(class_scope:io.opentraffic.datastore.TimeBucket)
  ))
_sym_db.RegisterMessage(TimeBucket)

Measurement = _reflection.GeneratedProtocolMessageType('Measurement', (_message.Message,), dict(
  DESCRIPTOR = _MEASUREMENT,
  __module__ = 'segment_pb2'
  # @@protoc_insertion_point(class_scope:io.opentraffic.datastore.Measurement)
  ))
_sym_db.RegisterMessage(Measurement)


# @@protoc_insertion_point(module_scope)
