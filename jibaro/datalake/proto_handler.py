from pyspark.sql import types
from jibaro.datalake import proto_parser
import json
from google.protobuf.descriptor import FieldDescriptor
from functools import partial
from google.protobuf.json_format import MessageToJson


_PROTOBUF_INT64_FIELDS = [
    FieldDescriptor.TYPE_INT64,
    FieldDescriptor.TYPE_UINT64,
    FieldDescriptor.TYPE_FIXED64,
    FieldDescriptor.TYPE_SFIXED64,
    FieldDescriptor.TYPE_SINT64,
]


def _mark_int64_fields_for_proto_maps(proto_map, value_field_type):
    """Converts a proto map to JSON, preserving only int64-related fields."""
    json_dict = {}
    for key, value in proto_map.items():
        # The value of a protobuf map can only be a scalar or a message (not a map or repeated
        # field).
        if value_field_type == FieldDescriptor.TYPE_MESSAGE:
            json_dict[key] = _mark_int64_fields(value)
        elif value_field_type in _PROTOBUF_INT64_FIELDS:
            json_dict[key] = int(value)
        elif isinstance(key, int):
            json_dict[key] = value
    return json_dict


def _mark_int64_fields(proto_message):
    """Converts a proto message to JSON, preserving only int64-related fields."""
    json_dict = {}
    for field, value in proto_message.ListFields():
        if (
            # These three conditions check if this field is a protobuf map.
            # See the official implementation: https://bit.ly/3EMx1rl
            field.type == FieldDescriptor.TYPE_MESSAGE
            and field.message_type.has_options
            and field.message_type.GetOptions().map_entry
        ):
            # Deal with proto map fields separately in another function.
            json_dict[field.name] = _mark_int64_fields_for_proto_maps(
                value, field.message_type.fields_by_name["value"].type
            )
            continue

        if field.type == FieldDescriptor.TYPE_MESSAGE:
            ftype = partial(_mark_int64_fields)
        elif field.type in _PROTOBUF_INT64_FIELDS:
            ftype = int
        else:
            # Skip all non-int64 fields.
            continue

        json_dict[field.name] = (
            [ftype(v) for v in value]
            if field.label == FieldDescriptor.LABEL_REPEATED
            else ftype(value)
        )
    return json_dict


def _merge_json_dicts(from_dict, to_dict):
    """Merges the json elements of from_dict into to_dict. Only works for json dicts
    converted from proto messages
    """
    for key, value in from_dict.items():
        if isinstance(key, int) and str(key) in to_dict:
            # When the key (i.e. the proto field name) is an integer, it must be a proto map field
            # with integer as the key. For example:
            # from_dict is {'field_map': {1: '2', 3: '4'}}
            # to_dict is {'field_map': {'1': '2', '3': '4'}}
            # So we need to replace the str keys with int keys in to_dict.
            to_dict[key] = to_dict[str(key)]
            del to_dict[str(key)]

        if key not in to_dict:
            continue

        if isinstance(value, dict):
            _merge_json_dicts(from_dict[key], to_dict[key])
        elif isinstance(value, list):
            for i, v in enumerate(value):
                if isinstance(v, dict):
                    _merge_json_dicts(v, to_dict[key][i])
                else:
                    to_dict[key][i] = v
        else:
            to_dict[key] = from_dict[key]
    return to_dict


def _override_protobuf_integer_types(proto, message_json: str) -> str:
    message_dict = json.loads(message_json)
    # We convert this proto message into a JSON dict where only int64 proto fields
    # are preserved, and they are treated as JSON numbers, not strings.
    json_dict_with_int64_fields_only = _mark_int64_fields(proto)
    # By merging these two JSON dicts, we end up with a JSON dict where int64 proto fields are not
    # converted to JSON strings. Int64 keys in proto maps will always be converted to JSON strings
    # because JSON doesn't support non-string keys.
    json_dict_with_int64_as_numbers = _merge_json_dicts(
        json_dict_with_int64_fields_only, message_dict
    )
    return json.dumps(json_dict_with_int64_as_numbers)


def convert_message_to_json(message):
    """Converts a message to JSON, using snake_case for field names.

    Solution from https://github.com/mlflow/mlflow/pull/5010
    """
    # TODO: Check Scala toJSON used by databricks
    message_json = MessageToJson(
        message, preserving_proto_field_name=True, ensure_ascii=False)
    message_json = _override_protobuf_integer_types(
        message, message_json)
    return message_json


def parse_protofile(filepath) -> dict:
    # root_obj['messages']['Envelope']
    return json.loads(proto_parser.serialize2json_from_file(filepath))


def typeFor(descriptor, obj, messages=[]):
    # https://developers.google.com/protocol-buffers/docs/proto3#scalar
    # https://developers.google.com/protocol-buffers/docs/proto3#json
    try:
        return {
            'double': types.DoubleType(),
            'float': types.FloatType(),
            'int32': types.IntegerType(),
            'int64': types.LongType(),
            'uint32': types.IntegerType(),
            'uint64': types.LongType(),
            'sint32': types.IntegerType(),
            'sint64': types.LongType(),
            'fixed32': types.IntegerType(),
            'fixed64': types.LongType(),
            'sfixed32': types.IntegerType(),
            'sfixed64': types.LongType(),
            'bool': types.BooleanType(),
            'string': types.StringType(),
            'bytes': types.ByteType(),
        }[descriptor]
    except KeyError as error:
        import sys
        if descriptor not in messages:
            print(f'Unknow type {descriptor}', file=sys.stderr)
            raise error
        else:
            return types.StructType(schemaFor(obj['messages'][descriptor]))


def schemaFor(obj):
    struct_fields = []
    OBJ_NAMES = list(obj['messages'])

    obj['fields'].sort(key=lambda x: x['number'])
    for d in obj['fields']:
        name = d['name']
        type_name = d['type']
        struct_fields.append(
            types.StructField(
                name=name,
                dataType=typeFor(type_name, obj, messages=OBJ_NAMES),
                nullable=True
            )
        )
    return types.StructType(struct_fields)
