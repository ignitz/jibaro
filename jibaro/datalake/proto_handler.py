from pyspark.sql import types
from jibaro.datalake import proto_parser
import json


def parse_protofile(filepath) -> dict:
    # root_obj['messages']['Envelope']
    return json.loads(proto_parser.serialize2json_from_file(filepath))


def typeFor(descriptor, obj, messages=[]):
    try:
        return {
            'string': types.StringType(),
            'int32': types.IntegerType(),
            'int64': types.IntegerType(),
            'double': types.DoubleType(),
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
