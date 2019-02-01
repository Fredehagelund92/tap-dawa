
import os
import json
import singer

from tap_dawa.streams import STREAMS


LOGGER = singer.get_logger()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        if filename.replace('.json', '') in STREAMS:
            path = get_abs_path('schemas') + '/' + filename
            file_raw = filename.replace('.json', '')
            with open(path) as file:
                schemas[file_raw] = json.load(file)

    return schemas

def discover(client):
    streams = []

    for stream in STREAMS.values():
        stream = stream(client)
        LOGGER.info('Loading schema for %s', stream.name)
        schema = singer.resolve_schema_references(stream.load_schema())
        streams.append({
            'stream': stream.name,
            'tap_stream_id': stream.name,
            'schema': schema,
            'metadata': stream.load_metadata()
        })

    return streams

