#!/usr/bin/env python3
import os
import sys
import json
import singer
from singer import utils
from singer import metadata
from singer.catalog import Catalog, CatalogEntry

from singer import utils, metadata
import dateutil

from tap_dawa.client import Client
from tap_dawa.discover import discover
from tap_dawa.sync import sync_stream
from tap_dawa.sync import add_automatic_properties
from tap_dawa.streams import STREAMS


import argparse

REQUIRED_CONFIG_KEYS = [
]

LOGGER = singer.get_logger()


# def transform_datetime_string(dts):
#     parsed_dt = dateutil.parser.parse(dts)
#     if parsed_dt.tzinfo is None:
#         parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
#     else:
#         parsed_dt = parsed_dt.astimezone(timezone.utc)
#     return singer.strftime(parsed_dt)

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_streams_to_sync(catalog):
    streams = []
    for stream in STREAMS:
        selected_stream = next((s for s in catalog.streams if s.tap_stream_id == stream), None)
        if selected_stream and selected_stream.schema.selected:
            streams.append(selected_stream.tap_stream_id)
    return streams

def populate_class_schemas(catalog, selected_stream_names):
    for stream in catalog.streams:
        if stream.tap_stream_id in selected_stream_names:
            STREAMS[stream.tap_stream_id].stream = stream

def do_discover(client):
    LOGGER.info('Loading schemas')
    catalog = {"streams": discover(client)}
    json.dump(catalog, sys.stdout, indent=4)


def do_sync(client, catalog, state):
    selected_stream_names = get_streams_to_sync(catalog)
    populate_class_schemas(catalog, selected_stream_names)


    if len(selected_stream_names) < 1:
        LOGGER.info("No Streams selected, please check that you have a schema selected in your catalog")
        return

    for stream in catalog.streams:
        stream = add_automatic_properties(stream)

        stream_name = stream.tap_stream_id
        mdata = metadata.to_map(stream.metadata)

        if stream_name not in selected_stream_names:
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream.schema.to_dict(), key_properties)
        LOGGER.info("%s: Starting sync", stream_name)
        instance = STREAMS[stream_name](client)
        instance.stream = stream
        counter_value = sync_stream(state, instance)
        singer.write_state(state)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    singer.write_state(state)
    LOGGER.info("Finished sync")


@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    client = Client()

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        do_discover(client)
    elif args.catalog:
        do_sync(client, args.catalog, args.state)

    else:
        LOGGER.info("No catalog were selected")

if __name__ == "__main__":
    main()
