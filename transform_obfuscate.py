#!/usr/bin/env python3

import argparse
import io
import sys
import json
import logging
import hashlib
import singer
from oauth2client import tools

try:
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument('-c', '--config', help='Config file', required=True)
    flags = parser.parse_args()

except ImportError:
    flags = None

logger = singer.get_logger()

def obfuscate_record(record, obfuscate_fields):
    for key, val in record.items():
        if key in obfuscate_fields: 
            if isinstance(val, dict):
                record[key] = obfuscate_record(record[key], obfuscate_fields[key]) #call recursively
            else:
                if obfuscate_fields[key] == True:
                    record[key] = hashlib.sha256(val.encode()).hexdigest()
    return record

def transform_lines(lines, obfuscate_fields):
    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise
        if isinstance(msg, singer.RecordMessage):
            obfuscated = obfuscate_record(msg.record, obfuscate_fields)
            singer.write_records(msg.stream, [obfuscated])
        elif isinstance(msg, singer.StateMessage):
            singer.write_state(msg.value)
        elif isinstance(msg, singer.SchemaMessage):
            singer.write_schema(msg.stream, msg.schema, msg.key_properties)
        else:
            raise Exception("Unrecognized message {}".format(msg))

def main():
    with open(flags.config) as input:
        config = json.load(input)
    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    transform_lines(input, config["obfuscate_fields"])

if __name__ == '__main__':
    main()
