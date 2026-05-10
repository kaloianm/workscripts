"""
Module providing common functionality for the workscripts set of scripts.
"""

import asyncio
import datetime
import logging
import sys

import aiofiles
import bson
from bson.objectid import ObjectId


class CToolsException(Exception):
    """
    Exception which serves as a common base for all exceptions thrown by the workscripts suite.
    """

    def __init__(self, *args, **kwargs):
        super(CToolsException, self).__init__(*args, **kwargs)


def yes_no(answer):
    """
    Function for a Yes/No result based on the answer provided as an argument
    """
    yes = set(['yes', 'y', 'y'])
    no = set(['no', 'n', ''])

    while True:
        choice = input(answer + '\nProceed (yes/NO)? ').lower()
        if choice in yes:
            return
        elif choice in no:
            raise KeyboardInterrupt('User canceled')
        else:
            print("Please respond with 'yes' or 'no'\n")


def exe_name(name):
    """
    Abstracts constructing the name of an executable on POSIX vs Windows platforms
    """
    if (sys.platform == 'win32'):
        return name + '.exe'
    return name


async def async_start_shell_command(command, logging_prefix):
    """
    Asynchronously starts a shell command and logs its stdin/stderr to the logging subsystem.
    """
    logging.info(f'[{logging_prefix}]: {command}')

    async with aiofiles.tempfile.TemporaryFile() as temp_file:
        command_shell_process = await asyncio.create_subprocess_shell(command, stdout=temp_file,
                                                                      stderr=temp_file)
        await command_shell_process.wait()

        await temp_file.seek(0)
        output_lines = []
        async for line in temp_file:
            stripped_line = line.decode('ascii').replace('\n', '')
            logging.info(f'[{logging_prefix}]: {stripped_line}')
            output_lines.append(stripped_line)

        if command_shell_process.returncode != 0:
            tail = '\n'.join(output_lines[-20:])
            raise CToolsException(
                f'[{logging_prefix}]: Command failed with code {command_shell_process.returncode}.'
                f' Command: {command}\nOutput (last 20 lines):\n{tail}')


class ShardCollectionUtil:
    """
    Utility class to generate the documents for manually sharding a collection through a process
    external to the core server. Does not perform any modifications to the cluster itself.
    """

    def __init__(self, ns, uuid, shard_key, unique, fcv):
        self.ns = ns
        self.uuid = uuid
        self.shard_key = shard_key
        self.unique = unique
        self.fcv = fcv

        self.shard_key_is_string = (self.fcv <= '4.2')

        self.epoch = bson.objectid.ObjectId()
        self.creation_time = datetime.datetime.now()
        self.timestamp = bson.timestamp.Timestamp(self.creation_time, 1)

        logging.info(f'''Sharding an existing collection {self.ns} with the following parameters:
                            uuid: {self.uuid}
                            shard_key: {self.shard_key}
                            unique: {self.unique}
        ''')

    # Accepts an array of tuples which must contain exactly the following fields:
    #    min, max, shard
    # AND MUST be sorted according to range['min']
    def generate_config_chunks(self, chunks):

        def make_chunk_id(i):
            if self.shard_key_is_string:
                return f'shard-key-{self.ns}-{str(i).zfill(8)}'
            else:
                return ObjectId()

        chunk_idx = 0
        for c in chunks:
            chunk_obj = {
                '_id': make_chunk_id(chunk_idx),
                'min': c['min'],
                'max': c['max'],
                'shard': c['shard'],
                'lastmod': bson.timestamp.Timestamp(1, chunk_idx),
            }

            if self.fcv >= '5.0':
                chunk_obj.update({'uuid': self.uuid})
            else:
                chunk_obj.update({
                    'ns': self.ns,
                    'lastmodEpoch': self.epoch,
                })

            chunk_idx += 1
            yield chunk_obj

    def generate_collection_entry(self):
        coll_obj = {
            '_id': self.ns,
            'lastmodEpoch': self.epoch,
            'lastmod': self.creation_time,
            'key': self.shard_key,
            'unique': self.unique,
            'uuid': self.uuid
        }

        if self.fcv >= '5.0':
            coll_obj.update({'timestamp': self.timestamp})
        else:
            coll_obj.update({'dropped': False})

        return coll_obj
