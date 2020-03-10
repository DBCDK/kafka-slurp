#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer, TopicPartition
from multiprocessing import Process
import argparse
import base64
import glob
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile


# This tool dumps data from kafka topics to files in a directory structure like
# data-dir/topic/partition/fromOffset-toOffset.jsonl.xz


from_to_file_regex = re.compile(r'(?P<from>\d+)-(?P<to>\d+)\.jsonl\.xz') # TODO: Don't hardcode suffix here


def parse_args():
    argparser = argparse.ArgumentParser(description='Dump Kafka records into files.')
    argparser.add_argument('--brokers', type=str, help='Brokers to bootstrap from', required=True)
    argparser.add_argument('--topic', type=str, help='Topic to dump', required=True)
    argparser.add_argument('--data-dir', type=str, help='Directory to store data in', required=True)
    argparser.add_argument('--record-limit', type=int, help='Maximum number of records pr file', default=10000)
    argparser.add_argument('--read-timeout', type=int, help='Maximum duration (in minutes) to read data before starting a new file', default=10)
    return argparser.parse_args()

# Serialize a record as JSON, containing the topic, partition and offset as-is,
# while base64 encoding the key and value, since they can contain arbitrary bytes.
def serialize_record(consumerRecord):
    # key is optional, and it's impossible to base64 encode NoneType
    key = None
    if consumerRecord.key is not None:
        key = base64.b64encode(consumerRecord.key).decode("utf-8")

    return json.dumps(
        { 'topic': consumerRecord.topic
        , 'partition': consumerRecord.partition
        , 'offset': consumerRecord.offset
        , 'key': key
        , 'timestamp': consumerRecord.timestamp
        , 'value': base64.b64encode(consumerRecord.value).decode("utf-8")
        })


# Find the next offset to consume from, based on the existing slices of data as
# stored in the partition_dir. Files are named "fromOffset-toOffset.${ext}".
# Returning None indicates that the consumer should start from the beginning
# instead of from an offset.
def get_next_offset(partition_dir):
    existing_snapshots = glob.glob(os.path.join(partition_dir, "*.jsonl.xz"))
    basenames = map(os.path.basename, existing_snapshots)
    latest = None
    # loop over all files, parsing the filename, and keeping the largets to-offset
    for b in basenames:
        to = int(from_to_file_regex.match(b).group("to"))
        if latest is None or to > latest:
            latest = to

    if latest is None:
        return None
    else:
        return latest+1


# Keep reading data from topic,partition until the end of time. Read chunks of
# at most args.record_limit but never read data for more than args.read_timeout.
# The former ensures that files wont grow unbounded, while the latter is mostly
# useful after the backup has caught up. At that point the data rate might be
# low, but by limiting the time window data is read, data will still be stored
# occasionally, instead of waiting a potentially very long time for
# args.record_limit records to show up.

def run(topic, partition, tmp_dir):
    msg_prefix = f"{topic}/{partition}: "
    # path to store partition data to
    partition_dir = os.path.join(args.data_dir, str(topic), str(partition))
    pathlib.Path(partition_dir).mkdir(parents=True, exist_ok=True)

    # consumer isn't thread safe, do not reuse
    consumer = KafkaConsumer(bootstrap_servers=args.brokers, consumer_timeout_ms=1000*60*args.read_timeout)
    consumer.assign([TopicPartition(topic, partition)])

    while True:
        from_offset = get_next_offset(partition_dir)

        # Start from the beginning if None was return instead of an actual offset
        if from_offset == None:
            print(f"{msg_prefix}starting from beginning, bucket_size={args.record_limit}")
            consumer.seek_to_beginning(TopicPartition(topic, partition))
        else:
            print(f"{topic}/{partition}: resuming from {from_offset}, bucket_size={args.record_limit}")
            consumer.seek(TopicPartition(topic, partition), from_offset)

        try:
            # Data is first written to a temporary file, then compressed, and
            # lastly moved to its final destination
            (fd, temporary_fname) = tempfile.mkstemp(dir=tmp_dir)
            last_offset = None
            records_read = 0
            with open(fd, 'w') as f:
                for msg in consumer:
                    if records_read >= args.record_limit: break
                    x = serialize_record(msg)
                    f.write(x + '\n')
                    records_read += 1

                    # When snapshotting data for the first time, the first offset isn't know, so we use the offset from the first record we see
                    if from_offset is None:
                        from_offset = msg.offset

                    last_offset = msg.offset

            if records_read > 0:
                # do the actual compressing/moving of data dance
                suffix = ".xz"
                print(f"{msg_prefix}compressing {temporary_fname}")
                # xz without the flag `-k` will delete the original file
                subprocess.run(["xz", "-3", "--suffix", suffix, temporary_fname], capture_output=True, check=True)

                target_file = os.path.join(partition_dir, f"{from_offset}-{last_offset}.jsonl{suffix}")

                print(f"{msg_prefix}moving {temporary_fname}{suffix} to {target_file}")
                # Do our best to ensure an atomic move -- this should also fail if renaming
                # across filesystems
                os.rename(f"{temporary_fname}{suffix}", target_file)
            else:
                # No new records were read -> perform cleanup
                print(f"{msg_prefix}no new records")
                os.unlink(temporary_fname)

        except Exception as e:
            print(e)


if __name__ == "__main__":
    args = parse_args()
    # Keep tmp-files on the same filesystem as the real data, to avoid
    # copying files across filesystems. The files can also get rather large,
    # so using a tmpfs or similar is not advisable.
    tmp_base_dir = os.path.join(args.data_dir, ".tmp")
    pathlib.Path(tmp_base_dir).mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(dir=tmp_base_dir) as tmp_dir:
        # consumer isn't thread safe, do not reuse
        consumer = KafkaConsumer(bootstrap_servers=args.brokers)
        partitions = consumer.partitions_for_topic(args.topic)
        consumer.close()

        if partitions is None:
            # partitions_for_topic(..) returns None if topic wasn't foudn
            print(f"topic {args.topic} not found")
            exit(1)
        else:
            print(f"detected {len(partitions)} partitions")

        # spawn consumer process for each partition
        processes = []
        for partition in partitions:
            p = Process(target=run, args=(args.topic, partition, tmp_dir))
            processes.append(p)
            p.start()

        # wait for all processes to complete
        for p in processes: p.join()
