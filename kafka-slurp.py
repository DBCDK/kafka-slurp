#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer, TopicPartition
from datetime import datetime, timezone
import argparse
import base64
import glob
import json
import multiprocessing
import os
import pathlib
import queue
import re
import signal
import subprocess
import sys
import tempfile
import time
import threading


# This tool dumps data from kafka topics to files in a directory structure like
# data-dir/topic/partition/fromOffset-toOffset.jsonl.xz


from_to_file_regex = re.compile(r'(?P<from>\d+)-(?P<to>\d+)\.jsonl\.xz') # TODO: Don't hardcode suffix here

# raised when the worker process is asked to terminate
class InterruptedException(Exception):
    pass

def parse_args():
    argparser = argparse.ArgumentParser(description='Dump Kafka records into files.')
    argparser.add_argument('--brokers', type=str, help='Brokers to bootstrap from', required=True)
    argparser.add_argument('--topic', type=str, help='Topic to dump', required=True)
    argparser.add_argument('--data-dir', type=str, help='Directory to store data in', required=True)
    argparser.add_argument('--record-limit', type=int, help='Maximum number of records pr file', default=10000)
    argparser.add_argument('--read-timeout', type=int, help='Maximum duration (in minutes) to read data before starting a new file', default=10)
    return argparser.parse_args()


def log(**attrs):
    meta = dict(
        timestamp = datetime.now(timezone.utc).astimezone().isoformat('T'),
        app = 'kafka-slurp'
    )
    print(json.dumps(dict(**meta, **attrs)), flush = True)


def log_duration(sli = None, **attrs):
    def decorator_log_duration(func):
        def decorated(*args, **kwargs):
            nonlocal sli

            t_start = datetime.now()
            result = func(*args, **kwargs)
            t_end = datetime.now()

            if sli is None: sli = func.__name__

            log(**dict(
                sli = sli,
                durationSec = (t_end - t_start).total_seconds(),
                **attrs
            ))

            return result

        return decorated
    return decorator_log_duration




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


# Find the next segment to consume, based on the existing slices of data as
# stored in the partition_dir. Files are named "fromOffset-toOffset.${ext}".
# Returning None indicates that the consumer should start from the beginning.
# Otherwise (fromOffset, toOffset) will be returned, both offsets to be included
# in the backup.
@log_duration()
def get_next_segment(logev, partition_dir, record_limit):
    existing_snapshots = glob.glob(os.path.join(partition_dir, "*.jsonl.xz"))
    basenames = map(os.path.basename, existing_snapshots)
    latest = None
    # loop over all files, parsing the filename, and keeping the largets to-offset
    for b in sorted(basenames):
        from_ = int(from_to_file_regex.match(b).group("from"))
        to = int(from_to_file_regex.match(b).group("to"))

        # test for missing segments
        if latest is not None and from_ > latest+1:
            # one offset after the previous, until one prior to the next
            first_missing = latest+1
            last_missing = from_-1
            logev(message=f"detected missing segment from {first_missing} to {last_missing}", event="found-missing-segment", start=first_missing, end=last_missing)
            # avoid creating segments larger than usual, by checking the size of the missing segment,
            # and return a smaller segment if required.
            segment_size = last_missing - first_missing + 1 # uh, all the possible off-by-one errors...
            if segment_size > record_limit:
                return (first_missing, first_missing+record_limit-1)
            else:
                return (first_missing, last_missing)

        if latest is None or to > latest:
            latest = to

    if latest is None:
        return None
    else:
        return (latest+1, latest+record_limit)


# Keep reading data from topic,partition until the end of time. Read chunks of
# at most args.record_limit but never read data for more than args.read_timeout.
# The former ensures that files wont grow unbounded, while the latter is mostly
# useful after the backup has caught up. At that point the data rate might be
# low, but by limiting the time window data is read, data will still be stored
# occasionally, instead of waiting a potentially very long time for
# args.record_limit records to show up.
@log_duration()
def compress(fname, suffix):
    # xz without the flag `-k` will delete the original file
    subprocess.run(["xz", "-3", "-T", "1", "--suffix", suffix, fname], capture_output=True, check=True)


@log_duration()
def move_into_place(source, dest):
    # Do our best to ensure an atomic move -- this should also fail if renaming
    # across filesystems
    os.rename(source, dest)

@log_duration()
def load_segment(tmp_dir, consumer, from_offset, to_offset):
    record_limit = to_offset - from_offset + 1
    # Data is first written to a temporary file, then compressed, and
    # lastly moved to its final destination
    (fd, temporary_fname) = tempfile.mkstemp(dir=tmp_dir)
    last_offset = None
    records_read = 0
    with open(fd, 'w') as f:
        for msg in consumer:
            if records_read >= record_limit: break
            x = serialize_record(msg)
            f.write(x + '\n')
            records_read += 1

            # When snapshotting data for the first time, the first offset isn't know, so we use the offset from the first record we see
            if from_offset is None:
                from_offset = msg.offset

            last_offset = msg.offset

    return (records_read, from_offset, last_offset, temporary_fname)


def run(log_queue, topic, partition, tmp_dir):
    def logev(**attrs):
        log_queue.put(dict(topic=topic, partition=partition, **attrs))


    def sigterm_handler(signum, frame):
        raise InterruptedException(f"got signal {signum}")

    signal.signal(signal.SIGTERM, sigterm_handler)

    # path to store partition data to
    partition_dir = os.path.join(args.data_dir, str(topic), str(partition))
    pathlib.Path(partition_dir).mkdir(parents=True, exist_ok=True)

    # consumer isn't thread safe, do not reuse
    consumer = KafkaConsumer(bootstrap_servers=args.brokers, consumer_timeout_ms=1000*60*args.read_timeout)
    consumer.assign([TopicPartition(topic, partition)])

    while True:
        (from_offset, to_offset) = get_next_segment(logev, partition_dir, args.record_limit)

        # Start from the beginning if None was return instead of an actual offset
        if from_offset == None:
            logev(message=f"starting from beginning, bucket_size={args.record_limit}", state="starting", bucket_size=args.record_limit, start=from_offset, end=to_offset)
            consumer.seek_to_beginning(TopicPartition(topic, partition))
        else:
            logev(message=f"resuming from {from_offset}, bucket_size={args.record_limit}", state="resuming", bucket_size=args.record_limit, start=from_offset, end=to_offset)
            consumer.seek(TopicPartition(topic, partition), from_offset)

        try:
            (records_read, from_offset, last_offset, temporary_fname) = load_segment(tmp_dir, consumer, from_offset, to_offset)

            if records_read > 0:
                # do the actual compressing/moving of data dance
                suffix = ".xz"
                logev(message=f"compressing {temporary_fname}")
                compress(temporary_fname, suffix)

                target_file = os.path.join(partition_dir, f"{from_offset}-{last_offset}.jsonl{suffix}")

                logev(message=f"moving {temporary_fname}{suffix} to {target_file}")
                move_into_place(f"{temporary_fname}{suffix}", target_file)
            else:
                # No new records were read -> perform cleanup
                logev(message=f"no new records")
                os.unlink(temporary_fname)

        except InterruptedException as e:
            logev(message="interrupted", state="interrupted")
            # Parent process will handle cleanup
            break

        except Exception as e:
            pass


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
        # Force fetch of metadata. This is required to make the
        # partitions_for_topic-call return anything.
        consumer.topics()
        partitions = consumer.partitions_for_topic(args.topic)
        consumer.close()

        if partitions is None:
            # partitions_for_topic(..) returns None if topic wasn't foudn
            log(message=f"topic {args.topic} not found", thread="main")
            exit(1)
        else:
            log(message=f"detected {len(partitions)} partitions", thread="main")

        # spawn consumer process for each partition
        processes = []
        log_queue = multiprocessing.Queue(maxsize=0) # maxsize <= 0 -> unlimited queue size

        log_stop_event = threading.Event()


        def sigterm_handler(signum, frame):
            log(message=f"got signal {signum}: Terminating children..", thread="main")
            for process in processes:
                process.terminate()

            log_stop_event.set()

        signal.signal(signal.SIGTERM, sigterm_handler)
        signal.signal(signal.SIGINT, sigterm_handler)

        for partition in partitions:
            p = multiprocessing.Process(target=run, args=(log_queue, args.topic, partition, tmp_dir), name=f"partition {partition}")
            processes.append(p)
            p.start()

        # this boolean expression might be iffy:
        # - run the loop until the stop_even is set, but allow the queue to empty before stopping
        while (not log_queue.empty) or (not log_stop_event.is_set()):
            try:
                e = log_queue.get(timeout=1)
                log(**e)
            except queue.Empty:
                pass

        # wait for all processes to complete
        for p in processes:
            p.join()
            log(message=f"Worker \"{p.name}\" completed", thread="main")

        log(message=f"All workers completed", thread="main")
