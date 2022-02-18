# Kafka-slurp

This tool can dump the content of a single topic into files, stored as JSON lines files containing the topic, partition, topic, timestamp, key and value of each record (key and value as base64 encoded strings).

A directory is created for the topic, containing subdirs for each partition, in which the actual files are stored with filenames containing the first and last offset of the contents, e.g. `${data-dir}/${topic-name}/${partition-number}/${first-offset}-${last-offset}.jsonl.xz`.

The uncompressed files are first stored in temporary files created in `${data-dir}/.tmp`, then compressed using `xz` in the same directory, and finally moved into place following the above scheme. `.tmp` should _not_ be put on a separate filesystem, since that will break the atomic move that some filesystems provide.


## Usage

```
$ nix-shell # or fetch the dependencies manually
$ ./kafka-slurp.py --help
usage: kafka-slurp.py [-h] --brokers BROKERS --topic TOPIC --data-dir DATA_DIR
                      [--record-limit RECORD_LIMIT]
                      [--read-timeout READ_TIMEOUT]

Dump Kafka records into files.

optional arguments:
  -h, --help            show this help message and exit
  --brokers BROKERS     Brokers to bootstrap from
  --topic TOPIC         Topic to dump
  --data-dir DATA_DIR   Directory to store data in
  --record-limit RECORD_LIMIT
                        Maximum number of records pr file
  --read-timeout READ_TIMEOUT
                        Maximum duration (in minutes) to read data before
                        starting a new file
```


## Development

Project contains a buildable test to verify basic functionality. 
Build and run with:

```
nix-build test.nix -A test
```
A successful run reports:
(finished: waiting for file ‘/root/out/testtopic/0/0-0.jsonl.xz‘, in X.XX seconds)
