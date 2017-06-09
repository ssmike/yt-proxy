#!/usr/bin/env python2
from __future__ import print_function
import yt.yson as yson
import json
import yt.wrapper as yt
import sys
import os
import time
import uuid
from subprocess import call
from yt.wrapper.transaction_commands import *

PATH="//atom"
raw_config = open(os.environ['YT_DRIVER_CONFIG_PATH']).read()
yt.config = yson.loads(raw_config)

TABLE_PATH="//table"

def yt_key(x):
    return {0:2, 1:21, 2:31}[x]

def wait_for_yt():
    toBreak = False
    while not toBreak:
        toBreak = True
        try:
            yt.get('/')
        except Exception as e:
            toBreak = False
            time.sleep(2)

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def init_table():
    repeat = False
    while repeat:
        try:
            yt.insert_rows(TABLE_PATH, [dict(key=yt_key(i), value=1)
                                             for i in range(3)])
        except Exception as e:
            repeat = True
            eprint(e)


def mount_table():
    try:
        if yt.get("//sys/tablet_cells/@count") == 0:
            for i in range(3):
                yt.create("tablet_cell", attributes={"replication_factor": 3,
                                                     "read_quorum":2,
                                                     "write_quorum":2})
        yt.remove(TABLE_PATH, force=True)
        schema = yson.loads("<strict=%true; unique_keys=%true>[{name=key;\
                                type=int64; sort_order=ascending}; {name=value; type=int64}]")
        yt.create_table(TABLE_PATH, attributes={"dynamic": True, "schema": schema})
        yt.reshard_table(TABLE_PATH, pivot_keys=[[], [20], [30]])
    except Exception as e:
        eprint(e)
    while yt.get(TABLE_PATH + "/@tablet_state") != "mounted":
        try:
            yt.mount_table(TABLE_PATH)
        except Exception as e:
            eprint(e)
    init_table()

CURRENT_TRANSACTION = None
TO_CLEAN = False

def consume_input():
    raw_message = raw_input()
    eprint(raw_message)
    message = json.loads(raw_message)
    op = message["f"]
    op_val = None
    if "value" in message:
        op_val = message["value"]
    return message, op, op_val


def answer(message):
    print(json.dumps(message))
    sys.stdout.flush()

def handlemessage():
    message, op, op_val = consume_input()
    try:
        if op == "wait-for-yt":
            wait_for_yt()
        elif op == "read":
            val = int(yt.get(PATH))
            message["value"] = val
            message["type"] = "ok"
        elif op == "write":
            message["type"] = "info"
            yt.set(PATH, message["value"])
            message["type"] = "ok"
        elif op == "read-and-lock":
            with yt.Transaction(type='tablet', sticky=True):
                row = next(yt.lookup_rows(TABLE_PATH, [dict(key=yt_key(op_val[0]))]))
                op_val[1] = row['value']
                message["type"] = "ok"
                answer(message)
                message, op, op_val = consume_input()
                assert(op == "write-and-unlock")
                yt.insert_rows(TABLE_PATH, [dict(key=yt_key(op_val[0]), value=op_val[1])])
            message["type"] = "ok"
        elif op == "mount-table":
            mount_table()
        elif op == "terminate":
            answer(message)
            sys.exit(0)
        else:
            message["type"] = "fail"
    except Exception as e:
        my_id = uuid.uuid4()
        eprint(my_id)
        eprint(e)
        if op in ["write", "write-and-unlock"]:
            message["type"] = "info"
        else:
            message["type"] = "fail"
        message["error"] = str(my_id)
    answer(message)


if __name__ == "__main__":
    while True:
        handlemessage()
