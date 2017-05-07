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
            yt.insert_rows(TABLE_PATH, [dict(key=yt_key(i), value=1)
                                             for i in range(3)])
        except Exception as e:
            eprint(e)

def handlemessage():
    raw_message = raw_input()
    eprint(raw_message)
    message = json.loads(raw_message)
    op = message["f"]
    op_val = None
    if "value" in message:
        op_val = message["value"]
    try:
        if op == "wait-for-yt":
            wait_for_yt()
        elif op == "read":
            val = int(yt.get(PATH))
            message["value"] = val
            message["type"] = "ok"
        elif op == "write":
            yt.set(PATH, message["value"])
            message["type"] = "ok"
        elif op == "dyn-table-cas":
            from_key, to_key = map(yt_key, op_val["key"])
            from_val, to_val = op_val["val"]
            try:
                with yt.Transaction(type='tablet', sticky=True):
                    rows = list(yt.lookup_rows('//table', [dict(key=from_key)]))
                    if len(rows) == 0:
                        message["type"] = "fail"
                    else:
                        from_val = rows[0]['value']
                        op["ret"] = from_val
                        yt.insert_rows(TABLE_PATH, [dict(key=to_key,
                                                         value=(to_val + from_val)%5)])
                        message["type"] = "ok"
            except yt.YtResponseError as e:
                if e.contains_code(1700): #Transaction lock conflict
                    message["type"] = "fail"
                else:
                    raise e
        elif op == "dyn-table-write":
            yt.insert_rows(TABLE_PATH, [dict(key=yt_key(op_val["key"]), value=op_val["val"])])
            message["type"] = "ok"
        elif op == "dyn-table-read":
            rows = list(yt.lookup_rows('//table', [dict(key=yt_key(op_val["key"]))]))
            if len(rows) == 0:
                message["type"] = "fail"
            else:
                message["type"] = "ok"
                op_val["val"] = rows[0]["value"]
        elif op == "mount-table":
            mount_table()

    except Exception as e:
        my_id = uuid.uuid4()
        eprint(my_id)
        eprint(e)
        if op in ["write", "dyn-table-write", "dyn-table-cas"]:
            message["type"] = "info"
        else:
            message["type"] = "fail"
        message["error"] = str(my_id)
    print(json.dumps(message))
    sys.stdout.flush()
    if op == "terminate":
        sys.exit(0)


if __name__ == "__main__":
    while True:
        handlemessage()
