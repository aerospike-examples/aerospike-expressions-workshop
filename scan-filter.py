# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception
from aerospike_helpers import expressions as exp
from aerospike_helpers.expressions import arithmetic
from aerospike_helpers.operations import operations
import sys
import time

if options.set == "demo":
    options.set = "scan-filter"
config = {"hosts": [(options.host, options.port)]}
if options.alternate:
    config["use_services_alternate"] = True
try:
    client = aerospike.client(config).connect(options.username, options.password)
except exception.ClientError as e:
    print("failed to connect to the cluster with", config["hosts"])
    sys.exit(1)

client.truncate(options.namespace, options.set, 0)

input("\nAdd a batch of keys 0-9, even keys with TTL of 1 hour, odd ones with 1 minute")
policy = {"key": aerospike.POLICY_KEY_SEND}
for i in range(10):
    key = (options.namespace, options.set, i)
    if i % 2 == 0:
        meta = {"ttl": 3600}
    else:
        meta = {"ttl": 60}
    # if you get this error
    # exception.ForbiddenError: (22, 'AEROSPIKE_ERR_FAIL_FORBIDDEN', 'src/main/client/put.c', 115, False)
    # you need to set the configuration parameter allow-ttl-without-nsup=true
    # and the default-ttl configuration parameter to something greater than 1h
    client.put(key, {"num": i}, meta, policy)
print("Done")

input("\nPause 1s, add batch of keys 10-19 and scan for records older than 1s")
time.sleep(1)
print("Start")
for i in range(10, 20):
    key = (options.namespace, options.set, i)
    if i % 2 == 0:
        meta = {"ttl": 3600}
    else:
        meta = {"ttl": 60}
    client.put(key, {"num": i}, meta, policy)
print("Done")

def show_rec(record):
    k, m, _ = record
    print(k, m)


older_than_1sec = exp.GT(exp.SinceUpdateTime(), 1000).compile()
policy = {"expressions": older_than_1sec}
scan_options = {"nobins": True}
scan = client.scan(options.namespace, options.set)
scan.foreach(show_rec, policy, scan_options)

input(
    "\nExtend the TTL on records that have a TTL less than 30m, and an odd bin value < 10"
)
odd_num_bin_and_expire_in_lt_30m = exp.And(
    exp.And(
        exp.LT(exp.IntBin("num"), 10),
        exp.Eq(arithmetic.Mod(exp.IntBin("num"), 2), 1)
    ),
    exp.LT(exp.TTL(), 60 * 30),
).compile()
policy = {"expressions": odd_num_bin_and_expire_in_lt_30m}
scan_options = {"nobins": True}
scan = client.scan(options.namespace, options.set)
scan.add_ops([operations.touch()])
scan.execute_background(policy)

input("\nShow the records that expire in the next minute")
expires_in_next_minute = exp.LE(exp.TTL(), 60).compile()
policy = {"expressions": expires_in_next_minute}
scan_options = {"nobins": True}
scan = client.scan(options.namespace, options.set)
scan.foreach(show_rec, policy, scan_options)

input(
    "\nExtend the TTL on records that have a TTL less than 30m, and an odd key with value > 10"
)
odd_keys_val_gt_10_expire_soon = exp.And(
    exp.And(
        exp.GT(exp.KeyInt(), 10),
        exp.Eq(arithmetic.Mod(exp.KeyInt(), 2), 1)),
    exp.LT(exp.TTL(), 60 * 30),
).compile()
policy = {"expressions": odd_keys_val_gt_10_expire_soon}
scan_options = {"nobins": True}
scan = client.scan(options.namespace, options.set)
scan.add_ops([operations.touch()])
scan.execute_background(policy)
print("Done")

input("\nShow all the records")
scan_options = {"nobins": True}
scan = client.scan(options.namespace, options.set)
scan.foreach(show_rec, {}, scan_options)

client.close()
