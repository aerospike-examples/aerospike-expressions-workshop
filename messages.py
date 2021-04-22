# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception
from aerospike_helpers import expressions as exp
import calendar
import sys
import time

if options.set == "demo":
    options.set = "messages"
config = {"hosts": [(options.host, options.port)]}
if options.alternate:
    config["use_services_alternate"] = True
try:
    client = aerospike.client(config).connect(options.username, options.password)
except exception.ClientError as e:
    print("failed to connect to the cluster with", config["hosts"])
    sys.exit(1)


def show_msg(record):
    k, _, b = record
    print("Message {}: {}".format(k[2], b))


client.truncate(options.namespace, options.set, 0)

input("\nPopulate {}.{}?".format(options.namespace, options.set))
messages = [
    (0, "Suddenly, a dark cloud settled over first period.", []),
    (1, "I got a C in Debate?", ["cher"]),
    (2, "Did you get your report card?", ["cher", "dee"]),
    (3, "Yeah. I'm toast. How did you do?", ["cher", "dee"]),
    (4, "My father's going to go ballistic on me.", ["cher", "dee"]),
    (5, "Isn't my house classic?  The columns date all the way back to 1972.", []),
    (6, "Yuck! The maudlin music of the university station?", []),
]
policy = {"key": aerospike.POLICY_KEY_SEND}
for m in messages:
    mid, msg, access = m
    key = (options.namespace, options.set, mid)
    client.put(key, {"msg": msg, "access": access}, policy=policy)
now1 = calendar.timegm(time.gmtime())
scan = client.scan(options.namespace, options.set)
scan.foreach(show_msg, policy)
print("Done")

input("\nGet message ID 0 if it is public")
is_public = exp.Eq(exp.ListSize(None, exp.ListBin("access")), 0).compile()
policy = {"expressions": is_public}

key = (options.namespace, options.set, 0)
rec = client.get(key, policy=policy)
show_msg(rec)

input("\nGet message ID 1 if it is public")
try:
    key = (options.namespace, options.set, 1)
    rec = client.get(key, policy=policy)
    show_msg(rec)
except exception.FilteredOut as e:
    print("Message with ID 1 is not public. Error code {}, {}".format(e.code, e.msg))

input("\nGet the public messages out of a batch of records")
keys = []
for i in range(7):
    keys.append((options.namespace, options.set, i))
res = client.get_many(keys, policy=policy)
for rec in res:
    show_msg(rec)

input("\nGet only personal messages (note to self) from a batch of records")
is_personal = exp.Let(
    exp.Def("access_size", exp.ListSize(None, exp.ListBin("access"))),
    exp.And(
        exp.Eq(exp.Var("access_size"), 1),
        exp.Eq(
            exp.ListGetByIndex(
                None,
                aerospike.LIST_RETURN_VALUE,
                exp.ResultType.STRING,
                0,
                exp.ListBin("access"),
            ),
            "cher",
        ),
    ),
).compile()
policy = {"expressions": is_personal}
res = client.get_many(keys, policy=policy)
for rec in res:
    show_msg(rec)

input("\nAlternative Filter Expression to get personal messages")
is_personal = exp.And(
    exp.Eq(exp.ListSize(None, "access"), 1),
    exp.Eq(
        exp.ListGetByIndex(
            None, aerospike.LIST_RETURN_VALUE, exp.ResultType.STRING, 0, "access"
        ),
        "cher",
    ),
).compile()
policy = {"expressions": is_personal}
res = client.get_many(keys, policy=policy)
for rec in res:
    show_msg(rec)

input("\nFind all private messages")
is_private = exp.And(
    exp.GE(exp.ListSize(None, exp.ListBin("access")), 2),
    exp.Eq(
        exp.ListGetByValue(
            None, aerospike.LIST_RETURN_COUNT, "cher", exp.ListBin("access")
        ),
        1,
    ),
).compile()
policy = {"expressions": is_private}
res = client.get_many(keys, policy=policy)
for rec in res:
    show_msg(rec)

input("\nFind private messages less than 5 seconds old")
now2 = calendar.timegm(time.gmtime())
if now2 - now1 <= 5:
    # make sure to pause 5 seconds from adding the test messages
    time.sleep(now2 - now1)
private_and_recent = exp.And(
    exp.LT(exp.SinceUpdateTime(), 5000),
    exp.GE(exp.ListSize(None, exp.ListBin("access")), 2),
    exp.Eq(
        exp.ListGetByValue(
            None, aerospike.LIST_RETURN_COUNT, "cher", exp.ListBin("access")
        ),
        1,
    ),
).compile()
policy = {"expressions": private_and_recent}
res = client.get_many(keys, policy=policy)
for rec in res:
    show_msg(rec)

client.close()
