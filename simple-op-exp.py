# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception
from aerospike_helpers import expressions as exp
import aerospike_helpers.expressions.arithmetic as exar
from aerospike_helpers.operations import list_operations as lh
from aerospike_helpers.operations import operations as oh
from aerospike_helpers.operations import expression_operations as opexp
import calendar
from datetime import datetime
import sys
import time

if options.set == "demo":
    options.set = "op-expressions"
config = {"hosts": [(options.host, options.port)]}
if options.alternate:
    config["use_services_alternate"] = True
try:
    client = aerospike.client(config).connect(options.username, options.password)
except exception.ClientError as e:
    print("failed to connect to the cluster with", config["hosts"])
    sys.exit(1)

client.truncate(options.namespace, options.set, 0)

input("\nAdd a record, then get its last-update-time information as bins")
key = (options.namespace, options.set, "dummy")
policy = {"key": aerospike.POLICY_KEY_SEND}
client.put(key, {"us": "them", "me": "you"}, policy=policy)

since_update = exp.SinceUpdateTime().compile()
get_lut = exp.LastUpdateTime().compile()
ops = [
    oh.read("us"),
    opexp.expression_read("since_update", since_update),
    opexp.expression_read("lut", get_lut),
]
k, m, b = client.operate(key, ops)
print("The raw result bins: {}".format(b))
print("Milliseconds since the record was last updated: {}ms".format(b["since_update"]))
dt = datetime.fromtimestamp(int(b["lut"] / 1000000000))
print("Last update time was: {}".format(dt))

input("\nSome cross-bin arithmetic saved into an existing and a new bin")
client.put(
    key,
    {
        "a": [78, 91, 105, 1, 10, 15, 55],
        "b": [66, 120, 136, 153, 21, 45, 171, 190, 210],
        "c": 27,
        "d": 201,
    },
    policy=policy,
)

calc = exp.Let(
    exp.Def(
        "combined", exp.ListAppendItems(None, None, exp.ListBin("b"), exp.ListBin("a"))
    ),
    exp.Def(
        "combined_min",
        exp.ListGetByRank(
            None,
            aerospike.LIST_RETURN_VALUE,
            exp.ResultType.INTEGER,
            0,
            exp.Var("combined"),
        ),
    ),
    exp.Def(
        "combined_max",
        exp.ListGetByRank(
            None,
            aerospike.LIST_RETURN_VALUE,
            exp.ResultType.INTEGER,
            -1,
            exp.Var("combined"),
        ),
    ),
    exar.Sub(exp.Var("combined_max"), exp.Var("combined_min")),
).compile()
get_min = exp.Min(exp.IntBin("c"), exp.IntBin("d"), exp.IntBin("e")).compile()
ops = [
    opexp.expression_write("e", calc),
    oh.read("e"),
    oh.read("d"),
    oh.read("c"),
    opexp.expression_read("min", get_min),
    opexp.expression_read(
        "sum_d_e", exp.Add(exp.IntBin("e"), exp.IntBin("d")).compile()
    ),
]
_, _, b = client.operate(key, ops, policy=policy)
print(b)

client.close()
