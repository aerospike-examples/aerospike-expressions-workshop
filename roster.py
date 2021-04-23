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
    options.set = "roster"
config = {"hosts": [(options.host, options.port)]}
if options.alternate:
    config["use_services_alternate"] = True
try:
    client = aerospike.client(config).connect(options.username, options.password)
except exception.ClientError as e:
    print("failed to connect to the cluster with", config["hosts"])
    sys.exit(1)

client.truncate(options.namespace, options.set, 0)

input("\nTrack the churn on a team: initialize with 2016 roster")
key = (options.namespace, options.set, "sc-sharks-02-blue")
try:
    client.remove(key)
except:
    pass
policy = {"key": aerospike.POLICY_KEY_SEND}
list_policy = {
    "write_flags": aerospike.LIST_WRITE_ADD_UNIQUE
    | aerospike.LIST_WRITE_PARTIAL
    | aerospike.LIST_WRITE_NO_FAIL,
    "list_order": aerospike.LIST_ORDERED,
}
# initialize the roster
roster_2016 = [
    "Cooper K",
    "Max M",
    "Lachlan N",
    "Pranav P",
    "Denali W",
    "Teilo",
    "Kai O",
    "Diego S",
    "Jack T",
    "Abel C",
    "Edgar C",
    "Kevin A",
]
_, _, b = client.operate(
    key, [lh.list_append_items("roster", roster_2016, list_policy),
        oh.read("roster")], policy=policy
)
print(b)

input("\nUpdate the 2017 roster")
current = exp.Cond(
    exp.Eq(exp.BinType("roster"), aerospike.AS_BYTES_LIST),
    exp.ListSize(None, exp.ListBin("roster")),
    0,
).compile()
churn = exp.Add(
    exp.Cond(
        exp.Eq(exp.BinType("churn"), aerospike.AS_BYTES_INTEGER), exp.IntBin("churn"), 0
    ),
    exar.Abs(exar.Sub(exp.ListSize(None, exp.ListBin("roster")), exp.IntBin("tmp"))),
).compile()

# drop 3 players from the roster
remove_2017 = ["Cooper K", "Lachlan N", "Kai O"]
# add 5 new players to the roster
add_2017 = [
    "Cameron M",
    "Max M",
    "Mateo M",
    "Pranav P",
    "Denali W",
    "Teilo",
    "Ian F",
    "Diego S",
    "Jack T",
    "Abel C",
    "Edgar C",
    "Kevin A",
    "Eric R",
    "Nico C",
]
print("Remove the players {}".format(remove_2017))
print("Add the players {}".format(add_2017))
ops = [
    opexp.expression_write("tmp", current),
    lh.list_append_items("roster", add_2017, list_policy),
    opexp.expression_write("churn", churn),
    opexp.expression_write("tmp", current),
    lh.list_remove_by_value_list("roster", remove_2017, aerospike.LIST_RETURN_NONE),
    opexp.expression_write("churn", churn),
    oh.write("tmp", aerospike.null()),
    oh.read("churn"),
    oh.read("roster"),
]
_, _, b = client.operate(key, ops, policy=policy)
print(b)

input("\nUpdate the 2018 roster")
# drop 4 players from the roster
remove_2018 = ["Eric R", "Mateo M", "Abel C", "Kevin A"]
# add 5 new players to the roster
add_2018 = ["Jack B", "Miles H", "Adrian H", "Max M", "Christian C", "Steve V", "Teilo"]
print("Remove the players {}".format(remove_2018))
print("Add the players {}".format(add_2018))
ops = [
    opexp.expression_write("tmp", current),
    lh.list_append_items("roster", add_2018, list_policy),
    opexp.expression_write("churn", churn),
    opexp.expression_write("tmp", current),
    lh.list_remove_by_value_list("roster", remove_2018, aerospike.LIST_RETURN_NONE),
    opexp.expression_write("churn", churn),
    oh.write("tmp", aerospike.null()),
    oh.read("churn"),
    oh.read("roster"),
]
_, _, b = client.operate(key, ops, policy=policy)
print(b)

input("\nUpdate the 2019 roster")
# drop 3 players from the roster
remove_2019 = ["Steve V", "Miles H", "Jack B"]
# add 8 new players to the roster
add_2019 = [
    "Sergio G",
    "Cristian G",
    "Kincaid J",
    "Leonardo L",
    "Mason M",
    "Rowen P",
    "Casten S",
    "Gavin W",
]
print("Remove the players {}".format(remove_2019))
print("Add the players {}".format(add_2019))
ops = [
    opexp.expression_write("tmp", current),
    lh.list_append_items("roster", add_2019, list_policy),
    opexp.expression_write("churn", churn),
    opexp.expression_write("tmp", current),
    lh.list_remove_by_value_list("roster", remove_2019, aerospike.LIST_RETURN_NONE),
    opexp.expression_write("churn", churn),
    oh.write("tmp", aerospike.null()),
    oh.read("churn"),
    oh.read("roster"),
]
_, _, b = client.operate(key, ops, policy=policy)
print(b)

client.close()
