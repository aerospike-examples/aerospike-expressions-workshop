# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception
from aerospike_helpers import expressions as exp
from aerospike_helpers.operations import list_operations as lh
from aerospike_helpers.operations import expression_operations as opexp
import sys

if options.set == "demo":
    options.set = "exp-compose"
config = {"hosts": [(options.host, options.port)]}
if options.alternate:
    config["use_services_alternate"] = True
try:
    client = aerospike.client(config).connect(options.username, options.password)
except exception.ClientError as e:
    print("failed to connect to the cluster with", config["hosts"])
    sys.exit(1)

client.truncate(options.namespace, options.set, 0)

input("\nAdd the record, with a list of numbers in bin 'n'")
key = (options.namespace, options.set, "composition")
policy = {"key": aerospike.POLICY_KEY_SEND}
client.put(key, { "n": [78, 91, 105, 1, 10, 15, 55, 66, 120, 136, 153, 21, 45, 171, 190, 210]}, policy=policy)
_, _, b = client.get(key, policy=policy)
print(b)

input("\nUse the Expression equivalents of getByValueRange(10, 90)[0..3] and sort(getByValueRange)[0..3]")
unordered_slice = exp.ListGetByIndexRange(
    None, aerospike.LIST_RETURN_VALUE, 0, 3,
        exp.ListGetByValueRange(None, aerospike.LIST_RETURN_VALUE, 10, 90,
            exp.ListBin("n"))).compile()

ordered_slice = exp.ListGetByIndexRange(
    None, aerospike.LIST_RETURN_VALUE, 0, 3,
        exp.ListSort(None, aerospike.LIST_SORT_DEFAULT,
            exp.ListGetByValueRange(None, aerospike.LIST_RETURN_VALUE, 10, 90,
                exp.ListBin("n")))).compile()
ops = [
    lh.list_get_by_value_range("n", aerospike.LIST_RETURN_VALUE, 10, 90),
    opexp.expression_read("unordered0-3", unordered_slice),
    opexp.expression_read("ordered-03", ordered_slice),
]
_, _, b = client.operate(key, ops)
print(b)

print("\nThe record shouldn't be changed by read operations and expressions")
_, _, b = client.get(key, policy=policy)
print(b)
client.close()
