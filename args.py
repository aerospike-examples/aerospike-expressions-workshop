import argparse
import sys

argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument(
    "--help", dest="help", action="store_true", help="Displays this message."
)
argparser.add_argument(
    "-U",
    "--username",
    dest="username",
    metavar="<USERNAME>",
    help="Username to connect to database.",
)
argparser.add_argument(
    "-P",
    "--password",
    dest="password",
    metavar="<PASSWORD>",
    help="Password to connect to database.",
)
argparser.add_argument(
    "-h",
    "--host",
    dest="host",
    default="172.17.0.2",
    metavar="<ADDRESS>",
    help="Address of Aerospike server.",
)
argparser.add_argument(
    "-p",
    "--port",
    dest="port",
    type=int,
    default=3000,
    metavar="<PORT>",
    help="Port of the Aerospike server.",
)
argparser.add_argument(
    "-n",
    "--namespace",
    dest="namespace",
    default="summit",
    metavar="<NS>",
    help="Namespace name to use",
)
argparser.add_argument(
    "-s",
    "--set",
    dest="set",
    default="demo",
    metavar="<SET>",
    help="Set name to use.",
)
argparser.add_argument(
    "--services-alternate",
    dest="alternate",
    action="store_true",
    help="Use services alternate",
)
options = argparser.parse_args()
if options.help:
    argparser.print_help()
    print()
    sys.exit()
