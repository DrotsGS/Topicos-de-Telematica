"""
Cliente DFSha.

Semana 6: ping, mkdir, ls.
Semana 8: put y get.

    python client/cli.py ping
    python client/cli.py mkdir /docs
    python client/cli.py ls /
"""

import os
import sys
import argparse

import grpc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import dfsha_pb2, dfsha_pb2_grpc   # noqa: E402
from common.config import env                     # noqa: E402

NAMENODE = env("NAMENODE_ADDR", "localhost:50051")
TOKEN = env("DFSHA_TOKEN", "fake-token:drots")


def build_parser():
    ap = argparse.ArgumentParser(
        prog="dfsha", description="Cliente del sistema de archivos DFSha")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("ping", help="verifica que el NameNode responde")

    p = sub.add_parser("mkdir", help="crea un directorio")
    p.add_argument("path")

    p = sub.add_parser("ls", help="lista un directorio")
    p.add_argument("path", nargs="?", default="/")

    # TODO semana 7: rmdir, rm, stat
    # TODO semana 8:
    #   put <archivo_local> <ruta_remota>
    #   get <ruta_remota> <archivo_local>
    return ap


def cmd_ping(stub, args):
    r = stub.Ping(dfsha_pb2.Empty())
    print("{}   lider={}".format(r.node_id, r.is_leader))


def cmd_mkdir(stub, args):
    r = stub.Mkdir(dfsha_pb2.PathRequest(path=args.path, token=TOKEN))
    print(r.message)


def cmd_ls(stub, args):
    r = stub.Ls(dfsha_pb2.PathRequest(path=args.path, token=TOKEN))
    if not r.entries:
        print("(vacio)")
        return
    for e in r.entries:
        tipo = "d" if e.is_dir else "-"
        print("{}  {:<24} {}".format(tipo, e.name, e.size))


HANDLERS = {"ping": cmd_ping, "mkdir": cmd_mkdir, "ls": cmd_ls}


def main():
    args = build_parser().parse_args()
    channel = grpc.insecure_channel(NAMENODE)
    stub = dfsha_pb2_grpc.NameNodeServiceStub(channel)
    try:
        HANDLERS[args.cmd](stub, args)
    except grpc.RpcError as err:
        print("error [{}]: {}".format(err.code().name, err.details()))
        sys.exit(1)


if __name__ == "__main__":
    main()
