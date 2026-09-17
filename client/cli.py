"""
Cliente DFSha.

Semana 6: ping, mkdir, ls.
Semana 7: login, rmdir, rm, stat.
Semana 8: put y get.

    python client/cli.py login drots
    python client/cli.py mkdir /docs
    python client/cli.py ls /
"""

import os
import sys
import getpass
import argparse

import grpc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import dfsha_pb2, dfsha_pb2_grpc            # noqa: E402
from common.config import env, read_token, token_file      # noqa: E402

NAMENODE = env("NAMENODE_ADDR", "localhost:50051")


def token():
    """Se lee en cada llamada, no al importar: el login lo cambia."""
    return read_token()


def build_parser():
    ap = argparse.ArgumentParser(
        prog="dfsha", description="Cliente del sistema de archivos DFSha")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("ping", help="verifica que el NameNode responde")

    p = sub.add_parser("login", help="pide un token y lo guarda")
    p.add_argument("usuario")
    p.add_argument("--password", help="si no lo pasas, se pide sin eco")

    p = sub.add_parser("mkdir", help="crea un directorio")
    p.add_argument("path")

    p = sub.add_parser("ls", help="lista un directorio")
    p.add_argument("path", nargs="?", default="/")

    p = sub.add_parser("rmdir", help="borra un directorio vacio")
    p.add_argument("path")

    p = sub.add_parser("rm", help="borra un archivo")
    p.add_argument("path")

    p = sub.add_parser("stat", help="muestra los metadatos de una ruta")
    p.add_argument("path")

    # TODO semana 8:
    #   put <archivo_local> <ruta_remota>
    #   get <ruta_remota> <archivo_local>
    return ap


def cmd_ping(stub, args):
    r = stub.Ping(dfsha_pb2.Empty())
    print("{}   lider={}".format(r.node_id, r.is_leader))


def cmd_login(stub, args):
    password = args.password or getpass.getpass("contrasena: ")
    r = stub.Login(dfsha_pb2.LoginRequest(
        user=args.usuario, password=password))
    destino = token_file()
    with open(destino, "w", encoding="utf-8") as f:
        f.write(r.token)
    try:
        os.chmod(destino, 0o600)     # en Windows es casi simbolico
    except OSError:
        pass
    print("token guardado en {}".format(destino))


def cmd_mkdir(stub, args):
    r = stub.Mkdir(dfsha_pb2.PathRequest(path=args.path, token=token()))
    print(r.message)


def cmd_ls(stub, args):
    r = stub.Ls(dfsha_pb2.PathRequest(path=args.path, token=token()))
    if not r.entries:
        print("(vacio)")
        return
    for e in r.entries:
        tipo = "d" if e.is_dir else "-"
        print("{}  {:<24} {}".format(tipo, e.name, e.size))


def cmd_rmdir(stub, args):
    r = stub.Rmdir(dfsha_pb2.PathRequest(path=args.path, token=token()))
    print(r.message)


def cmd_rm(stub, args):
    r = stub.Rm(dfsha_pb2.PathRequest(path=args.path, token=token()))
    print(r.message)


def cmd_stat(stub, args):
    r = stub.Stat(dfsha_pb2.PathRequest(path=args.path, token=token()))
    tipo = "directorio" if r.is_dir else "archivo"
    estado = "COMMITTED" if r.state == dfsha_pb2.COMMITTED \
        else "UNDER_CONSTRUCTION"
    print("ruta    : {}".format(r.path))
    print("tipo    : {}".format(tipo))
    print("tamano  : {} bytes".format(r.size))
    print("bloques : {}".format(r.num_blocks))
    print("estado  : {}".format(estado))


HANDLERS = {
    "ping": cmd_ping,
    "login": cmd_login,
    "mkdir": cmd_mkdir,
    "ls": cmd_ls,
    "rmdir": cmd_rmdir,
    "rm": cmd_rm,
    "stat": cmd_stat,
}


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
