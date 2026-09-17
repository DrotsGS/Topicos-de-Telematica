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
import hashlib
import argparse

import grpc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import dfsha_pb2, dfsha_pb2_grpc            # noqa: E402
from common.config import env, read_token, token_file      # noqa: E402
from common.interfaces import CHUNK_SIZE                   # noqa: E402

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

    p = sub.add_parser("put", help="sube un archivo")
    p.add_argument("local")
    p.add_argument("remoto")

    p = sub.add_parser("get", help="baja un archivo")
    p.add_argument("remoto")
    p.add_argument("local")
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


# ---------------------------------------------------------------------
#  RF2: transferencia de archivos (plano de datos)
#
#  El NameNode no aparece en ninguna de estas funciones mas que para
#  preguntarle donde poner o donde buscar. Los bytes van del cliente al
#  DataNode y de vuelta, directo.
# ---------------------------------------------------------------------

def _canal_datanode(direccion):
    return grpc.insecure_channel(direccion)


def _leer_tramo(ruta, offset, cuantos):
    """Genera el tramo [offset, offset+cuantos) en trozos de CHUNK_SIZE.

    Con seek, no leyendo el archivo entero: un archivo de 500 MB no cabe
    comodo en memoria y no tiene por que caber.
    """
    with open(ruta, "rb") as f:
        f.seek(offset)
        restante = cuantos
        while restante > 0:
            datos = f.read(min(CHUNK_SIZE, restante))
            if not datos:
                return
            restante -= len(datos)
            yield datos


def _put_a_un_datanode(direccion, asignacion, ruta, offset):
    """Sube un bloque a UN DataNode y verifica el sha256 que devuelve."""
    canal = _canal_datanode(direccion)
    try:
        stub = dfsha_pb2_grpc.DataNodeServiceStub(canal)
        digest = hashlib.sha256()

        def stream():
            yield dfsha_pb2.BlockChunk(header=dfsha_pb2.BlockHeader(
                block_id=asignacion.block_id,
                size=asignacion.size,
                access_token=asignacion.access_token))
            # TODO semana 9: aqui va cipher.encrypt(datos). Ojo: el bloque
            # cifrado pesa 28 bytes mas por chunk, asi que hay que anunciar
            # el tamano cifrado en el header y llevar DOS checksums: el del
            # texto claro (lo verifica el cliente) y el del cifrado (lo
            # verifica el DataNode).
            for datos in _leer_tramo(ruta, offset, asignacion.size):
                digest.update(datos)
                yield dfsha_pb2.BlockChunk(data=datos)

        respuesta = stub.PutBlock(stream())
        mio = digest.hexdigest()
        if respuesta.sha256 != mio:
            raise RuntimeError(
                "el bloque {} llego corrupto a {} ({} != {})".format(
                    asignacion.block_id, direccion,
                    respuesta.sha256[:12], mio[:12]))
        return respuesta
    finally:
        canal.close()


def subir_bloque(asignacion, ruta, offset):
    """Recorre las replicas hasta que una acepte el bloque.

    Hoy asignacion.datanodes trae un solo elemento, pero se itera igual:
    en la semana 11 traera tres y lo unico que cambia es que en vez de
    quedarse con la primera que funcione, escribe en todas.
    """
    ultimo_error = None
    for destino in asignacion.datanodes:
        try:
            return _put_a_un_datanode(destino, asignacion, ruta, offset)
        except (grpc.RpcError, RuntimeError) as err:
            print("  aviso: {} fallo ({})".format(destino, err))
            ultimo_error = err
    raise RuntimeError(
        "ningun DataNode acepto el bloque {}: {}".format(
            asignacion.block_id, ultimo_error))


def bajar_bloque(ubicacion, ruta, offset):
    """Baja un bloque y lo escribe EN SU SITIO dentro del archivo.

    Nada de acumular y concatenar al final: se hace seek al offset del
    bloque y se escribe ahi. Asi la memoria no depende del tamano del
    archivo.
    """
    ultimo_error = None
    for origen in ubicacion.datanodes:
        canal = _canal_datanode(origen)
        try:
            stub = dfsha_pb2_grpc.DataNodeServiceStub(canal)
            digest = hashlib.sha256()
            escritos = 0
            with open(ruta, "r+b") as f:
                f.seek(offset)
                for chunk in stub.GetBlock(dfsha_pb2.GetBlockRequest(
                        block_id=ubicacion.block_id,
                        access_token=ubicacion.access_token)):
                    # TODO semana 9: cipher.decrypt(chunk.data)
                    digest.update(chunk.data)
                    f.write(chunk.data)
                    escritos += len(chunk.data)

            if ubicacion.sha256 and digest.hexdigest() != ubicacion.sha256:
                raise RuntimeError("checksum distinto al que registro el NameNode")
            if escritos != ubicacion.size:
                raise RuntimeError("se esperaban {} bytes y llegaron {}".format(
                    ubicacion.size, escritos))
            return escritos
        except (grpc.RpcError, RuntimeError, OSError) as err:
            print("  aviso: {} fallo ({})".format(origen, err))
            ultimo_error = err
        finally:
            canal.close()
    raise RuntimeError("ningun DataNode sirvio el bloque {}: {}".format(
        ubicacion.block_id, ultimo_error))


def cmd_put(stub, args):
    if not os.path.isfile(args.local):
        print("no existe el archivo {}".format(args.local))
        sys.exit(1)
    size = os.path.getsize(args.local)

    asignacion = stub.Create(dfsha_pb2.CreateRequest(
        path=args.remoto, size=size, token=token()))
    bloques = sorted(asignacion.blocks, key=lambda b: b.index)
    print("{} -> {}   {} bytes en {} bloques".format(
        args.local, args.remoto, size, len(bloques)))

    try:
        checksums = []
        offset = 0
        for b in bloques:
            r = subir_bloque(b, args.local, offset)
            checksums.append(dfsha_pb2.BlockChecksum(
                block_id=b.block_id, sha256=r.sha256))
            offset += b.size      # el offset sale de los tamanos reales,
            print("  bloque {}/{}  {} bytes  ok".format(
                b.index + 1, len(bloques), b.size))

        stub.Complete(dfsha_pb2.CompleteRequest(
            path=args.remoto, lease_id=asignacion.lease_id,
            checksums=checksums))
        print("listo: {} quedo COMMITTED".format(args.remoto))
    except Exception as err:
        # Si algo se rompe, cancela el lease para no dejar el path
        # bloqueado con un archivo invisible a medio subir.
        print("fallo la subida: {}".format(err))
        try:
            stub.Abort(dfsha_pb2.LeaseRequest(
                lease_id=asignacion.lease_id, token=token()))
            print("subida cancelada, el path quedo libre")
        except grpc.RpcError as err2:
            print("ademas fallo el Abort: {}".format(err2.details()))
        sys.exit(1)


def cmd_get(stub, args):
    info = stub.Open(dfsha_pb2.PathRequest(path=args.remoto, token=token()))
    bloques = sorted(info.blocks, key=lambda b: b.index)
    print("{} -> {}   {} bytes en {} bloques".format(
        args.remoto, args.local, info.size, len(bloques)))

    # Reservar el archivo completo de una vez permite escribir cada
    # bloque en su offset sin depender del orden en que lleguen.
    with open(args.local, "wb") as f:
        f.truncate(info.size)

    offset = 0
    for b in bloques:
        bajar_bloque(b, args.local, offset)
        offset += b.size
        print("  bloque {}/{}  {} bytes  ok".format(
            b.index + 1, len(bloques), b.size))
    print("listo: {} ({} bytes)".format(args.local, os.path.getsize(args.local)))


HANDLERS = {
    "ping": cmd_ping,
    "login": cmd_login,
    "mkdir": cmd_mkdir,
    "ls": cmd_ls,
    "rmdir": cmd_rmdir,
    "rm": cmd_rm,
    "stat": cmd_stat,
    "put": cmd_put,
    "get": cmd_get,
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
