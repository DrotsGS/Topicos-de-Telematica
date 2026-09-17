"""Prueba de aceptacion del hito 1: sube, borra, baja y compara el hash.

Levanta un NameNode y un DataNode en este mismo proceso, en puertos
efimeros, para poder correrla sin montar nada:

    python scripts/prueba_hito1.py --mb 500

Con el valor de diseno de BLOCK_SIZE (128 MB), 500 MB son cuatro
bloques: tres llenos y uno parcial de 116 MB.
"""

import os
import sys
import time
import shutil
import hashlib
import argparse
import tempfile
from concurrent import futures

import grpc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import dfsha_pb2, dfsha_pb2_grpc     # noqa: E402
from common.interfaces import BLOCK_SIZE            # noqa: E402
from namenode import server as nn_server            # noqa: E402
from datanode import server as dn_server            # noqa: E402
from client import cli                              # noqa: E402

MB = 1024 * 1024


class Args:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def sha256(ruta):
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for trozo in iter(lambda: f.read(8 * MB), b""):
            h.update(trozo)
    return h.hexdigest()


def generar(ruta, mb):
    """Escribe un archivo de mb megabytes sin cargarlo en memoria."""
    patron = os.urandom(MB)
    with open(ruta, "wb") as f:
        for i in range(mb):
            # Que cada MB sea distinto: si se traspapelan bloques, se nota.
            f.write(bytes([i % 251]) + patron[1:])


def levantar(carpeta_bloques):
    servicio = nn_server.NameNodeService()
    servicio.auth = nn_server.build_auth()

    nn = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    dfsha_pb2_grpc.add_NameNodeServiceServicer_to_server(servicio, nn)
    puerto_nn = nn.add_insecure_port("localhost:0")
    nn.start()

    dnodo = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    dfsha_pb2_grpc.add_DataNodeServiceServicer_to_server(
        dn_server.DataNodeService(carpeta_bloques), dnodo)
    puerto_dn = dnodo.add_insecure_port("localhost:0")
    dnodo.start()

    servicio.datanodes["dn-1"] = {
        "addr": "localhost:{}".format(puerto_dn),
        "free_bytes": shutil.disk_usage(carpeta_bloques).free,
        "num_blocks": 0}

    canal = grpc.insecure_channel("localhost:{}".format(puerto_nn))
    return servicio, dfsha_pb2_grpc.NameNodeServiceStub(canal), [nn, dnodo]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mb", type=int, default=500)
    ap.add_argument("--dir", default=None, help="donde dejar los temporales")
    args = ap.parse_args()

    base = args.dir or tempfile.mkdtemp(prefix="dfsha_hito1_")
    bloques = os.path.join(base, "bloques")
    os.makedirs(bloques, exist_ok=True)
    origen = os.path.join(base, "origen.bin")
    bajado = os.path.join(base, "bajado.bin")

    print("BLOCK_SIZE = {} MB   archivo = {} MB   temporales en {}".format(
        BLOCK_SIZE // MB, args.mb, base))

    print("\n[1/4] generando el archivo...")
    t = time.time()
    generar(origen, args.mb)
    hash_original = sha256(origen)
    print("      {} bytes   sha256={}   ({:.1f} s)".format(
        os.path.getsize(origen), hash_original[:16], time.time() - t))

    servicio, stub, servidores = levantar(bloques)
    token = stub.Login(dfsha_pb2.LoginRequest(
        user="drots", password="dfsha")).token
    cli.token = lambda: token

    try:
        print("\n[2/4] subiendo...")
        t = time.time()
        cli.cmd_put(stub, Args(local=origen, remoto="/grande.bin"))
        subida = time.time() - t
        print("      {:.1f} s   {:.1f} MB/s".format(
            subida, args.mb / subida))

        print("\n[3/4] borrando el original y bajando...")
        os.remove(origen)
        t = time.time()
        cli.cmd_get(stub, Args(remoto="/grande.bin", local=bajado))
        bajada = time.time() - t
        print("      {:.1f} s   {:.1f} MB/s".format(
            bajada, args.mb / bajada))

        print("\n[4/4] comparando...")
        hash_bajado = sha256(bajado)
        iguales = hash_bajado == hash_original
        print("      original: {}".format(hash_original))
        print("      bajado  : {}".format(hash_bajado))
        print("      tamano  : {} bytes".format(os.path.getsize(bajado)))
        print("      bloques en el DataNode: {}".format(
            len([f for f in os.listdir(bloques) if f.startswith("blk_")])))
        print("\n{}".format("HITO 1 OK" if iguales else "FALLO: los hashes no coinciden"))
        return 0 if iguales else 1
    finally:
        for s in servidores:
            s.stop(None)
        if args.dir is None:
            shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
