"""
DataNode de DFSha.

Semana 6: se registra ante el NameNode por heartbeat cada 3 segundos.
La transferencia de bloques llega en la semana 8.

    python datanode/server.py
"""

import os
import sys
import time
import shutil
import hashlib
import threading
from concurrent import futures

import grpc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import dfsha_pb2, dfsha_pb2_grpc   # noqa: E402
from common.config import env, data_dir           # noqa: E402
from common.interfaces import CHUNK_SIZE          # noqa: E402

NODE_ID = env("NODE_ID", "dn-1")
PORT = env("PORT", "50060")
ADDR = env("ADVERTISE_ADDR", "localhost:" + PORT)
NAMENODE = env("NAMENODE_ADDR", "localhost:50051")
DATA_DIR = data_dir(NODE_ID)


class DataNodeService(dfsha_pb2_grpc.DataNodeServiceServicer):
    """El almacen de bloques.

    La carpeta se recibe por parametro y no se lee del entorno aqui
    dentro: asi las pruebas pueden levantar varios DataNodes en un mismo
    proceso, cada uno con su disco.
    """

    def __init__(self, carpeta=None):
        self.carpeta = carpeta or DATA_DIR

    def ruta_bloque(self, block_id):
        return os.path.join(self.carpeta, "blk_" + block_id)

    def ruta_temporal(self, block_id):
        # Los tmp_ no empiezan por blk_, asi que el heartbeat no los
        # cuenta como bloques hasta que esten completos.
        return os.path.join(self.carpeta, "tmp_" + block_id)

    def PutBlock(self, request_iterator, context):
        """Recibe un bloque por streaming y lo deja en disco.

        El primer mensaje del stream trae el BlockHeader; los siguientes,
        bytes. Nada se acumula en memoria: cada chunk se escribe apenas
        llega y el sha256 se calcula de forma incremental.

        Se escribe a tmp_<id> y se renombra con os.replace al terminar.
        El renombrado es atomico en Windows y en Linux, asi que en disco
        nunca hay un blk_<id> a medias: o esta completo, o no esta.
        """
        header = None
        digest = hashlib.sha256()
        recibidos = 0
        f = None
        temporal = None

        try:
            for chunk in request_iterator:
                if chunk.HasField("header"):
                    if header is not None:
                        context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                                      "el header llego dos veces")
                    header = chunk.header
                    temporal = self.ruta_temporal(header.block_id)
                    f = open(temporal, "wb")
                    continue

                if header is None:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                                  "el primer mensaje debe ser el header")

                datos = chunk.data
                recibidos += len(datos)
                if recibidos > header.size:
                    context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "llegaron mas bytes de los anunciados ({} > {})".format(
                            recibidos, header.size))
                digest.update(datos)
                f.write(datos)

            if header is None:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                              "stream vacio, sin header")
            if recibidos != header.size:
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "se anunciaron {} bytes y llegaron {}".format(
                        header.size, recibidos))

            f.close()
            f = None
            os.replace(temporal, self.ruta_bloque(header.block_id))
            sha = digest.hexdigest()
            print("[PutBlock] {}  {} bytes  sha256={}".format(
                header.block_id, recibidos, sha[:12]))
            # TODO semana 11: reenviar a header.pipeline[0] si viene lleno
            return dfsha_pb2.PutBlockResponse(ok=True, sha256=sha, message="ok")

        finally:
            if f is not None:
                f.close()
            # Si la subida se corto, no dejes basura ocupando disco.
            if temporal and os.path.exists(temporal):
                try:
                    os.remove(temporal)
                except OSError:
                    pass

    def GetBlock(self, request, context):
        """Devuelve el bloque en trozos de CHUNK_SIZE. Es un generador."""
        ruta = self.ruta_bloque(request.block_id)
        if not os.path.exists(ruta):
            context.abort(grpc.StatusCode.NOT_FOUND,
                          "este DataNode no tiene el bloque " + request.block_id)
        print("[GetBlock] {}  {} bytes".format(
            request.block_id, os.path.getsize(ruta)))
        with open(ruta, "rb") as f:
            while True:
                datos = f.read(CHUNK_SIZE)
                if not datos:
                    return
                yield dfsha_pb2.BlockChunk(data=datos)

    # TODO semana 11: el recolector de basura lo llama por comando piggyback.
    def DeleteBlock(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("DeleteBlock llega en la semana 11")
        return dfsha_pb2.StatusResponse()

    # TODO semana 11: ReplicateTo, para el pipeline DataNode -> DataNode


def heartbeat_loop():
    """El DataNode reporta su propia direccion.

    Solo el es capaz de saber por donde lo alcanzan los clientes: en
    Windows es localhost, en Docker el nombre del servicio, en EC2 una
    IP privada. Por eso el NameNode no la adivina, se la preguntan.
    """
    stub = dfsha_pb2_grpc.ControlServiceStub(grpc.insecure_channel(NAMENODE))
    while True:
        try:
            free = shutil.disk_usage(DATA_DIR).free
            n = len([f for f in os.listdir(DATA_DIR) if f.startswith("blk_")])
            resp = stub.Heartbeat(dfsha_pb2.HeartbeatRequest(
                node_id=NODE_ID, addr=ADDR, free_bytes=free, num_blocks=n))
            for cmd in resp.commands:
                print("[comando piggyback] {}".format(cmd))   # TODO semana 11
        except grpc.RpcError as e:
            print("[heartbeat] NameNode no responde: {}".format(e.code().name))
        time.sleep(3)


def serve():
    os.makedirs(DATA_DIR, exist_ok=True)
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dfsha_pb2_grpc.add_DataNodeServiceServicer_to_server(DataNodeService(), server)
    server.add_insecure_port("[::]:" + PORT)
    server.start()
    print("DataNode {} escuchando en el puerto {}".format(NODE_ID, PORT))
    print("  se anuncia como : {}".format(ADDR))
    print("  NameNode        : {}".format(NAMENODE))
    print("  bloques en      : {}".format(DATA_DIR))
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nDeteniendo DataNode.")


if __name__ == "__main__":
    serve()
