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
import threading
from concurrent import futures

import grpc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import dfsha_pb2, dfsha_pb2_grpc   # noqa: E402
from common.config import env, data_dir           # noqa: E402

NODE_ID = env("NODE_ID", "dn-1")
PORT = env("PORT", "50060")
ADDR = env("ADVERTISE_ADDR", "localhost:" + PORT)
NAMENODE = env("NAMENODE_ADDR", "localhost:50051")
DATA_DIR = data_dir(NODE_ID)


class DataNodeService(dfsha_pb2_grpc.DataNodeServiceServicer):

    # TODO semana 8: PutBlock
    #   El primer mensaje del stream trae BlockHeader (block_id, size,
    #   token). Los siguientes traen bytes. Vas acumulando a disco en
    #   DATA_DIR/blk_<id>, calculas el sha256 y lo devuelves.
    def PutBlock(self, request_iterator, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("PutBlock llega en la semana 8")
        return dfsha_pb2.PutBlockResponse()

    # TODO semana 8: GetBlock
    #   Lees el archivo en trozos de CHUNK_SIZE y haces yield de
    #   BlockChunk(data=...). Es un generador.
    def GetBlock(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("GetBlock llega en la semana 8")
        return
        yield   # esto hace que Python trate la funcion como generador

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
