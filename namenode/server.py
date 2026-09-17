"""
NameNode de DFSha.

Semana 6: Ping, Mkdir y Ls contra el namespace en memoria, mas el
registro de DataNodes por heartbeat.

    python namenode/server.py
"""

import os
import sys
from concurrent import futures

import grpc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import dfsha_pb2, dfsha_pb2_grpc          # noqa: E402
from common.config import env                            # noqa: E402
from common.interfaces import NoopAuth, RoundRobinPlacer  # noqa: E402
from namenode.namespace import Namespace                  # noqa: E402

NODE_ID = env("NODE_ID", "nn-1")
PORT = env("PORT", "50051")


class NameNodeService(dfsha_pb2_grpc.NameNodeServiceServicer):
    def __init__(self):
        self.ns = Namespace()
        self.auth = NoopAuth()
        self.placer = RoundRobinPlacer()
        self.datanodes = {}   # node_id -> {addr, free_bytes, last_seen}

    def _check(self, token, context):
        user = self.auth.verify(token)
        if user is None:
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            context.set_details("token invalido")
        return user

    def Ping(self, request, context):
        return dfsha_pb2.PingResponse(
            node_id=NODE_ID, is_leader=True, leader_addr="")

    def Mkdir(self, request, context):
        if self._check(request.token, context) is None:
            return dfsha_pb2.StatusResponse()

        ok, msg = self.ns.mkdir(request.path)
        if not ok:
            context.set_code(
                grpc.StatusCode.ALREADY_EXISTS if msg == "ya existe"
                else grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
        print("[Mkdir] {} -> {}".format(request.path, msg))
        return dfsha_pb2.StatusResponse(ok=ok, message=msg)

    def Ls(self, request, context):
        entries = self.ns.ls(request.path)
        if entries is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("no existe o no es un directorio")
            return dfsha_pb2.LsResponse()
        print("[Ls] {} -> {} entradas".format(request.path, len(entries)))
        return dfsha_pb2.LsResponse(entries=[
            dfsha_pb2.Entry(name=e.name, is_dir=e.is_dir, size=e.size)
            for e in entries
        ])

    # TODO semana 7: Rmdir, Rm, Stat
    #   Escoge bien el StatusCode: NOT_FOUND, FAILED_PRECONDITION
    #   (borrar directorio no vacio), PERMISSION_DENIED.

    # TODO semana 8: Create, Complete, Abort, Open
    #   Create asigna blockIDs con self.placer sobre self.datanodes vivos
    #   y devuelve el lease. Complete hace el commit a COMMITTED.


class ControlService(dfsha_pb2_grpc.ControlServiceServicer):
    """El DataNode siempre inicia la conexion. El NameNode nunca al reves."""

    def __init__(self, nn):
        self.nn = nn

    def Heartbeat(self, request, context):
        nuevo = request.node_id not in self.nn.datanodes
        self.nn.datanodes[request.node_id] = {
            "addr": request.addr,
            "free_bytes": request.free_bytes,
            "num_blocks": request.num_blocks,
        }
        if nuevo:
            print("[Heartbeat] nuevo DataNode: {} en {}".format(
                request.node_id, request.addr))
        # TODO semana 11: aqui van los comandos piggyback (replicar, borrar)
        return dfsha_pb2.HeartbeatResponse()

    # TODO semana 10: BlockReport, BlockReceived


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    nn = NameNodeService()
    dfsha_pb2_grpc.add_NameNodeServiceServicer_to_server(nn, server)
    dfsha_pb2_grpc.add_ControlServiceServicer_to_server(ControlService(nn), server)
    server.add_insecure_port("[::]:" + PORT)
    server.start()
    print("NameNode {} escuchando en el puerto {}".format(NODE_ID, PORT))
    print("Ctrl+C para detener.")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nDeteniendo NameNode.")


if __name__ == "__main__":
    serve()
