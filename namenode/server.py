"""
NameNode de DFSha.

Semana 6: Ping, Mkdir y Ls contra el namespace en memoria, mas el
registro de DataNodes por heartbeat.
Semana 7: Rmdir, Rm y Stat, con el mapeo uniforme a StatusCode.

    python namenode/server.py
"""

import os
import sys
import time
from concurrent import futures

import grpc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import dfsha_pb2, dfsha_pb2_grpc          # noqa: E402
from common.config import env                            # noqa: E402
from common.interfaces import build_auth, RoundRobinPlacer  # noqa: E402
from namenode.namespace import Namespace                  # noqa: E402

NODE_ID = env("NODE_ID", "nn-1")
PORT = env("PORT", "50051")


# ---------------------------------------------------------------------
#  Mapeo de errores del namespace a codigos gRPC.
#
#  En un solo sitio a proposito: es lo que el curso evalua como "uso
#  correcto del middleware", y disperso en ifs se vuelve inconsistente.
#
#  | Situacion                                  | StatusCode          |
#  |--------------------------------------------|---------------------|
#  | Token invalido o ausente                   | UNAUTHENTICATED     |
#  | Usuario sin permiso sobre el path          | PERMISSION_DENIED   |
#  | El path (o el padre) no existe             | NOT_FOUND           |
#  | Ya existe                                  | ALREADY_EXISTS      |
#  | Directorio no vacio sin recursivo          | FAILED_PRECONDITION |
#  | Tipo equivocado (rmdir de archivo, etc.)   | INVALID_ARGUMENT    |
#  | No hay DataNodes vivos                     | UNAVAILABLE         |
#  | Error interno inesperado                   | INTERNAL            |
# ---------------------------------------------------------------------
CODIGOS = {
    "no existe": grpc.StatusCode.NOT_FOUND,
    "el directorio padre no existe": grpc.StatusCode.NOT_FOUND,
    "ya existe": grpc.StatusCode.ALREADY_EXISTS,
    "el directorio no esta vacio": grpc.StatusCode.FAILED_PRECONDITION,
    "no se puede borrar la raiz": grpc.StatusCode.FAILED_PRECONDITION,
    "no es un directorio": grpc.StatusCode.INVALID_ARGUMENT,
    "es un directorio, usa rmdir": grpc.StatusCode.INVALID_ARGUMENT,
}


class NameNodeService(dfsha_pb2_grpc.NameNodeServiceServicer):
    def __init__(self):
        self.ns = Namespace()
        self.auth = build_auth()
        self.placer = RoundRobinPlacer()
        self.datanodes = {}   # node_id -> {addr, free_bytes, last_seen}

        # Bloques que perdieron a su dueno y todavia ocupan disco en algun
        # DataNode. Hoy solo se acumulan. En la semana 11 el Heartbeat
        # vacia esta cola mandando DeleteCommand por piggyback.
        self.pendientes_borrado = []

    # ---------------- helpers ----------------

    def _autorizar(self, token, context):
        """context.abort lanza excepcion: el resto del metodo no corre.

        Es mas limpio que set_code + return, y evita el error clasico de
        devolver un mensaje vacio con codigo de exito.
        """
        usuario = self.auth.verify(token)
        if usuario is None:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "token invalido")
        return usuario

    @staticmethod
    def _fallar(context, msg):
        context.abort(CODIGOS.get(msg, grpc.StatusCode.INTERNAL), msg)

    def _agendar_borrado(self, huerfanos):
        if huerfanos:
            self.pendientes_borrado.extend(huerfanos)
            print("[GC] {} bloques huerfanos agendados ({} en cola)".format(
                len(huerfanos), len(self.pendientes_borrado)))

    # ---------------- RF1: namespace ----------------

    def Ping(self, request, context):
        return dfsha_pb2.PingResponse(
            node_id=NODE_ID, is_leader=True, leader_addr="")

    def Login(self, request, context):
        token = self.auth.login(request.user, request.password)
        if token is None:
            print("[Login] {} -> rechazado".format(request.user))
            context.abort(grpc.StatusCode.UNAUTHENTICATED,
                          "usuario o contrasena incorrectos")
        print("[Login] {} -> ok".format(request.user))
        vence = getattr(self.auth, "ttl", 0)
        return dfsha_pb2.LoginResponse(
            token=token, expires_at=int(time.time()) + vence)

    def Mkdir(self, request, context):
        self._autorizar(request.token, context)
        ok, msg = self.ns.mkdir(request.path)
        print("[Mkdir] {} -> {}".format(request.path, msg))
        if not ok:
            self._fallar(context, msg)
        return dfsha_pb2.StatusResponse(ok=True, message=msg)

    def Ls(self, request, context):
        self._autorizar(request.token, context)
        entries = self.ns.ls(request.path)
        if entries is None:
            context.abort(grpc.StatusCode.NOT_FOUND,
                          "no existe o no es un directorio")
        print("[Ls] {} -> {} entradas".format(request.path, len(entries)))
        return dfsha_pb2.LsResponse(entries=[
            dfsha_pb2.Entry(name=e.name, is_dir=e.is_dir, size=e.size)
            for e in entries
        ])

    def Stat(self, request, context):
        self._autorizar(request.token, context)
        nodo = self.ns.stat(request.path)
        if nodo is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "no existe")
        return dfsha_pb2.FileInfo(
            path=request.path,
            is_dir=nodo.is_dir,
            size=nodo.size,
            num_blocks=len(nodo.blocks),
            state=nodo.state,
            created_at=int(nodo.created_at))

    def Rmdir(self, request, context):
        self._autorizar(request.token, context)
        # PathRequest todavia no lleva el flag recursivo: la v1 no es
        # recursiva a proposito (D5) y el .proto no se toca esta semana.
        ok, msg, huerfanos = self.ns.rmdir(request.path)
        print("[Rmdir] {} -> {}".format(request.path, msg))
        if not ok:
            self._fallar(context, msg)
        self._agendar_borrado(huerfanos)
        return dfsha_pb2.StatusResponse(ok=True, message=msg)

    def Rm(self, request, context):
        self._autorizar(request.token, context)
        ok, msg, huerfanos = self.ns.rm(request.path)
        print("[Rm] {} -> {}".format(request.path, msg))
        if not ok:
            self._fallar(context, msg)
        self._agendar_borrado(huerfanos)
        return dfsha_pb2.StatusResponse(ok=True, message=msg)

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
