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
import uuid
import threading
from concurrent import futures

import grpc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import dfsha_pb2, dfsha_pb2_grpc          # noqa: E402
from common.config import env                            # noqa: E402
from common.interfaces import (                            # noqa: E402
    build_auth, ConsistentHashPlacer, BLOCK_SIZE, REPLICATION_FACTOR)
from namenode.namespace import Namespace, COMMITTED, UNDER_CONSTRUCTION  # noqa: E402

NODE_ID = env("NODE_ID", "nn-1")
PORT = env("PORT", "50051")

# Con heartbeats cada 3 s, 30 segundos son diez fallos seguidos: no se
# declara muerto a un nodo por una hipo de la red.
TIMEOUT_DATANODE = int(env("TIMEOUT_DATANODE", "30"))
INTERVALO_VIGILANCIA = 10


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
    "el lease ya no es valido": grpc.StatusCode.FAILED_PRECONDITION,
    "el archivo ya esta completo": grpc.StatusCode.FAILED_PRECONDITION,
}


class NameNodeService(dfsha_pb2_grpc.NameNodeServiceServicer):
    def __init__(self):
        self.ns = Namespace()
        self.auth = build_auth()
        self.placer = ConsistentHashPlacer()
        self.datanodes = {}   # node_id -> {addr, free_bytes, last_seen}

        # El block map: block_id -> {index, size, datanodes, sha256}.
        # Va aparte del namespace a proposito, son dos estructuras
        # distintas. El namespace dice que archivos hay; el block map,
        # donde estan sus bloques.
        self.block_map = {}
        self.leases = {}      # lease_id -> path

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
            for block_id in huerfanos:
                self.block_map.pop(block_id, None)
            print("[GC] {} bloques huerfanos agendados ({} en cola)".format(
                len(huerfanos), len(self.pendientes_borrado)))

    def _purgar_leases(self):
        """Un borrado puede haberse llevado por delante un archivo en
        construccion (D6). Su lease deja de existir con el."""
        vigentes = {}
        for lease_id, path in self.leases.items():
            nodo = self.ns.stat(path)
            if nodo is not None and nodo.lease_id == lease_id:
                vigentes[lease_id] = path
        self.leases = vigentes

    def vivos(self):
        """DataNodes de los que se supo hace menos de TIMEOUT_DATANODE.

        Create usa esto y no datanodes.keys(): es un cambio de una linea
        con consecuencias grandes, porque sin el el NameNode asigna
        bloques a nodos muertos.
        """
        corte = time.time() - TIMEOUT_DATANODE
        return [nid for nid, info in self.datanodes.items()
                if info.get("last_seen", 0) > corte]

    def muertos(self):
        corte = time.time() - TIMEOUT_DATANODE
        return [nid for nid, info in self.datanodes.items()
                if info.get("last_seen", 0) <= corte]

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
        self._purgar_leases()
        return dfsha_pb2.StatusResponse(ok=True, message=msg)

    def Rm(self, request, context):
        self._autorizar(request.token, context)
        ok, msg, huerfanos = self.ns.rm(request.path)
        print("[Rm] {} -> {}".format(request.path, msg))
        if not ok:
            self._fallar(context, msg)
        self._agendar_borrado(huerfanos)
        self._purgar_leases()
        return dfsha_pb2.StatusResponse(ok=True, message=msg)

    # ---------------- RF2: escritura (WORM) ----------------

    def Create(self, request, context):
        """Otorga el lease y dice donde va cada bloque.

        No mueve un solo byte: el NameNode nunca toca el plano de datos.
        Devuelve direcciones (host:puerto) y no node_id, porque es el
        cliente quien va a marcarlas.
        """
        self._autorizar(request.token, context)

        if self.ns.stat(request.path) is not None:
            # WORM: no se sobreescribe. Para reemplazar hay que borrar
            # primero, y eso incluye limpiar una subida que quedo a medias.
            self._fallar(context, "ya existe")
        padre, _ = self.ns.parent_of(request.path)
        if padre is None or not padre.is_dir:
            self._fallar(context, "el directorio padre no existe")

        vivos = self.vivos()
        if not vivos:
            context.abort(grpc.StatusCode.UNAVAILABLE,
                          "no hay DataNodes vivos")

        # El ultimo bloque es parcial. Con un contador explicito, y no
        # dividiendo, no hay forma de equivocarse en su tamano.
        asignaciones = []
        restante = request.size
        indice = 0
        while restante > 0:
            tam = min(BLOCK_SIZE, restante)
            block_id = uuid.uuid4().hex
            destinos = self.placer.place(block_id, vivos, REPLICATION_FACTOR)
            self.block_map[block_id] = {
                "index": indice, "size": tam,
                "datanodes": destinos, "sha256": ""}
            asignaciones.append(dfsha_pb2.BlockAssignment(
                block_id=block_id, index=indice, size=tam,
                datanodes=[self.datanodes[n]["addr"] for n in destinos],
                access_token=""))     # TODO semana 12: firmado por el NameNode
            restante -= tam
            indice += 1

        lease_id = uuid.uuid4().hex
        ok, msg = self.ns.create(
            request.path, request.size,
            [a.block_id for a in asignaciones], lease_id)
        if not ok:
            for a in asignaciones:
                self.block_map.pop(a.block_id, None)
            self._fallar(context, msg)

        self.leases[lease_id] = request.path
        print("[Create] {}  {} bytes  {} bloques  lease={}".format(
            request.path, request.size, len(asignaciones), lease_id[:8]))
        return dfsha_pb2.CreateResponse(
            lease_id=lease_id, blocks=asignaciones)

    def Complete(self, request, context):
        """El commit. Aqui el archivo se vuelve visible e inmutable.

        CompleteRequest no lleva token: el lease ES la credencial. Se
        entrego a un usuario ya autenticado en el Create y solo sirve
        para ese path, asi que vale como capability.
        """
        nodo = self.ns.stat(request.path)
        if nodo is not None and nodo.state == UNDER_CONSTRUCTION:
            # Verifica que llegue el checksum de cada bloque antes de
            # dar el archivo por bueno.
            llegaron = {c.block_id for c in request.checksums}
            faltan = [b for b in nodo.blocks if b not in llegaron]
            if faltan:
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "faltan los checksums de {} bloques".format(len(faltan)))

        ok, msg = self.ns.complete(request.path, request.lease_id)
        if not ok:
            print("[Complete] {} -> {}".format(request.path, msg))
            self._fallar(context, msg)

        for c in request.checksums:
            if c.block_id in self.block_map:
                self.block_map[c.block_id]["sha256"] = c.sha256
        self.leases.pop(request.lease_id, None)
        print("[Complete] {} -> COMMITTED".format(request.path))
        return dfsha_pb2.StatusResponse(ok=True, message="ok")

    def Abort(self, request, context):
        self._autorizar(request.token, context)
        path = self.leases.get(request.lease_id)
        if path is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "lease desconocido")

        ok, msg, huerfanos = self.ns.abort(path, request.lease_id)
        self.leases.pop(request.lease_id, None)
        if not ok:
            self._fallar(context, msg)
        self._agendar_borrado(huerfanos)
        print("[Abort] {} cancelado".format(path))
        return dfsha_pb2.StatusResponse(ok=True, message="ok")

    # ---------------- RF2: lectura ----------------

    def Open(self, request, context):
        """Donde esta cada bloque, en orden. El cliente hace el resto."""
        self._autorizar(request.token, context)
        nodo = self.ns.stat(request.path)
        if nodo is None:
            self._fallar(context, "no existe")
        if nodo.is_dir:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "es un directorio")
        if nodo.state != COMMITTED:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                          "el archivo todavia esta en construccion")

        vivos = set(self.vivos())
        ubicaciones = []
        for block_id in nodo.blocks:
            meta = self.block_map.get(block_id)
            if meta is None:
                context.abort(grpc.StatusCode.INTERNAL,
                              "no hay ubicacion para el bloque " + block_id)
            # Las direcciones se resuelven AL LEER, no al escribir: un
            # DataNode que se reinicio puede anunciarse en otra direccion.
            direcciones = [self.datanodes[n]["addr"]
                           for n in meta["datanodes"] if n in vivos]
            if not direcciones:
                context.abort(
                    grpc.StatusCode.UNAVAILABLE,
                    "ningun DataNode vivo tiene el bloque " + block_id)
            ubicaciones.append(dfsha_pb2.BlockLocation(
                block_id=block_id, index=meta["index"], size=meta["size"],
                datanodes=direcciones, sha256=meta["sha256"],
                access_token=""))

        ubicaciones.sort(key=lambda b: b.index)
        print("[Open] {}  {} bloques".format(request.path, len(ubicaciones)))
        return dfsha_pb2.OpenResponse(size=nodo.size, blocks=ubicaciones)


class ControlService(dfsha_pb2_grpc.ControlServiceServicer):
    """El DataNode siempre inicia la conexion. El NameNode nunca al reves."""

    def __init__(self, nn):
        self.nn = nn

    def Heartbeat(self, request, context):
        anterior = self.nn.datanodes.get(request.node_id)
        self.nn.datanodes[request.node_id] = {
            "addr": request.addr,
            "free_bytes": request.free_bytes,
            "num_blocks": request.num_blocks,
            "last_seen": time.time(),
        }
        if anterior is None:
            print("[Heartbeat] nuevo DataNode: {} en {}".format(
                request.node_id, request.addr))
        elif anterior.get("last_seen", 0) <= time.time() - TIMEOUT_DATANODE:
            print("[Heartbeat] {} revivio".format(request.node_id))
        # TODO semana 11: aqui van los comandos piggyback (replicar, borrar)
        return dfsha_pb2.HeartbeatResponse()

    def BlockReceived(self, request, context):
        """El DataNode avisa que ya tiene un bloque.

        Sirve para confirmar que quedo donde el NameNode esperaba: si un
        nodo reporta un bloque que no se le asigno, se anota igual, porque
        el que manda es el disco.
        """
        meta = self.nn.block_map.get(request.block_id)
        if meta is None:
            print("[BlockReceived] {} reporto un bloque desconocido: {}".format(
                request.node_id, request.block_id))
            return dfsha_pb2.StatusResponse(ok=True, message="bloque sin dueno")
        if request.node_id not in meta["datanodes"]:
            meta["datanodes"].append(request.node_id)
        if request.sha256 and not meta["sha256"]:
            meta["sha256"] = request.sha256
        return dfsha_pb2.StatusResponse(ok=True, message="ok")

    def BlockReport(self, request, context):
        """La lista completa de bloques de un DataNode.

        Esto es lo que permite NO persistir las ubicaciones: el namespace
        es lo que hay que hacer durable (semana 12, con Raft), pero el
        mapa blockID -> DataNodes se reconstruye solo con los reports al
        arrancar. Es lo que hace HDFS, y por eso el NameNode puede
        reiniciarse sin saber donde estaba nada.
        """
        reportados = set(request.block_ids)
        conocidos = 0
        for block_id, meta in self.nn.block_map.items():
            if block_id in reportados:
                conocidos += 1
                if request.node_id not in meta["datanodes"]:
                    meta["datanodes"].append(request.node_id)
            elif request.node_id in meta["datanodes"]:
                # El nodo ya no lo tiene: dejo de contar como replica.
                meta["datanodes"].remove(request.node_id)

        huerfanos = [b for b in reportados if b not in self.nn.block_map]
        print("[BlockReport] {}: {} bloques ({} conocidos, {} huerfanos)".format(
            request.node_id, len(reportados), conocidos, len(huerfanos)))
        # Los huerfanos son de archivos ya borrados: a la cola de borrado.
        self.nn._agendar_borrado(huerfanos)
        return dfsha_pb2.StatusResponse(ok=True, message="ok")


def vigilar(nn):
    """Anuncia por consola cuando un DataNode pasa a muerto o revive.

    No decide nada: vivos() ya filtra por last_seen cada vez que se
    consulta. Esto es para verlo en la demo.
    """
    caidos = set()
    while True:
        time.sleep(INTERVALO_VIGILANCIA)
        ahora = set(nn.muertos())
        for nid in ahora - caidos:
            print("[vigilancia] {} no responde hace mas de {} s".format(
                nid, TIMEOUT_DATANODE))
        for nid in caidos - ahora:
            print("[vigilancia] {} volvio".format(nid))
        caidos = ahora


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    nn = NameNodeService()
    threading.Thread(target=vigilar, args=(nn,), daemon=True).start()
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
