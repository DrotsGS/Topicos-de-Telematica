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

from common.pb import comun, nn, nn_grpc, dn, dn_grpc, ctl, ctl_grpc  # noqa: E402
from common.config import env, data_dir           # noqa: E402
from common.interfaces import CHUNK_SIZE          # noqa: E402

NODE_ID = env("NODE_ID", "dn-1")
PORT = env("PORT", "50060")
ADDR = env("ADVERTISE_ADDR", "localhost:" + PORT)
NAMENODE = env("NAMENODE_ADDR", "localhost:50051")
DATA_DIR = data_dir(NODE_ID)


class DataNodeService(dn_grpc.DataNodeServiceServicer):
    """El almacen de bloques.

    La carpeta se recibe por parametro y no se lee del entorno aqui
    dentro: asi las pruebas pueden levantar varios DataNodes en un mismo
    proceso, cada uno con su disco.
    """

    def __init__(self, carpeta=None, avisar=None):
        self.carpeta = carpeta or DATA_DIR
        # Callback para avisarle al NameNode que llego un bloque. Se
        # inyecta para que las pruebas puedan levantar un DataNode sin
        # NameNode detras.
        self.avisar = avisar

    def bloques(self):
        return [f[4:] for f in os.listdir(self.carpeta)
                if f.startswith("blk_")]

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
            if self.avisar is not None:
                # BlockReceived: el NameNode se entera por el DataNode, no
                # por el cliente. El que sabe que el bloque esta en disco
                # es quien lo escribio.
                self.avisar(header.block_id, sha)
            # TODO semana 11: reenviar a header.pipeline[0] si viene lleno
            return dn.PutBlockResponse(ok=True, sha256=sha, message="ok")

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
                yield dn.BlockChunk(data=datos)

    # TODO semana 11: el recolector de basura lo llama por comando piggyback.
    def DeleteBlock(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("DeleteBlock llega en la semana 11")
        return comun.StatusResponse()

    # TODO semana 11: ReplicateTo, para el pipeline DataNode -> DataNode


INTERVALO_HEARTBEAT = 3
INTERVALO_BLOCK_REPORT = 60


def control_stub():
    return ctl_grpc.ControlServiceStub(grpc.insecure_channel(NAMENODE))


def heartbeat_loop(servicio):
    """El DataNode reporta su propia direccion.

    Solo el es capaz de saber por donde lo alcanzan los clientes: en
    Windows es localhost, en Docker el nombre del servicio, en EC2 una
    IP privada. Por eso el NameNode no la adivina, se la preguntan.
    """
    stub = control_stub()
    while True:
        try:
            free = shutil.disk_usage(servicio.carpeta).free
            resp = stub.Heartbeat(ctl.HeartbeatRequest(
                node_id=NODE_ID, addr=ADDR, free_bytes=free,
                num_blocks=len(servicio.bloques())))
            for cmd in resp.commands:
                print("[comando piggyback] {}".format(cmd))   # TODO semana 11
        except grpc.RpcError as e:
            print("[heartbeat] NameNode no responde: {}".format(e.code().name))
        time.sleep(INTERVALO_HEARTBEAT)


def block_report_loop(servicio):
    """La lista completa de bloques, al arrancar y cada minuto.

    Con esto el NameNode reconstruye el mapa de ubicaciones sin tener que
    persistirlo: si se reinicia, en un minuto sabe donde esta todo.
    """
    stub = control_stub()
    while True:
        try:
            ids = servicio.bloques()
            stub.BlockReport(ctl.BlockReportRequest(
                node_id=NODE_ID, block_ids=ids))
            print("[BlockReport] se reportaron {} bloques".format(len(ids)))
        except grpc.RpcError as e:
            print("[BlockReport] no se pudo reportar: {}".format(e.code().name))
        time.sleep(INTERVALO_BLOCK_REPORT)


def avisar_bloque_recibido(block_id, sha):
    try:
        control_stub().BlockReceived(ctl.BlockReceivedRequest(
            node_id=NODE_ID, block_id=block_id, sha256=sha))
    except grpc.RpcError as e:
        # Que falle el aviso no invalida el bloque: el proximo
        # BlockReport lo vuelve a contar.
        print("[BlockReceived] no se pudo avisar: {}".format(e.code().name))


def serve():
    os.makedirs(DATA_DIR, exist_ok=True)
    servicio = DataNodeService(DATA_DIR, avisar=avisar_bloque_recibido)
    threading.Thread(target=heartbeat_loop, args=(servicio,),
                     daemon=True).start()
    threading.Thread(target=block_report_loop, args=(servicio,),
                     daemon=True).start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dn_grpc.add_DataNodeServiceServicer_to_server(servicio, server)
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
