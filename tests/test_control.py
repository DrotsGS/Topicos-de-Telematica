"""Plano de control: deteccion de nodos muertos, BlockReport y BlockReceived."""

import os
import sys
import time
from concurrent import futures

import grpc
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import dfsha_pb2, dfsha_pb2_grpc     # noqa: E402
from common.interfaces import FileAuth              # noqa: E402
from namenode import server as nn_server            # noqa: E402

USUARIOS = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "deploy", "usuarios.json")


@pytest.fixture
def control():
    """NameNode con su ControlService, sin DataNodes de verdad."""
    servicio = nn_server.NameNodeService()
    servicio.auth = FileAuth(USUARIOS, "secreto-de-prueba")

    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    dfsha_pb2_grpc.add_NameNodeServiceServicer_to_server(servicio, servidor)
    dfsha_pb2_grpc.add_ControlServiceServicer_to_server(
        nn_server.ControlService(servicio), servidor)
    puerto = servidor.add_insecure_port("localhost:0")
    servidor.start()

    canal = grpc.insecure_channel("localhost:{}".format(puerto))
    yield (dfsha_pb2_grpc.ControlServiceStub(canal),
           dfsha_pb2_grpc.NameNodeServiceStub(canal),
           servicio)
    canal.close()
    servidor.stop(None)


def latir(stub, node_id, direccion=None):
    return stub.Heartbeat(dfsha_pb2.HeartbeatRequest(
        node_id=node_id, addr=direccion or (node_id + ":50060"),
        free_bytes=10 ** 9, num_blocks=0))


# ---------------- vivos y muertos ----------------

def test_un_nodo_que_late_esta_vivo(control):
    ctrl, _, servicio = control
    latir(ctrl, "dn-1")
    assert servicio.vivos() == ["dn-1"]
    assert servicio.muertos() == []


def test_un_nodo_que_deja_de_latir_se_declara_muerto(control, monkeypatch):
    ctrl, _, servicio = control
    latir(ctrl, "dn-1")
    latir(ctrl, "dn-2")

    # En vez de esperar 30 s, se envejece el ultimo latido de dn-1.
    servicio.datanodes["dn-1"]["last_seen"] -= nn_server.TIMEOUT_DATANODE + 1
    assert servicio.vivos() == ["dn-2"]
    assert servicio.muertos() == ["dn-1"]


def test_un_nodo_muerto_revive_al_volver_a_latir(control):
    ctrl, _, servicio = control
    latir(ctrl, "dn-1")
    servicio.datanodes["dn-1"]["last_seen"] -= nn_server.TIMEOUT_DATANODE + 1
    assert servicio.muertos() == ["dn-1"]
    latir(ctrl, "dn-1")
    assert servicio.vivos() == ["dn-1"]


def test_create_no_asigna_bloques_a_un_nodo_muerto(control):
    """El cambio de una linea con consecuencias grandes."""
    ctrl, nn, servicio = control
    latir(ctrl, "dn-1")
    latir(ctrl, "dn-2")
    servicio.datanodes["dn-1"]["last_seen"] -= nn_server.TIMEOUT_DATANODE + 1

    token = nn.Login(dfsha_pb2.LoginRequest(
        user="drots", password="dfsha")).token
    r = nn.Create(dfsha_pb2.CreateRequest(
        path="/f.bin", size=1000, token=token))
    for b in r.blocks:
        assert list(b.datanodes) == ["dn-2:50060"]


def test_el_heartbeat_actualiza_la_direccion(control):
    """Un DataNode reiniciado puede anunciarse en otro sitio."""
    ctrl, _, servicio = control
    latir(ctrl, "dn-1", "10.0.1.5:50060")
    latir(ctrl, "dn-1", "10.0.1.9:50060")
    assert servicio.datanodes["dn-1"]["addr"] == "10.0.1.9:50060"


# ---------------- BlockReceived ----------------

def test_block_received_agrega_la_replica(control):
    ctrl, _, servicio = control
    servicio.block_map["b1"] = {
        "index": 0, "size": 10, "datanodes": [], "sha256": ""}
    ctrl.BlockReceived(dfsha_pb2.BlockReceivedRequest(
        node_id="dn-1", block_id="b1", sha256="abc"))
    assert servicio.block_map["b1"]["datanodes"] == ["dn-1"]
    assert servicio.block_map["b1"]["sha256"] == "abc"


def test_block_received_no_duplica(control):
    ctrl, _, servicio = control
    servicio.block_map["b1"] = {
        "index": 0, "size": 10, "datanodes": ["dn-1"], "sha256": ""}
    ctrl.BlockReceived(dfsha_pb2.BlockReceivedRequest(
        node_id="dn-1", block_id="b1", sha256=""))
    assert servicio.block_map["b1"]["datanodes"] == ["dn-1"]


def test_block_received_de_un_bloque_desconocido_no_revienta(control):
    ctrl, _, _ = control
    r = ctrl.BlockReceived(dfsha_pb2.BlockReceivedRequest(
        node_id="dn-1", block_id="fantasma", sha256=""))
    assert r.ok


# ---------------- BlockReport ----------------

def test_block_report_reconstruye_las_ubicaciones(control):
    """Esto es lo que permite NO persistir el mapa de ubicaciones."""
    ctrl, _, servicio = control
    for bid in ("b1", "b2"):
        servicio.block_map[bid] = {
            "index": 0, "size": 10, "datanodes": [], "sha256": ""}

    ctrl.BlockReport(dfsha_pb2.BlockReportRequest(
        node_id="dn-1", block_ids=["b1", "b2"]))
    assert servicio.block_map["b1"]["datanodes"] == ["dn-1"]
    assert servicio.block_map["b2"]["datanodes"] == ["dn-1"]


def test_block_report_quita_lo_que_el_nodo_ya_no_tiene(control):
    ctrl, _, servicio = control
    servicio.block_map["b1"] = {
        "index": 0, "size": 10, "datanodes": ["dn-1", "dn-2"], "sha256": ""}
    ctrl.BlockReport(dfsha_pb2.BlockReportRequest(
        node_id="dn-1", block_ids=[]))       # dn-1 perdio su disco
    assert servicio.block_map["b1"]["datanodes"] == ["dn-2"]


def test_block_report_agenda_los_bloques_sin_dueno(control):
    """Bloques de archivos ya borrados: a la cola del recolector."""
    ctrl, _, servicio = control
    ctrl.BlockReport(dfsha_pb2.BlockReportRequest(
        node_id="dn-1", block_ids=["viejo1", "viejo2"]))
    assert sorted(servicio.pendientes_borrado) == ["viejo1", "viejo2"]
