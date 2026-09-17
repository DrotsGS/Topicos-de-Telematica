"""Pruebas de punta a punta: NameNode + DataNodes + cliente de verdad.

Levanta el sistema completo en puertos efimeros y usa las mismas
funciones que el CLI, no una copia. Con bloques de 64 KB para que un
archivo de pocos cientos de KB ya tenga varios bloques y un ultimo
bloque parcial, que es donde se esconden los errores.
"""

import os
import sys
import types
import hashlib
from concurrent import futures

import grpc
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import dfsha_pb2, dfsha_pb2_grpc     # noqa: E402
from common.interfaces import FileAuth              # noqa: E402
from namenode import server as nn_server            # noqa: E402
from datanode import server as dn_server            # noqa: E402
from client import cli                              # noqa: E402

BLOQUE = 64 * 1024

USUARIOS = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "deploy", "usuarios.json")


class Sistema:
    """El cluster completo, en un proceso, con sus puertos efimeros."""

    def __init__(self, servicio, stub, token, datanodes, servidores, canales):
        self.servicio = servicio
        self.stub = stub
        self.token = token
        self.datanodes = datanodes      # node_id -> carpeta de bloques
        self._servidores = servidores
        self._canales = canales

    def cerrar(self):
        for c in self._canales:
            c.close()
        for s in self._servidores:
            s.stop(None)

    def bloques_en_disco(self):
        salida = {}
        for node_id, carpeta in self.datanodes.items():
            salida[node_id] = sorted(
                p.name for p in carpeta.glob("blk_*"))
        return salida


def _levantar_datanode(node_id, carpeta):
    """Un DataNode con su propia carpeta de bloques."""
    servicio = dn_server.DataNodeService(str(carpeta))
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    dfsha_pb2_grpc.add_DataNodeServiceServicer_to_server(servicio, servidor)
    puerto = servidor.add_insecure_port("localhost:0")
    servidor.start()
    return servidor, "localhost:{}".format(puerto)


@pytest.fixture
def sistema(tmp_path, monkeypatch, request):
    """NameNode + N DataNodes. Marca con @pytest.mark.datanodes(n)."""
    marca = request.node.get_closest_marker("datanodes")
    cuantos = marca.args[0] if marca else 1

    monkeypatch.setattr(nn_server, "BLOCK_SIZE", BLOQUE)
    monkeypatch.setattr(cli, "CHUNK_SIZE", 16 * 1024)

    servicio = nn_server.NameNodeService()
    servicio.auth = FileAuth(USUARIOS, "secreto-de-prueba")

    servidores, canales, carpetas, direcciones = [], [], {}, {}

    nn = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    dfsha_pb2_grpc.add_NameNodeServiceServicer_to_server(servicio, nn)
    dfsha_pb2_grpc.add_ControlServiceServicer_to_server(
        nn_server.ControlService(servicio), nn)
    puerto_nn = nn.add_insecure_port("localhost:0")
    nn.start()
    servidores.append(nn)

    for i in range(1, cuantos + 1):
        node_id = "dn-{}".format(i)
        carpeta = tmp_path / node_id
        carpeta.mkdir()
        carpetas[node_id] = carpeta
        servidor, direccion = _levantar_datanode(node_id, carpeta)
        servidores.append(servidor)
        direcciones[node_id] = direccion

    canal = grpc.insecure_channel("localhost:{}".format(puerto_nn))
    canales.append(canal)
    control = dfsha_pb2_grpc.ControlServiceStub(canal)
    for node_id, direccion in direcciones.items():
        # Se registran como lo hace un DataNode de verdad: por heartbeat.
        control.Heartbeat(dfsha_pb2.HeartbeatRequest(
            node_id=node_id, addr=direccion,
            free_bytes=10 ** 12, num_blocks=0))

    stub = dfsha_pb2_grpc.NameNodeServiceStub(canal)
    token = stub.Login(dfsha_pb2.LoginRequest(
        user="drots", password="dfsha")).token
    monkeypatch.setattr(cli, "token", lambda: token)

    s = Sistema(servicio, stub, token, carpetas, servidores, canales)
    yield s
    s.cerrar()


def args(**kw):
    return types.SimpleNamespace(**kw)


def archivo(tmp_path, nombre, tam):
    ruta = tmp_path / nombre
    ruta.write_bytes(os.urandom(tam))
    return ruta


def sha(ruta):
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for trozo in iter(lambda: f.read(1 << 20), b""):
            h.update(trozo)
    return h.hexdigest()


# ---------------- el hito 1 ----------------

@pytest.mark.parametrize("tam", [
    1,                  # un byte
    BLOQUE - 1,         # justo por debajo de un bloque
    BLOQUE,             # exacto: ningun bloque parcial
    BLOQUE + 1,         # un bloque lleno y uno de 1 byte
    3 * BLOQUE + 777,   # varios bloques y un ultimo parcial
])
def test_subir_borrar_bajar_y_comparar_hash(sistema, tmp_path, tam):
    """La definicion de listo de la semana 8, en pequeno.

    Sube, borra el local, baja y compara el sha256. El parametro cubre
    el error que mas cuesta: el tamano del ultimo bloque.
    """
    origen = archivo(tmp_path, "origen.bin", tam)
    esperado = sha(origen)

    cli.cmd_put(sistema.stub, args(local=str(origen), remoto="/f.bin"))
    origen.unlink()                       # se borra del disco local

    destino = tmp_path / "bajado.bin"
    cli.cmd_get(sistema.stub, args(remoto="/f.bin", local=str(destino)))

    assert destino.stat().st_size == tam
    assert sha(destino) == esperado


def test_un_archivo_vacio(sistema, tmp_path):
    origen = tmp_path / "vacio.bin"
    origen.write_bytes(b"")
    cli.cmd_put(sistema.stub, args(local=str(origen), remoto="/vacio.bin"))

    info = sistema.stub.Stat(dfsha_pb2.PathRequest(
        path="/vacio.bin", token=sistema.token))
    assert info.num_blocks == 0 and info.size == 0

    destino = tmp_path / "bajado.bin"
    cli.cmd_get(sistema.stub, args(remoto="/vacio.bin", local=str(destino)))
    assert destino.read_bytes() == b""


def test_los_bloques_quedan_en_disco_del_datanode(sistema, tmp_path):
    origen = archivo(tmp_path, "origen.bin", 2 * BLOQUE + 10)
    cli.cmd_put(sistema.stub, args(local=str(origen), remoto="/f.bin"))
    assert len(sistema.bloques_en_disco()["dn-1"]) == 3


# ---------------- WORM ----------------

def test_no_se_sobreescribe(sistema, tmp_path):
    origen = archivo(tmp_path, "o.bin", 100)
    cli.cmd_put(sistema.stub, args(local=str(origen), remoto="/f.bin"))
    with pytest.raises(grpc.RpcError) as e:
        sistema.stub.Create(dfsha_pb2.CreateRequest(
            path="/f.bin", size=100, token=sistema.token))
    assert e.value.code() == grpc.StatusCode.ALREADY_EXISTS


def test_invisible_hasta_el_complete(sistema):
    """Mientras sube, el archivo no aparece en ls; stat si lo ve."""
    r = sistema.stub.Create(dfsha_pb2.CreateRequest(
        path="/subiendo.bin", size=10, token=sistema.token))
    assert r.lease_id

    P = dfsha_pb2.PathRequest(path="/", token=sistema.token)
    assert list(sistema.stub.Ls(P).entries) == []

    info = sistema.stub.Stat(dfsha_pb2.PathRequest(
        path="/subiendo.bin", token=sistema.token))
    assert info.state == dfsha_pb2.UNDER_CONSTRUCTION


def test_open_de_un_archivo_en_construccion(sistema):
    sistema.stub.Create(dfsha_pb2.CreateRequest(
        path="/a medias.bin", size=10, token=sistema.token))
    with pytest.raises(grpc.RpcError) as e:
        sistema.stub.Open(dfsha_pb2.PathRequest(
            path="/a medias.bin", token=sistema.token))
    assert e.value.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_complete_sin_los_checksums(sistema):
    r = sistema.stub.Create(dfsha_pb2.CreateRequest(
        path="/f.bin", size=BLOQUE * 2, token=sistema.token))
    with pytest.raises(grpc.RpcError) as e:
        sistema.stub.Complete(dfsha_pb2.CompleteRequest(
            path="/f.bin", lease_id=r.lease_id, checksums=[]))
    assert e.value.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_complete_con_un_lease_que_no_es(sistema):
    sistema.stub.Create(dfsha_pb2.CreateRequest(
        path="/f.bin", size=0, token=sistema.token))
    with pytest.raises(grpc.RpcError) as e:
        sistema.stub.Complete(dfsha_pb2.CompleteRequest(
            path="/f.bin", lease_id="inventado", checksums=[]))
    assert e.value.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_abort_libera_el_path(sistema):
    r = sistema.stub.Create(dfsha_pb2.CreateRequest(
        path="/f.bin", size=10, token=sistema.token))
    sistema.stub.Abort(dfsha_pb2.LeaseRequest(
        lease_id=r.lease_id, token=sistema.token))

    with pytest.raises(grpc.RpcError):
        sistema.stub.Stat(dfsha_pb2.PathRequest(
            path="/f.bin", token=sistema.token))
    assert sistema.servicio.pendientes_borrado == [
        b.block_id for b in r.blocks]
    assert sistema.servicio.leases == {}


# ---------------- D6 y D8: el cliente que se muere o al que le borran ----

def test_rm_durante_la_subida_invalida_el_lease(sistema):
    """D6 opcion (b) de punta a punta: el rm gana y el Complete falla."""
    r = sistema.stub.Create(dfsha_pb2.CreateRequest(
        path="/f.bin", size=10, token=sistema.token))

    sistema.stub.Rm(dfsha_pb2.PathRequest(path="/f.bin", token=sistema.token))
    assert sistema.servicio.leases == {}     # el lease murio con el nodo

    with pytest.raises(grpc.RpcError) as e:
        sistema.stub.Complete(dfsha_pb2.CompleteRequest(
            path="/f.bin", lease_id=r.lease_id, checksums=[]))
    assert e.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    # Y el path quedo libre para volver a usarlo.
    sistema.stub.Create(dfsha_pb2.CreateRequest(
        path="/f.bin", size=10, token=sistema.token))


def test_una_subida_fallida_cancela_el_lease(sistema, tmp_path, monkeypatch):
    """D8: si el cliente falla a mitad, hace Abort y no deja el path preso."""
    origen = archivo(tmp_path, "o.bin", BLOQUE * 2)

    def explota(*_a, **_k):
        raise RuntimeError("se cayo la red")

    monkeypatch.setattr(cli, "subir_bloque", explota)
    with pytest.raises(SystemExit):
        cli.cmd_put(sistema.stub, args(local=str(origen), remoto="/f.bin"))

    assert sistema.servicio.leases == {}
    with pytest.raises(grpc.RpcError):
        sistema.stub.Stat(dfsha_pb2.PathRequest(
            path="/f.bin", token=sistema.token))


# ---------------- errores del plano de control ----------------

def test_create_sin_datanodes_vivos(sistema):
    sistema.servicio.datanodes.clear()
    with pytest.raises(grpc.RpcError) as e:
        sistema.stub.Create(dfsha_pb2.CreateRequest(
            path="/f.bin", size=10, token=sistema.token))
    assert e.value.code() == grpc.StatusCode.UNAVAILABLE


def test_create_sin_directorio_padre(sistema):
    with pytest.raises(grpc.RpcError) as e:
        sistema.stub.Create(dfsha_pb2.CreateRequest(
            path="/no/existe/f.bin", size=10, token=sistema.token))
    assert e.value.code() == grpc.StatusCode.NOT_FOUND


def test_open_de_un_directorio(sistema):
    sistema.stub.Mkdir(dfsha_pb2.PathRequest(path="/d", token=sistema.token))
    with pytest.raises(grpc.RpcError) as e:
        sistema.stub.Open(dfsha_pb2.PathRequest(
            path="/d", token=sistema.token))
    assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_el_archivo_subido_aparece_en_ls_con_su_tamano(sistema, tmp_path):
    origen = archivo(tmp_path, "o.bin", 1234)
    sistema.stub.Mkdir(dfsha_pb2.PathRequest(path="/d", token=sistema.token))
    cli.cmd_put(sistema.stub, args(local=str(origen), remoto="/d/o.bin"))

    entradas = list(sistema.stub.Ls(dfsha_pb2.PathRequest(
        path="/d", token=sistema.token)).entries)
    assert len(entradas) == 1
    assert entradas[0].name == "o.bin" and entradas[0].size == 1234
