"""Pruebas del NameNode por gRPC real, en un puerto efimero.

Verifican lo que las del namespace no pueden: que cada situacion llegue
al cliente con el StatusCode correcto. Esa tabla es la parte que el
curso evalua como uso del middleware.
"""

import os
import sys
from concurrent import futures

import grpc
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import dfsha_pb2, dfsha_pb2_grpc     # noqa: E402
from common.interfaces import FileAuth              # noqa: E402
from namenode import server as nn_server            # noqa: E402
from namenode.namespace import Node, UNDER_CONSTRUCTION   # noqa: E402

USUARIOS = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "deploy", "usuarios.json")


@pytest.fixture
def nodo():
    """Levanta un NameNode de verdad y devuelve (stub, servicio)."""
    servicio = nn_server.NameNodeService()
    servicio.auth = FileAuth(USUARIOS, "secreto-de-prueba")

    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    dfsha_pb2_grpc.add_NameNodeServiceServicer_to_server(servicio, servidor)
    puerto = servidor.add_insecure_port("localhost:0")   # efimero
    servidor.start()

    canal = grpc.insecure_channel("localhost:{}".format(puerto))
    yield dfsha_pb2_grpc.NameNodeServiceStub(canal), servicio

    canal.close()
    servidor.stop(None)


@pytest.fixture
def sesion(nodo):
    """Igual, pero ya con el token de un login valido."""
    stub, servicio = nodo
    token = stub.Login(dfsha_pb2.LoginRequest(
        user="drots", password="dfsha")).token
    return stub, servicio, token


def codigo(excinfo):
    return excinfo.value.code()


# ---------------- login ----------------

def test_login_ok(nodo):
    stub, _ = nodo
    r = stub.Login(dfsha_pb2.LoginRequest(user="drots", password="dfsha"))
    assert r.token and r.expires_at > 0


def test_login_password_mala(nodo):
    stub, _ = nodo
    with pytest.raises(grpc.RpcError) as e:
        stub.Login(dfsha_pb2.LoginRequest(user="drots", password="nope"))
    assert codigo(e) == grpc.StatusCode.UNAUTHENTICATED


def test_login_usuario_inexistente(nodo):
    stub, _ = nodo
    with pytest.raises(grpc.RpcError) as e:
        stub.Login(dfsha_pb2.LoginRequest(user="nadie", password="x"))
    assert codigo(e) == grpc.StatusCode.UNAUTHENTICATED


# ---------------- las cinco RPCs rechazan un token invalido ----------------

@pytest.mark.parametrize("llamada", [
    lambda s, t: s.Mkdir(dfsha_pb2.PathRequest(path="/x", token=t)),
    lambda s, t: s.Ls(dfsha_pb2.PathRequest(path="/", token=t)),
    lambda s, t: s.Rmdir(dfsha_pb2.PathRequest(path="/x", token=t)),
    lambda s, t: s.Rm(dfsha_pb2.PathRequest(path="/x", token=t)),
    lambda s, t: s.Stat(dfsha_pb2.PathRequest(path="/", token=t)),
])
@pytest.mark.parametrize("token", ["", "basura", "fake-token:drots"])
def test_token_invalido_es_unauthenticated(nodo, llamada, token):
    stub, _ = nodo
    with pytest.raises(grpc.RpcError) as e:
        llamada(stub, token)
    assert codigo(e) == grpc.StatusCode.UNAUTHENTICATED


# ---------------- mkdir ----------------

def test_mkdir_duplicado_es_already_exists(sesion):
    stub, _, t = sesion
    stub.Mkdir(dfsha_pb2.PathRequest(path="/docs", token=t))
    with pytest.raises(grpc.RpcError) as e:
        stub.Mkdir(dfsha_pb2.PathRequest(path="/docs", token=t))
    assert codigo(e) == grpc.StatusCode.ALREADY_EXISTS


def test_mkdir_sin_padre_es_not_found(sesion):
    stub, _, t = sesion
    with pytest.raises(grpc.RpcError) as e:
        stub.Mkdir(dfsha_pb2.PathRequest(path="/a/b/c", token=t))
    assert codigo(e) == grpc.StatusCode.NOT_FOUND


# ---------------- la definicion de listo de la semana 7 ----------------

def test_recorrido_completo(sesion):
    """mkdir /docs, mkdir /docs/2026, stat, rmdir x3, ls vacio."""
    stub, _, t = sesion
    P = lambda p: dfsha_pb2.PathRequest(path=p, token=t)   # noqa: E731

    stub.Mkdir(P("/docs"))
    stub.Mkdir(P("/docs/2026"))

    info = stub.Stat(P("/docs"))
    assert info.is_dir and info.state == dfsha_pb2.COMMITTED

    with pytest.raises(grpc.RpcError) as e:
        stub.Rmdir(P("/docs"))                 # no esta vacio
    assert codigo(e) == grpc.StatusCode.FAILED_PRECONDITION

    assert stub.Rmdir(P("/docs/2026")).ok
    assert stub.Rmdir(P("/docs")).ok
    assert list(stub.Ls(P("/")).entries) == []


# ---------------- rmdir / rm: cada codigo ----------------

def test_rmdir_no_existe_es_not_found(sesion):
    stub, _, t = sesion
    with pytest.raises(grpc.RpcError) as e:
        stub.Rmdir(dfsha_pb2.PathRequest(path="/nada", token=t))
    assert codigo(e) == grpc.StatusCode.NOT_FOUND


def test_rmdir_sobre_archivo_es_invalid_argument(sesion):
    stub, servicio, t = sesion
    servicio.ns.root.children["f"] = Node(name="f", is_dir=False)
    with pytest.raises(grpc.RpcError) as e:
        stub.Rmdir(dfsha_pb2.PathRequest(path="/f", token=t))
    assert codigo(e) == grpc.StatusCode.INVALID_ARGUMENT


def test_rmdir_raiz_es_failed_precondition(sesion):
    stub, _, t = sesion
    with pytest.raises(grpc.RpcError) as e:
        stub.Rmdir(dfsha_pb2.PathRequest(path="/", token=t))
    assert codigo(e) == grpc.StatusCode.FAILED_PRECONDITION


def test_rm_sobre_directorio_es_invalid_argument(sesion):
    stub, _, t = sesion
    stub.Mkdir(dfsha_pb2.PathRequest(path="/d", token=t))
    with pytest.raises(grpc.RpcError) as e:
        stub.Rm(dfsha_pb2.PathRequest(path="/d", token=t))
    assert codigo(e) == grpc.StatusCode.INVALID_ARGUMENT


def test_rm_agenda_los_bloques_huerfanos(sesion):
    """El gancho de la semana 11: la cola se llena aunque nadie la vacie."""
    stub, servicio, t = sesion
    servicio.ns.root.children["f"] = Node(
        name="f", is_dir=False, blocks=["blk1", "blk2"])
    assert stub.Rm(dfsha_pb2.PathRequest(path="/f", token=t)).ok
    assert servicio.pendientes_borrado == ["blk1", "blk2"]


def test_rm_de_un_archivo_en_construccion(sesion):
    """D6 opcion (b): el rm gana, el path queda libre."""
    stub, servicio, t = sesion
    servicio.ns.root.children["f"] = Node(
        name="f", is_dir=False, state=UNDER_CONSTRUCTION,
        lease_id="lease-1", blocks=["blk9"])
    r = stub.Rm(dfsha_pb2.PathRequest(path="/f", token=t))
    assert r.ok and "subida en curso" in r.message
    assert servicio.pendientes_borrado == ["blk9"]


# ---------------- stat ----------------

def test_stat_no_existe_es_not_found(sesion):
    stub, _, t = sesion
    with pytest.raises(grpc.RpcError) as e:
        stub.Stat(dfsha_pb2.PathRequest(path="/nada", token=t))
    assert codigo(e) == grpc.StatusCode.NOT_FOUND


def test_stat_ve_un_archivo_en_construccion(sesion):
    stub, servicio, t = sesion
    servicio.ns.root.children["f"] = Node(
        name="f", is_dir=False, state=UNDER_CONSTRUCTION, size=42)
    info = stub.Stat(dfsha_pb2.PathRequest(path="/f", token=t))
    assert info.state == dfsha_pb2.UNDER_CONSTRUCTION and info.size == 42
    assert list(stub.Ls(dfsha_pb2.PathRequest(path="/", token=t)).entries) == []
