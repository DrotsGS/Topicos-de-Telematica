"""Pruebas del DataNode aislado, sin NameNode.

El DataNode es autonomo: si el streaming falla, el problema esta aqui y
no repartido en tres capas. Por eso se prueba primero y por separado.
"""

import os
import sys
import hashlib
from concurrent import futures

import grpc
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import dfsha_pb2, dfsha_pb2_grpc   # noqa: E402
from datanode import server as dn                  # noqa: E402

CHUNK = 64 * 1024


@pytest.fixture
def datanodo(tmp_path, monkeypatch):
    """Un DataNode de verdad, con sus bloques en un directorio temporal."""
    monkeypatch.setattr(dn, "DATA_DIR", str(tmp_path))
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    dfsha_pb2_grpc.add_DataNodeServiceServicer_to_server(
        dn.DataNodeService(), servidor)
    puerto = servidor.add_insecure_port("localhost:0")
    servidor.start()

    canal = grpc.insecure_channel("localhost:{}".format(puerto))
    yield dfsha_pb2_grpc.DataNodeServiceStub(canal), tmp_path

    canal.close()
    servidor.stop(None)


def subir(stub, block_id, datos, anunciar=None):
    """Manda header + chunks. anunciar permite mentir sobre el tamano."""
    size = len(datos) if anunciar is None else anunciar

    def stream():
        yield dfsha_pb2.BlockChunk(header=dfsha_pb2.BlockHeader(
            block_id=block_id, size=size))
        for i in range(0, len(datos), CHUNK):
            yield dfsha_pb2.BlockChunk(data=datos[i:i + CHUNK])

    return stub.PutBlock(stream())


def bajar(stub, block_id):
    return b"".join(c.data for c in stub.GetBlock(
        dfsha_pb2.GetBlockRequest(block_id=block_id)))


def test_ida_y_vuelta(datanodo):
    stub, _ = datanodo
    datos = os.urandom(300 * 1024)      # varios chunks, el ultimo parcial
    r = subir(stub, "b1", datos)
    assert r.ok
    assert r.sha256 == hashlib.sha256(datos).hexdigest()
    assert bajar(stub, "b1") == datos


def test_bloque_vacio(datanodo):
    stub, _ = datanodo
    r = subir(stub, "vacio", b"")
    assert r.ok and r.sha256 == hashlib.sha256(b"").hexdigest()
    assert bajar(stub, "vacio") == b""


def test_el_bloque_queda_en_disco_con_el_nombre_esperado(datanodo):
    stub, carpeta = datanodo
    subir(stub, "b2", b"hola")
    assert (carpeta / "blk_b2").read_bytes() == b"hola"


def test_get_de_un_bloque_que_no_existe(datanodo):
    stub, _ = datanodo
    with pytest.raises(grpc.RpcError) as e:
        bajar(stub, "fantasma")
    assert e.value.code() == grpc.StatusCode.NOT_FOUND


def test_faltan_bytes_respecto_al_header(datanodo):
    stub, carpeta = datanodo
    with pytest.raises(grpc.RpcError) as e:
        subir(stub, "corto", b"abc", anunciar=100)
    assert e.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert not (carpeta / "blk_corto").exists()     # no quedo a medias


def test_sobran_bytes_respecto_al_header(datanodo):
    stub, carpeta = datanodo
    with pytest.raises(grpc.RpcError) as e:
        subir(stub, "largo", b"abcdef", anunciar=2)
    assert e.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert not (carpeta / "blk_largo").exists()


def test_stream_sin_header(datanodo):
    stub, _ = datanodo

    def stream():
        yield dfsha_pb2.BlockChunk(data=b"sin header")

    with pytest.raises(grpc.RpcError) as e:
        stub.PutBlock(stream())
    assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_una_subida_fallida_no_deja_temporales(datanodo):
    stub, carpeta = datanodo
    with pytest.raises(grpc.RpcError):
        subir(stub, "sucio", b"abc", anunciar=999)
    assert list(carpeta.glob("tmp_*")) == []


def test_delete_block_sigue_sin_implementarse(datanodo):
    """Hasta la semana 11 el recolector no existe: que se note."""
    stub, _ = datanodo
    with pytest.raises(grpc.RpcError) as e:
        stub.DeleteBlock(dfsha_pb2.BlockRef(block_id="b1"))
    assert e.value.code() == grpc.StatusCode.UNIMPLEMENTED
