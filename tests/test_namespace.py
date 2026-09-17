"""Pruebas del namespace. Logica pura, sin red: corren en segundos."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from namenode.namespace import (                      # noqa: E402
    Namespace, Node, COMMITTED, UNDER_CONSTRUCTION)


# ---------------- mkdir / ls ----------------

def test_mkdir_anidado():
    ns = Namespace()
    assert ns.mkdir("/a")[0]
    assert ns.mkdir("/a/b")[0]
    assert ns.resolve("/a/b") is not None


def test_mkdir_sin_padre():
    ns = Namespace()
    ok, msg = ns.mkdir("/a/b/c")
    assert not ok and "padre" in msg


def test_mkdir_duplicado():
    ns = Namespace()
    ns.mkdir("/a")
    ok, msg = ns.mkdir("/a")
    assert not ok and msg == "ya existe"


def test_ls_oculta_under_construction():
    """Este test protege el WORM: si alguien rompe el filtro, se nota."""
    ns = Namespace()
    ns.mkdir("/d")
    ns.resolve("/d").children["x"] = Node(
        name="x", is_dir=False, state=UNDER_CONSTRUCTION)
    assert ns.ls("/d") == []


def test_ls_de_un_archivo_es_none():
    ns = Namespace()
    ns.mkdir("/d")
    ns.resolve("/d").children["x"] = Node(name="x", is_dir=False)
    assert ns.ls("/d/x") is None


# ---------------- rmdir ----------------

def test_rmdir_no_vacio():
    ns = Namespace()
    ns.mkdir("/a"); ns.mkdir("/a/b")
    ok, msg, _ = ns.rmdir("/a")
    assert not ok and "vacio" in msg


def test_rmdir_vacio():
    ns = Namespace()
    ns.mkdir("/a")
    ok, msg, huerfanos = ns.rmdir("/a")
    assert ok and huerfanos == [] and ns.resolve("/a") is None


def test_rmdir_recursivo():
    ns = Namespace()
    ns.mkdir("/a"); ns.mkdir("/a/b"); ns.mkdir("/a/b/c")
    ok, _, _ = ns.rmdir("/a", recursivo=True)
    assert ok and ns.resolve("/a") is None


def test_rmdir_recursivo_devuelve_los_bloques():
    ns = Namespace()
    ns.mkdir("/a"); ns.mkdir("/a/b")
    ns.resolve("/a/b").children["f"] = Node(
        name="f", is_dir=False, blocks=["blk1", "blk2"])
    ok, _, huerfanos = ns.rmdir("/a", recursivo=True)
    assert ok and sorted(huerfanos) == ["blk1", "blk2"]


def test_rmdir_no_existe():
    ns = Namespace()
    ok, msg, _ = ns.rmdir("/nada")
    assert not ok and msg == "no existe"


def test_rmdir_sobre_archivo():
    ns = Namespace()
    ns.root.children["f"] = Node(name="f", is_dir=False)
    ok, msg, _ = ns.rmdir("/f")
    assert not ok and "no es un directorio" in msg


def test_rmdir_raiz():
    ns = Namespace()
    ok, msg, _ = ns.rmdir("/")
    assert not ok and "raiz" in msg


# ---------------- rm ----------------

def test_rm_committed():
    ns = Namespace()
    ns.root.children["f"] = Node(
        name="f", is_dir=False, state=COMMITTED, blocks=["blk1"])
    ok, msg, huerfanos = ns.rm("/f")
    assert ok and huerfanos == ["blk1"] and ns.resolve("/f") is None


def test_rm_no_existe():
    ns = Namespace()
    ok, msg, _ = ns.rm("/nada")
    assert not ok and msg == "no existe"


def test_rm_sobre_directorio():
    ns = Namespace()
    ns.mkdir("/a")
    ok, msg, _ = ns.rm("/a")
    assert not ok and "rmdir" in msg


def test_rm_under_construction_gana_al_lease():
    """D6 opcion (b): el rm siempre funciona, el lease muere con el nodo."""
    ns = Namespace()
    ns.root.children["f"] = Node(
        name="f", is_dir=False, state=UNDER_CONSTRUCTION,
        lease_id="lease-123", blocks=["blk1", "blk2"])
    ok, msg, huerfanos = ns.rm("/f")
    assert ok
    assert ns.resolve("/f") is None          # el path queda libre
    assert huerfanos == ["blk1", "blk2"]     # y sus bloques, agendados
    assert "subida en curso" in msg


# ---------------- stat ----------------

def test_stat_ve_lo_que_ls_oculta():
    ns = Namespace()
    ns.mkdir("/d")
    ns.resolve("/d").children["x"] = Node(
        name="x", is_dir=False, state=UNDER_CONSTRUCTION)
    assert ns.ls("/d") == []
    assert ns.stat("/d/x") is not None


def test_stat_no_existe():
    ns = Namespace()
    assert ns.stat("/nada") is None
