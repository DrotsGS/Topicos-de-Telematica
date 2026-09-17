"""Pruebas del ConsistentHashPlacer.

Las dos primeras son material directo para el informe: miden lo que el
hash consistente promete (que agregar un nodo mueve pocas claves y que
el reparto queda balanceado) en vez de solo afirmarlo.
"""

import os
import sys
import uuid
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.interfaces import ConsistentHashPlacer   # noqa: E402

CLAVES = 10000


def test_agregar_nodo_mueve_pocas_claves():
    claves = [uuid.uuid4().hex for _ in range(CLAVES)]
    p = ConsistentHashPlacer()

    antes = {k: p.place(k, ["dn1", "dn2", "dn3", "dn4"], 1)[0] for k in claves}
    despues = {k: p.place(k, ["dn1", "dn2", "dn3", "dn4", "dn5"], 1)[0]
               for k in claves}

    movidas = sum(1 for k in claves if antes[k] != despues[k])
    fraccion = movidas / len(claves)
    print("\nclaves movidas al pasar de 4 a 5 nodos: {:.1%} "
          "(con modulo serian ~80%)".format(fraccion))
    # Con modulo se moveria cerca del 80%. Con anillo, cerca de 1/5.
    assert fraccion < 0.30


def test_reparto_balanceado():
    p = ConsistentHashPlacer()
    nodos = ["dn1", "dn2", "dn3", "dn4"]
    c = Counter(p.place(uuid.uuid4().hex, nodos, 1)[0] for _ in range(CLAVES))
    print("\nreparto entre 4 nodos: {}".format(
        {n: "{:.1%}".format(c[n] / CLAVES) for n in nodos}))
    for n in nodos:
        assert 0.18 < c[n] / CLAVES < 0.32     # ~25% cada uno


def test_quitar_un_nodo_solo_mueve_lo_suyo():
    claves = [uuid.uuid4().hex for _ in range(CLAVES)]
    p = ConsistentHashPlacer()
    antes = {k: p.place(k, ["dn1", "dn2", "dn3", "dn4"], 1)[0] for k in claves}
    despues = {k: p.place(k, ["dn1", "dn2", "dn3"], 1)[0] for k in claves}

    # Lo que no estaba en dn4 no tiene por que haberse movido.
    quietas = [k for k in claves if antes[k] != "dn4"]
    assert all(antes[k] == despues[k] for k in quietas)


def test_replicas_en_nodos_fisicos_distintos():
    """Si no se filtran los vnodos, las 3 replicas caen en la misma maquina."""
    p = ConsistentHashPlacer()
    nodos = ["dn1", "dn2", "dn3", "dn4"]
    for _ in range(500):
        destinos = p.place(uuid.uuid4().hex, nodos, 3)
        assert len(destinos) == 3
        assert len(set(destinos)) == 3


def test_pide_mas_replicas_que_nodos():
    p = ConsistentHashPlacer()
    assert p.place("b", ["dn1", "dn2"], 3) == p.place("b", ["dn1", "dn2"], 2)
    assert len(p.place("b", ["dn1", "dn2"], 3)) == 2


def test_sin_nodos_vivos():
    p = ConsistentHashPlacer()
    assert p.place("b", [], 3) == []


def test_es_determinista():
    nodos = ["dn1", "dn2", "dn3"]
    a = ConsistentHashPlacer()
    b = ConsistentHashPlacer(nodos)
    for _ in range(200):
        clave = uuid.uuid4().hex
        assert a.place(clave, nodos, 2) == b.place(clave, nodos, 2)


def test_un_nodo_que_vuelve_recupera_sus_claves():
    """Apagar y prender un nodo no deja el anillo distinto."""
    claves = [uuid.uuid4().hex for _ in range(1000)]
    p = ConsistentHashPlacer()
    nodos = ["dn1", "dn2", "dn3"]
    antes = {k: p.place(k, nodos, 1)[0] for k in claves}
    for k in claves:
        p.place(k, ["dn1", "dn2"], 1)          # dn3 se cae
    despues = {k: p.place(k, nodos, 1)[0] for k in claves}   # y vuelve
    assert antes == despues
