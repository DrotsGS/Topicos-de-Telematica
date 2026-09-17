"""Namespace en memoria.

Semana 7 lo terminas. Semana 12 lo pones detras de Raft sin cambiar
esta interfaz.
"""

import time
from dataclasses import dataclass, field

UNDER_CONSTRUCTION = 0
COMMITTED = 1


@dataclass
class Node:
    name: str
    is_dir: bool
    size: int = 0
    state: int = COMMITTED
    created_at: float = field(default_factory=time.time)
    children: dict = field(default_factory=dict)   # solo si is_dir
    blocks: list = field(default_factory=list)     # solo si es archivo


class Namespace:
    def __init__(self):
        self.root = Node(name="", is_dir=True)

    @staticmethod
    def _split(path):
        return [p for p in path.replace("\\", "/").strip("/").split("/") if p]

    def resolve(self, path):
        node = self.root
        for part in self._split(path):
            if not node.is_dir or part not in node.children:
                return None
            node = node.children[part]
        return node

    def parent_of(self, path):
        parts = self._split(path)
        if not parts:
            return None, ""
        return self.resolve("/".join(parts[:-1])), parts[-1]

    # ---------------- RF1 ----------------

    def mkdir(self, path):
        parent, name = self.parent_of(path)
        if parent is None or not parent.is_dir:
            return False, "el directorio padre no existe"
        if name in parent.children:
            return False, "ya existe"
        parent.children[name] = Node(name=name, is_dir=True)
        return True, "ok"

    def ls(self, path):
        node = self.resolve(path)
        if node is None or not node.is_dir:
            return None
        # Los archivos UNDER_CONSTRUCTION no se listan. Eso es el WORM:
        # un archivo o no existe, o existe completo.
        return [c for c in node.children.values()
                if c.is_dir or c.state == COMMITTED]

    # TODO semana 7: rmdir(path)
    #   Decide: error si el directorio no esta vacio, o borrado recursivo?
    #   Sea cual sea, va al log de decisiones.

    # TODO semana 7: rm(path)
    #   Pregunta dificil: que pasa si borran un archivo que otro esta
    #   subiendo (UNDER_CONSTRUCTION con lease activo)? Esa es tu primera
    #   decision real de consistencia.

    # TODO semana 7: stat(path)
    # TODO semana 8: create / complete / abort / open
