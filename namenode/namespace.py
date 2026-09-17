"""Namespace en memoria.

Semana 7 lo terminas. Semana 12 lo pones detras de Raft sin cambiar
esta interfaz.

Las operaciones de borrado devuelven TRES valores: (ok, mensaje, huerfanos).
La tercera es la lista de block_id que quedaron sin dueno. Hoy el NameNode
solo la acumula en una cola; en la semana 11 esa cola se vacia mandando
DeleteCommand por piggyback en la respuesta al heartbeat.
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
    lease_id: str = ""                             # solo si UNDER_CONSTRUCTION


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

    def stat(self, path):
        """Devuelve el Node o None si no existe.

        A diferencia de ls, stat SI ve los archivos en construccion: es la
        unica forma de diagnosticar una subida que quedo a medias.
        """
        return self.resolve(path)

    def rmdir(self, path, recursivo=False):
        """Borra un directorio. D5: por defecto exige que este vacio.

        El parametro recursivo ya esta en la firma aunque el protocolo
        todavia no tenga como pedirlo (PathRequest no lleva el flag).
        Cuando se agregue al .proto, la logica no cambia.
        """
        nodo = self.resolve(path)
        if nodo is None:
            return False, "no existe", []
        if not nodo.is_dir:
            return False, "no es un directorio", []
        if nodo is self.root:
            return False, "no se puede borrar la raiz", []
        if nodo.children and not recursivo:
            return False, "el directorio no esta vacio", []

        huerfanos = self._recolectar_bloques(nodo)
        padre, nombre = self.parent_of(path)
        del padre.children[nombre]
        return True, "ok", huerfanos

    def rm(self, path):
        """Borra un archivo. D6 opcion (b): el rm siempre gana.

        Si el archivo estaba UNDER_CONSTRUCTION, el nodo desaparece y con
        el su lease. El cliente que estuviera subiendo se entera cuando su
        Complete falle con FAILED_PRECONDITION. Los bloques ya escritos
        quedan huerfanos y se agendan para borrado.

        Un path no puede quedar bloqueado para siempre por un cliente que
        se murio, y eso importa mas que unos bloques huerfanos temporales.
        """
        nodo = self.resolve(path)
        if nodo is None:
            return False, "no existe", []
        if nodo.is_dir:
            return False, "es un directorio, usa rmdir", []

        huerfanos = list(nodo.blocks)
        en_construccion = nodo.state == UNDER_CONSTRUCTION
        padre, nombre = self.parent_of(path)
        del padre.children[nombre]
        msg = "ok (se cancelo una subida en curso)" if en_construccion else "ok"
        return True, msg, huerfanos

    def _recolectar_bloques(self, nodo):
        """Todos los block_id del subarbol. Para el borrado diferido."""
        if not nodo.is_dir:
            return list(nodo.blocks)
        salida = []
        for hijo in nodo.children.values():
            salida.extend(self._recolectar_bloques(hijo))
        return salida

    # TODO semana 8: create / complete / abort / open
