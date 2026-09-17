"""
Las cinco costuras del sistema.

Cada una arranca con una implementacion deliberadamente tonta.
Cuando quieras mejorar algo, cambias UNA clase de aqui y nada mas.
"""

from abc import ABC, abstractmethod
import hashlib
import itertools

# 128 MB es el valor de diseno. Para la demo en EC2 (disco de 8 GB)
# bajalo a 8 * 1024 * 1024 y documentalo. Una sola linea.
BLOCK_SIZE = 128 * 1024 * 1024

REPLICATION_FACTOR = 1      # sube a 3 en la semana 11
CHUNK_SIZE = 1024 * 1024    # tamano de cada mensaje del stream


# ---------------------------------------------------------------------
# 1. Colocacion de bloques    semanas 10-11
# ---------------------------------------------------------------------
class BlockPlacer(ABC):
    @abstractmethod
    def place(self, block_id: str, datanodes: list, n: int) -> list:
        """Devuelve las n replicas donde va este bloque."""


class RoundRobinPlacer(BlockPlacer):
    """v1. Simple, pero se desbalancea si entra o sale un nodo."""

    def __init__(self):
        self._counter = itertools.count()

    def place(self, block_id, datanodes, n):
        if not datanodes:
            return []
        start = next(self._counter) % len(datanodes)
        k = min(n, len(datanodes))
        return [datanodes[(start + i) % len(datanodes)] for i in range(k)]


# TODO semana 10: ConsistentHashPlacer con nodos virtuales.
#   hash(block_id) -> punto del anillo -> los n siguientes nodos distintos.
#   Recuerda: el NameNode PERSISTE donde quedo cada bloque, no recalcula
#   el hash al leer. Eso le permite desviarse si un nodo esta lleno.


# ---------------------------------------------------------------------
# 2. Almacen de metadatos     semanas 8 y 12
# ---------------------------------------------------------------------
class MetadataStore(ABC):
    @abstractmethod
    def get(self, path: str): ...

    @abstractmethod
    def put(self, path: str, meta) -> None: ...

    @abstractmethod
    def delete(self, path: str) -> None: ...

    @abstractmethod
    def children(self, path: str) -> list: ...


# La implementacion en memoria vive en namenode/namespace.py
# TODO semana 12: RaftStore, mismo contrato, log replicado por debajo.


# ---------------------------------------------------------------------
# 3. Replicacion              semanas 11-12
# ---------------------------------------------------------------------
class Replicator(ABC):
    @abstractmethod
    def write(self, block_id: str, data: bytes, targets: list) -> list:
        """Escribe el bloque en los targets. Devuelve donde quedo de verdad."""


class SingleWriteReplicator(Replicator):
    """v1. Una sola copia, sin replica. Semana 8."""

    def write(self, block_id, data, targets):
        raise NotImplementedError("semana 8")


# TODO semana 11: ParallelReplicator  - el cliente escribe a las 3 replicas
# TODO semana 11: PipelineReplicator  - cliente -> DN1 -> DN2 -> DN3
#   El pipeline no triplica el upstream del cliente, pero es mas codigo.


# ---------------------------------------------------------------------
# 4. Autenticacion            semanas 7, 9 y 12
# ---------------------------------------------------------------------
class AuthProvider(ABC):
    @abstractmethod
    def login(self, user: str, password: str) -> str: ...

    @abstractmethod
    def verify(self, token: str):
        """Devuelve el usuario si el token es valido, None si no."""


class NoopAuth(AuthProvider):
    """v1. Todo el mundo pasa. Semana 6."""

    def login(self, user, password):
        return "fake-token:" + user

    def verify(self, token):
        if token.startswith("fake-token:"):
            return token.split(":", 1)[1]
        return None


# TODO semana 7: FileAuth con usuarios en un archivo
# TODO semana 9: JwtAuth
# TODO semana 12: block access tokens firmados por el NameNode, que el
#   DataNode valida sin consultar al NameNode (no lo vuelvas cuello de botella)


# ---------------------------------------------------------------------
# 5. Cifrado de bloques       semana 9 o 12
# ---------------------------------------------------------------------
class BlockCipher(ABC):
    @abstractmethod
    def encrypt(self, data: bytes) -> bytes: ...

    @abstractmethod
    def decrypt(self, data: bytes) -> bytes: ...


class NoopCipher(BlockCipher):
    """v1. Passthrough."""

    def encrypt(self, data):
        return data

    def decrypt(self, data):
        return data


# TODO: AesGcmCipher. Se cifra EN EL CLIENTE: los DataNodes nunca ven
# texto claro. Es lo mas simple y lo mas fuerte a la vez.


# ---------------------------------------------------------------------
# utilidades
# ---------------------------------------------------------------------
def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def num_blocks(size: int, block_size: int = BLOCK_SIZE) -> int:
    return max(1, (size + block_size - 1) // block_size)
