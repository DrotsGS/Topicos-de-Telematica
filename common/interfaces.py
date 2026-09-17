"""
Las cinco costuras del sistema.

Cada una arranca con una implementacion deliberadamente tonta.
Cuando quieras mejorar algo, cambias UNA clase de aqui y nada mas.
"""

from abc import ABC, abstractmethod
import base64
import bisect
import hashlib
import hmac
import itertools
import json
import os
import time

_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 128 MB es el valor de DISENO. Mientras desarrollas conviene bajarlo:
# con 128 MB, probar que el ultimo bloque parcial funciona obliga a
# generar archivos de 384 MB y cada iteracion se vuelve insoportable.
#
#   set DFSHA_BLOCK_SIZE=8388608        (8 MB, desarrollo)
#   set DFSHA_BLOCK_SIZE=1048576        (1 MB, pruebas automaticas)
#
# Va por variable de entorno y no editando esta linea para que las
# pruebas puedan usar bloques chicos sin tocar el codigo, que es el
# mismo patron de config.py.
BLOCK_SIZE = int(os.environ.get("DFSHA_BLOCK_SIZE", 128 * 1024 * 1024))

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


class ConsistentHashPlacer(BlockPlacer):
    """Anillo de hash consistente con nodos virtuales.

    Por que no hash % N: con modulo, agregar un DataNode remapea casi
    todas las claves (de hash % 4 a hash % 5 practicamente ningun bloque
    cae donde estaba). Con un anillo solo se mueve alrededor de 1/N.

    Cada nodo fisico se replica VNODOS veces en el anillo. Sin nodos
    virtuales, con pocos nodos el reparto queda muy desbalanceado: uno
    puede quedarse con el 60% de las claves por pura suerte de donde
    cayo su hash.

    OJO: esto es politica de colocacion INICIAL, no mecanismo de
    busqueda. El NameNode persiste donde quedo cada bloque y no
    recalcula el hash al leer; por eso puede desviarse del anillo si un
    nodo esta lleno o caido sin que el sistema se pierda.
    """

    VNODOS = 150

    def __init__(self, nodos=None):
        self._anillo = {}       # posicion -> nodo fisico
        self._posiciones = []   # ordenado, para bisect
        for n in (nodos or []):
            self.agregar(n)

    @staticmethod
    def _hash(clave):
        return int(hashlib.md5(clave.encode()).hexdigest()[:8], 16)

    def agregar(self, nodo):
        for i in range(self.VNODOS):
            self._anillo[self._hash("{}#{}".format(nodo, i))] = nodo
        self._posiciones = sorted(self._anillo.keys())

    def quitar(self, nodo):
        for i in range(self.VNODOS):
            self._anillo.pop(self._hash("{}#{}".format(nodo, i)), None)
        self._posiciones = sorted(self._anillo.keys())

    def _sincronizar(self, vivos):
        for nodo in vivos - set(self._anillo.values()):
            self.agregar(nodo)
        for nodo in set(self._anillo.values()) - vivos:
            self.quitar(nodo)

    def place(self, block_id, datanodes, n):
        """Los n primeros nodos FISICOS distintos, en sentido horario."""
        vivos = set(datanodes)
        self._sincronizar(vivos)
        if not self._posiciones:
            return []

        inicio = bisect.bisect_left(self._posiciones, self._hash(block_id))
        salida = []
        total = len(self._posiciones)
        cuantos = min(n, len(vivos))
        for k in range(total):
            nodo = self._anillo[self._posiciones[(inicio + k) % total]]
            # Caminando el anillo te topas varias veces con el mismo nodo
            # fisico (son sus vnodos). Si no se filtra, las 3 "replicas"
            # pueden caer en la misma maquina, que es justo lo contrario
            # de replicar.
            if nodo not in salida:
                salida.append(nodo)
            if len(salida) == cuantos:
                break
        return salida


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


class FileAuth(AuthProvider):
    """Usuarios en un JSON, tokens firmados con HMAC-SHA256.

    No es JWT todavia, pero ya tiene lo esencial: la contrasena solo
    viaja en el login, y el token expira.

    Formato del token: base64url("<usuario>:<vencimiento>:<firma>").
    """

    def __init__(self, ruta_usuarios, secreto, ttl=3600):
        with open(ruta_usuarios, encoding="utf-8") as f:
            self.usuarios = json.load(f)   # {"drots": "<sha256 del password>"}
        self.secreto = secreto.encode()
        self.ttl = ttl

    def _firmar(self, payload):
        return hmac.new(self.secreto, payload.encode(),
                        hashlib.sha256).hexdigest()[:32]

    def login(self, user, password):
        h = hashlib.sha256(password.encode()).hexdigest()
        # compare_digest tambien aqui: no filtres si el usuario existe
        # por el tiempo que tarda en responder.
        if not hmac.compare_digest(self.usuarios.get(user, ""), h):
            return None
        payload = "{}:{}".format(user, int(time.time()) + self.ttl)
        return base64.urlsafe_b64encode(
            "{}:{}".format(payload, self._firmar(payload)).encode()).decode()

    def verify(self, token):
        try:
            crudo = base64.urlsafe_b64decode(token.encode()).decode()
            user, exp, firma = crudo.rsplit(":", 2)
            # compare_digest en vez de ==: la comparacion en tiempo
            # constante evita que se pueda adivinar la firma byte a byte
            # midiendo cuanto tarda el rechazo.
            if not hmac.compare_digest(firma, self._firmar(
                    "{}:{}".format(user, exp))):
                return None
            if int(exp) < time.time():
                return None
            return user
        except Exception:
            return None


SECRETO_DEV = "dfsha-dev-no-usar-en-produccion"


def build_auth():
    """Escoge el proveedor segun el entorno. Es la costura en accion.

    DFSHA_USERS   ruta del JSON de usuarios (por defecto deploy/usuarios.json)
    DFSHA_SECRET  secreto del HMAC. Nunca va escrito en el codigo.

    Si no hay archivo de usuarios se cae a NoopAuth, para que un clon
    recien hecho arranque sin configurar nada.
    """
    ruta = os.environ.get("DFSHA_USERS") or os.path.join(
        _RAIZ, "deploy", "usuarios.json")
    if not os.path.exists(ruta):
        print("[auth] sin {}: usando NoopAuth (solo desarrollo)".format(ruta))
        return NoopAuth()
    secreto = os.environ.get("DFSHA_SECRET", SECRETO_DEV)
    if secreto == SECRETO_DEV:
        print("[auth] DFSHA_SECRET sin definir: usando el secreto de desarrollo")
    print("[auth] FileAuth con los usuarios de {}".format(ruta))
    return FileAuth(ruta, secreto)


# TODO semana 9: JwtAuth (PyJWT, mismo contrato, cambia el formato del token)
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
