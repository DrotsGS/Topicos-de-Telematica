# Plan detallado — Semanas 7 a 10

**Proyecto:** DFSha — sistema de archivos distribuido por bloques
**Curso:** Sistemas Distribuidos (SI3007) — Universidad EAFIT
**Punto de partida:** final de la semana 6, verificado en ejecución
**Alcance de este plan:** hasta el hito 2 (semana 10)

---

## 0. Diagnóstico de partida

### Lo que ya está resuelto y no se vuelve a tocar

- **El contrato.** `proto/dfsha.proto` cubre las ocho semanas restantes.
  Los campos de lease, pipeline, tokens de bloque, descubrimiento de líder
  y comandos piggyback ya existen. Esto es lo más valioso que tienes: no
  vas a rehacer el protocolo cuando llegue Raft.
- **La separación de planos.** Verificada en el código: el NameNode no
  abre un solo canal hacia un DataNode.
- **Las cinco costuras.** Cada mejora futura toca una clase.
- **La configuración por entorno.** Cero direcciones escritas en el
  código, tres matrices de despliegue que difieren solo en variables.

### El problema real: vas atrasado respecto al calendario

El hito 1 se entrega en la **semana 8**. El código está al final de la
**semana 6**. En la práctica tienes que hacer el trabajo de dos semanas
en algo más de una.

Eso no es una catástrofe, pero cambia las prioridades: **este plan
recorta agresivamente la semana 7** para que la 8 tenga el tiempo que
necesita. Lo que se recorta está marcado con 🔻 y se recupera en la
semana 9, que está vacía en el cronograma oficial.

### Lo que hay que corregir antes de escribir una línea nueva

| # | Problema | Tiempo | Por qué ahora |
|---|---|---|---|
| 1 | No hay repositorio git | 20 min | Sin esto no hay entrega posible |
| 2 | `docker compose` sin probar | 45 min | Si falla, quieres saberlo hoy y no en la semana 10 |
| 3 | `Ls` no valida token | 10 min | Se vuelve hueco real cuando entre `FileAuth` |
| 4 | `DeleteBlock` sin stub | 5 min | Asimetría que confunde |
| 5 | `pendientes.md` desfasado | 10 min | Es con lo que mides el avance |
| 6 | Python 3.14 local vs 3.12 en Docker | 10 min | Fijar versiones en `requirements.txt` |

Total: **1 hora 40 minutos**. Es el bloque 0 de la semana 7.

---

# SEMANA 7 — Cerrar RF1 y la especificación

**Objetivo:** el namespace completo, autenticación real y el entregable
del curso. Todo lo que no sea eso se aplaza.

**Definición de listo:**

```bat
dfsha.bat mkdir /docs
dfsha.bat mkdir /docs/2026
dfsha.bat stat /docs
dfsha.bat rmdir /docs          REM  FAILED_PRECONDITION: no está vacío
dfsha.bat rmdir /docs/2026     REM  ok
dfsha.bat rmdir /docs          REM  ok
dfsha.bat ls /                 REM  (vacio)
```

Y los cinco comandos rechazando un token inválido con `UNAUTHENTICATED`.

---

## Bloque 0 — Higiene (1 h 40 min)

### 0.1 Control de versiones (20 min)

```bat
cd dfsha
git init
git add .
git commit -m "Semana 6: contrato gRPC, esqueleto de los tres nodos, cinco costuras"
git tag semana-06
```

Después crea el repositorio remoto en GitHub (**privado** hasta la
entrega) y:

```bat
git remote add origin https://github.com/<tu-usuario>/dfsha.git
git branch -M main
git push -u origin main --tags
```

**Verifica que `proto/gen/`, `venv/` y `data/` NO subieron.** Si
subieron, el `.gitignore` se agregó después del `git add`:

```bat
git rm -r --cached proto/gen venv data
git commit -m "Aplicar .gitignore"
```

**A partir de aquí, un commit por bloque de trabajo.** No uno al final
del día. El historial es parte de lo que se evalúa.

### 0.2 Verificar Docker (45 min)

```bat
docker compose build
docker compose up
```

Qué debes ver en el log:

```
dfsha-namenode      | NameNode nn-1 escuchando en el puerto 50051
dfsha-datanode-1-1  | DataNode dn-1 escuchando en el puerto 50060
dfsha-namenode      | [Heartbeat] nuevo DataNode: dn-1 en datanode-1:50060
dfsha-namenode      | [Heartbeat] nuevo DataNode: dn-2 en datanode-2:50060
```

Si los dos DataNodes aparecen con direcciones distintas, la composición
está bien. Prueba el cliente:

```bat
docker compose run --rm cliente python client/cli.py ping
docker compose run --rm cliente python client/cli.py mkdir /docs
```

**Fallos típicos y su causa:**

| Síntoma | Causa | Arreglo |
|---|---|---|
| `Connection refused` al arrancar | El DataNode arranca antes que el NameNode | Normal: el heartbeat reintenta cada 3 s. Verifica que a los 10 s ya conectó |
| `ModuleNotFoundError: dfsha_pb2` | El `gen_proto.py` del Dockerfile falló | `docker compose build --no-cache` y lee el log del build |
| Un DataNode no aparece | `ADVERTISE_ADDR` mal puesto | Revisa el `docker-compose.yml` |

Cuando funcione: `git commit -m "Verificar docker compose"` y marca el
ítem en `docs/pendientes.md`.

### 0.3 Las cuatro correcciones menores (35 min)

**`Ls` sin validación de token** — en `namenode/server.py`:

```python
def Ls(self, request, context):
    if self._check(request.token, context) is None:
        return dfsha_pb2.LsResponse()
    # ...resto igual
```

Aplica el mismo patrón a las cinco RPCs de namespace cuando las escribas.

**`DeleteBlock` sin stub** — en `datanode/server.py`:

```python
def DeleteBlock(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details("DeleteBlock llega en la semana 11")
    return dfsha_pb2.StatusResponse()
```

**Fijar versiones** — en `requirements.txt`:

```
grpcio==1.83.1
grpcio-tools==1.83.1
protobuf==7.36.1
```

Así el contenedor y tu venv usan exactamente lo mismo. La diferencia de
Python (3.14 local, 3.12 en Docker) no importa mientras las bibliotecas
coincidan.

**`docs/pendientes.md`**: marca `deploy/setup.sh` como hecho y el ítem
de Docker que acabas de verificar.

---

## Bloque A — Namespace: `rmdir`, `rm`, `stat` (4–5 h)

**Archivo:** `namenode/namespace.py`

Esto es lógica pura, sin red. Es el bloque donde más rinde escribir
pruebas.

### A.1 `stat(path)` — empieza por aquí (30 min)

La más simple, sirve para calentar.

```python
def stat(self, path):
    """Devuelve el Node o None si no existe."""
    return self.resolve(path)
```

Casi todo el trabajo está en el servidor, mapeando `Node` a `FileInfo`.

### A.2 `rmdir(path)` (1–1.5 h)

**Firma:** `rmdir(self, path, recursivo=False) -> (bool, str)`

**Casos que debe cubrir, en este orden:**

1. El path no existe → `(False, "no existe")`
2. El path es un archivo, no un directorio → `(False, "no es un directorio")`
3. Es la raíz (`/`) → `(False, "no se puede borrar la raiz")`
4. Tiene hijos y `recursivo=False` → `(False, "el directorio no esta vacio")`
5. Tiene hijos y `recursivo=True` → borra recursivo, devuelve los `block_id`
   huérfanos para que el servidor los agende para borrado
6. Está vacío → borra y `(True, "ok")`

**Esqueleto:**

```python
def rmdir(self, path, recursivo=False):
    nodo = self.resolve(path)
    if nodo is None:
        return False, "no existe", []
    if not nodo.is_dir:
        return False, "no es un directorio", []
    if nodo is self.root:
        return False, "no se puede borrar la raiz", []
    if nodo.children and not recursivo:
        return False, "el directorio no esta vacio", []

    huerfanos = self._recolectar_bloques(nodo)   # recorre el subárbol
    padre, nombre = self.parent_of(path)
    del padre.children[nombre]
    return True, "ok", huerfanos


def _recolectar_bloques(self, nodo):
    """Todos los block_id del subárbol. Para el borrado diferido."""
    if not nodo.is_dir:
        return list(nodo.blocks)
    salida = []
    for hijo in nodo.children.values():
        salida.extend(self._recolectar_bloques(hijo))
    return salida
```

Fíjate que devuelve **tres** valores. La lista de bloques huérfanos no
sirve hoy, pero es lo que la semana 11 convierte en comandos piggyback.
Dejarla puesta ahora cuesta cero.

### A.3 `rm(path)` (1.5–2 h)

Aquí está la decisión difícil del proyecto. Ver **D6** más abajo.

**Firma:** `rm(self, path) -> (bool, str, list)`

**Casos:**

1. No existe → `(False, "no existe", [])`
2. Es un directorio → `(False, "es un directorio, usa rmdir", [])`
3. Está `UNDER_CONSTRUCTION` con lease activo → **decisión D6**
4. Está `COMMITTED` → borra del namespace, devuelve sus `block_id`

El punto 3 es lo que hay que resolver antes de escribir el código.

### A.4 Pruebas unitarias del namespace (1 h) 🔻

`tests/test_namespace.py`. No necesita red, corre en segundos.

```python
import pytest
from namenode.namespace import Namespace, COMMITTED, UNDER_CONSTRUCTION


def test_mkdir_anidado():
    ns = Namespace()
    assert ns.mkdir("/a")[0]
    assert ns.mkdir("/a/b")[0]
    assert ns.resolve("/a/b") is not None


def test_mkdir_sin_padre():
    ns = Namespace()
    ok, msg = ns.mkdir("/a/b/c")
    assert not ok and "padre" in msg


def test_rmdir_no_vacio():
    ns = Namespace()
    ns.mkdir("/a"); ns.mkdir("/a/b")
    ok, msg, _ = ns.rmdir("/a")
    assert not ok and "vacio" in msg


def test_rmdir_recursivo():
    ns = Namespace()
    ns.mkdir("/a"); ns.mkdir("/a/b"); ns.mkdir("/a/b/c")
    ok, _, _ = ns.rmdir("/a", recursivo=True)
    assert ok and ns.resolve("/a") is None


def test_ls_oculta_under_construction():
    ns = Namespace()
    ns.mkdir("/d")
    # crear un archivo en construcción directamente
    from namenode.namespace import Node
    ns.resolve("/d").children["x"] = Node(
        name="x", is_dir=False, state=UNDER_CONSTRUCTION)
    assert ns.ls("/d") == []
```

Este último test es el que protege el WORM. Si alguien rompe el filtro
de `ls`, te enteras al instante.

**Es recortable si vas muy justo**, pero son 5 tests y una hora, y te
cubren la parte del sistema donde más fácil se cuela un bug silencioso.

---

## Bloque B — Conectar en el servidor (2 h)

**Archivo:** `namenode/server.py`

### Tabla de mapeo a `StatusCode`

Esto es lo que el profesor evalúa como "uso correcto del middleware".
Tenla a la vista mientras escribes:

| Situación | `StatusCode` |
|---|---|
| Token inválido o ausente | `UNAUTHENTICATED` |
| Usuario válido pero sin permiso sobre el path | `PERMISSION_DENIED` |
| El path no existe | `NOT_FOUND` |
| El directorio padre no existe | `NOT_FOUND` |
| Ya existe (mkdir duplicado, create sobre COMMITTED) | `ALREADY_EXISTS` |
| Directorio no vacío sin `-r` | `FAILED_PRECONDITION` |
| `rm` sobre un archivo en construcción | `FAILED_PRECONDITION` |
| `rmdir` sobre un archivo, o `rm` sobre un directorio | `INVALID_ARGUMENT` |
| No hay DataNodes vivos | `UNAVAILABLE` |
| Error interno inesperado | `INTERNAL` |

### El patrón uniforme

Extrae un helper para no repetir la validación:

```python
def _autorizar(self, token, context):
    usuario = self.auth.verify(token)
    if usuario is None:
        context.abort(grpc.StatusCode.UNAUTHENTICATED, "token invalido")
    return usuario
```

`context.abort()` lanza excepción, así que el resto del método no se
ejecuta. Es más limpio que `set_code` + `return` y evita el error de
devolver un mensaje vacío con código de éxito.

### `Stat`

```python
def Stat(self, request, context):
    self._autorizar(request.token, context)
    nodo = self.ns.stat(request.path)
    if nodo is None:
        context.abort(grpc.StatusCode.NOT_FOUND, "no existe")
    return dfsha_pb2.FileInfo(
        path=request.path,
        is_dir=nodo.is_dir,
        size=nodo.size,
        num_blocks=len(nodo.blocks),
        state=nodo.state,
        created_at=int(nodo.created_at))
```

### `Rmdir` y `Rm`

Mismo patrón. Los bloques huérfanos que devuelve el namespace se
acumulan en una cola del NameNode:

```python
self.pendientes_borrado = []   # en __init__
# ...
ok, msg, huerfanos = self.ns.rmdir(request.path)
self.pendientes_borrado.extend(huerfanos)
```

Esa cola no se vacía todavía. En la semana 11, el `Heartbeat` la lee y
manda `DeleteCommand` por piggyback. **Es el gancho, déjalo puesto.**

`Rmdir` necesita el flag recursivo, pero `PathRequest` no tiene campo
para él. Dos opciones: usar la convención de que el path termine en `/`
(feo), o aceptar que la v1 no sea recursiva y anotar la deuda. **Toma
la segunda**: no toques el `.proto` esta semana.

---

## Bloque C — Subcomandos del cliente (1 h)

**Archivo:** `client/cli.py`

```python
p = sub.add_parser("rmdir", help="borra un directorio vacio")
p.add_argument("path")

p = sub.add_parser("rm", help="borra un archivo")
p.add_argument("path")

p = sub.add_parser("stat", help="muestra los metadatos de una ruta")
p.add_argument("path")
```

Y los handlers correspondientes en el diccionario `HANDLERS`. El de
`stat` merece formato legible:

```python
def cmd_stat(stub, args):
    r = stub.Stat(dfsha_pb2.PathRequest(path=args.path, token=TOKEN))
    tipo = "directorio" if r.is_dir else "archivo"
    estado = "COMMITTED" if r.state == 1 else "UNDER_CONSTRUCTION"
    print("ruta    : {}".format(r.path))
    print("tipo    : {}".format(tipo))
    print("tamano  : {} bytes".format(r.size))
    print("bloques : {}".format(r.num_blocks))
    print("estado  : {}".format(estado))
```

---

## Bloque D — `FileAuth` (1.5 h)

**Archivo:** `common/interfaces.py`

Reemplaza `NoopAuth`. Sigue siendo simple: usuarios en un archivo,
contraseñas hasheadas, token con expiración.

```python
import hmac, hashlib, base64, json, time, os


class FileAuth(AuthProvider):
    """Usuarios en un JSON, tokens firmados con HMAC.

    No es JWT todavía, pero ya tiene lo esencial: la contraseña nunca
    viaja después del login y el token expira.
    """

    def __init__(self, ruta_usuarios, secreto, ttl=3600):
        with open(ruta_usuarios) as f:
            self.usuarios = json.load(f)   # {"drots": "<sha256 del password>"}
        self.secreto = secreto.encode()
        self.ttl = ttl

    def login(self, user, password):
        h = hashlib.sha256(password.encode()).hexdigest()
        if self.usuarios.get(user) != h:
            return None
        payload = "{}:{}".format(user, int(time.time()) + self.ttl)
        firma = hmac.new(self.secreto, payload.encode(),
                         hashlib.sha256).hexdigest()[:32]
        return base64.urlsafe_b64encode(
            "{}:{}".format(payload, firma).encode()).decode()

    def verify(self, token):
        try:
            crudo = base64.urlsafe_b64decode(token.encode()).decode()
            user, exp, firma = crudo.rsplit(":", 2)
            payload = "{}:{}".format(user, exp)
            esperada = hmac.new(self.secreto, payload.encode(),
                                hashlib.sha256).hexdigest()[:32]
            if not hmac.compare_digest(firma, esperada):
                return None
            if int(exp) < time.time():
                return None
            return user
        except Exception:
            return None
```

**`hmac.compare_digest` en vez de `==`** es importante: evita ataques de
temporización. Es el tipo de detalle que vale mencionar en el informe.

Agrega un subcomando `login` al cliente que guarde el token en
`~/.dfsha_token`, y que `config.py` lo lea si existe.

El secreto sale de la variable `DFSHA_SECRET`, con un valor por defecto
solo para desarrollo. Nunca en el código.

---

## Bloque E — Documento de especificación (5–6 h) ⚠️ ENTREGABLE

**Este es el entregable calificado de la semana.** No es código y no se
puede recortar.

Estructura sugerida, con lo que ya tienes para llenarla:

| Sección | De dónde sale |
|---|---|
| 1. Descripción del servicio | El README y el diagrama de arquitectura |
| 2. Requisitos funcionales RF1–RF3 | El enunciado, más tu tabla de comandos |
| 3. Requisitos no funcionales RNF1–RNF8 | La tabla de la visualización de arquitectura |
| 4. Arquitectura: opción escogida | D1 de `docs/decisiones.md` |
| 5. Diagrama de componentes y capas | El HTML de arquitectura, exportado |
| 6. Modelo de datos (namespace, block map) | `namespace.py` y los mensajes del `.proto` |
| 7. Protocolos de comunicación | La tabla de los cinco pares |
| 8. Decisiones de diseño y alternativas | D1–D6 completas |
| 9. Plan de trabajo y cronograma | Este documento |

**Consejo de peso:** la sección 8 es la que separa un proyecto de
Sistemas Distribuidos de una tarea de programación. Desarrolla D2 (WORM)
y D3 (el cliente particiona) con su justificación completa: explica por
qué el NameNode no es cuello de botella y cómo eso sostiene RNF1 y RNF5.
Ese razonamiento es el corazón de la evaluación.

Para los diagramas: abre el HTML de arquitectura en el navegador y
captura cada SVG. Se ven bien en un Word o un PDF.

---

## Decisiones de la semana 7

### D5 — `rmdir` sobre directorio no vacío

**Recomendación: error con `FAILED_PRECONDITION`.**

HDFS exige `-r` explícito por la misma razón: el borrado recursivo
silencioso es la forma más rápida de perder datos. Además te ahorra
tener que tocar el `.proto` esta semana para agregar el flag.

Deja `rmdir(path, recursivo=False)` con el parámetro ya en la firma —
cuando agregues el flag al protocolo, no cambias la lógica.

### D6 — `rm` sobre un archivo `UNDER_CONSTRUCTION`

**La primera decisión real de consistencia del proyecto.** Merece una
entrada extensa.

Las dos opciones:

**(a) Rechazar mientras el lease esté vivo** → `FAILED_PRECONDITION`.
Simple, predecible, sin bloques huérfanos. Pero si el cliente murió y
el lease nunca expira, ese path queda bloqueado para siempre. Te obliga
a implementar expiración de leases o un comando `abort` manual.

**(b) Invalidar el lease y dejar los bloques huérfanos.**
El `rm` siempre funciona. Los bloques ya escritos se agendan para
borrado por comando piggyback. Cuando el cliente intente `Complete` con
un lease invalidado, recibe `FAILED_PRECONDITION` y sabe que su subida
se canceló.

**Recomendación: (b).** Tres razones que puedes escribir en el informe:

1. Es lo que hace HDFS.
2. Conecta directamente con `HeartbeatResponse.commands`, que ya está en
   tu contrato — le da razón de ser a un mecanismo que ya diseñaste.
3. Un path no puede quedar bloqueado por un cliente muerto, y eso
   importa más que dejar bloques huérfanos temporalmente.

Documenta también qué **no** resuelve: los bloques huérfanos ocupan
espacio hasta que el recolector corra en la semana 11. Esa deuda
reconocida es parte de un buen informe.

---

## Presupuesto de la semana 7

| Bloque | Horas | ¿Recortable? |
|---|---|---|
| 0. Higiene (git, docker, correcciones) | 1.7 | No |
| A. Namespace: rmdir, rm, stat | 4–5 | No |
| A.4 Pruebas unitarias | 1 | 🔻 Sí |
| B. Conectar en el servidor | 2 | No |
| C. Subcomandos del cliente | 1 | No |
| D. `FileAuth` | 1.5 | 🔻 Sí, aplazar a semana 9 |
| E. Documento de especificación | 5–6 | No (es entregable) |
| **Total completo** | **16–18** | |
| **Total recortado** | **13.5–15** | |

**Si vas muy justo:** aplaza `FileAuth` y las pruebas unitarias a la
semana 9. Deja `NoopAuth` pero con la validación uniforme en las cinco
RPCs — así el cambio de la semana 9 es de una línea.

---

# SEMANA 8 — Hito 1: transferencia de archivos

El detalle de implementación está en `docs/semana-08-implementacion.md`,
con el código de los cuatro puntos difíciles. Aquí va la secuencia, los
criterios de aceptación y lo que cambia respecto a ese documento.

**Definición de listo:** subes un archivo de 500 MB, lo borras del disco
local, lo bajas, y el `sha256` coincide.

## Secuencia recomendada

| Orden | Tarea | Horas | Se prueba con |
|---|---|---|---|
| 1 | `DataNode.PutBlock` (streaming) | 4–6 | `scripts/test_datanode.py`, sin NameNode |
| 2 | `DataNode.GetBlock` | 1–2 | El mismo script, ida y vuelta |
| 3 | `NameNode.Create` y `Abort` | 3–4 | Cliente que solo llama a Create e imprime |
| 4 | `NameNode.Complete` y `Open` | 2–3 | Igual |
| 5 | Cliente `put` | 4–5 | Archivo de 1 KB primero |
| 6 | Cliente `get` | 2–3 | Comparar hash |
| 7 | Pruebas end to end | 3–4 | `scripts/test_e2e.py` |
| | **Total** | **19–27** | |

**De abajo hacia arriba a propósito.** El DataNode es autónomo: cuando
el streaming falle, sabes que el problema está ahí y no repartido en
tres capas.

## Los tres errores que cuestan un día entero

**1. El tamaño del último bloque.** Todos miden `BLOCK_SIZE` menos el
último, que mide el resto. Si te equivocas, el archivo descargado tiene
basura al final. Calcúlalo con un contador explícito, no con división.

**2. Acumular en memoria.** Ni en el DataNode al recibir, ni en el
cliente al reensamblar. Escribe a disco conforme llega, y reensambla con
`seek(index * BLOCK_SIZE)`. Un `b"".join()` mata el proceso con archivos
grandes.

**3. Dejar bloques a medias en disco.** Escribe a `tmp_<id>` y renombra
con `os.replace` al terminar. Es atómico en Windows y en Linux.

## Ajuste importante para probar

**Baja `BLOCK_SIZE` a 8 MB mientras desarrollas.** Con 128 MB, probar
que el último bloque parcial funciona te obliga a generar archivos de
384 MB y cada iteración se vuelve insoportable. Es una línea en
`common/interfaces.py` — por eso está ahí.

Súbelo a 128 MB solo para la prueba final de 500 MB, y documenta 128 MB
como el valor de diseño.

## Decisiones de la semana 8

**D7 — ¿Persistes los metadatos?**
Recomendación: **no**. En la semana 12 los reemplaza Raft. Si el
profesor lo exige, la versión mínima es volcar el namespace a JSON en
cada `Complete` y releerlo al arrancar: una hora, no seis.

**D8 — ¿Qué pasa si el cliente muere entre `Create` y `Complete`?**
Con D6 ya resuelto (opción b), la respuesta natural es: el archivo queda
`UNDER_CONSTRUCTION`, invisible, y un `rm` lo limpia. Documenta que la
expiración automática de leases queda para la semana 12.

**D9 — ¿Bloques en serie o en paralelo?**
Recomendación: **en serie**. Con un solo DataNode el paralelismo no
compra nada, y en serie es mucho más fácil de depurar.

## El gancho que no puedes olvidar

En el cliente, **itera** sobre `asignacion.datanodes` aunque hoy tenga
un solo elemento:

```python
for destino in asignacion.datanodes:      # hoy 1, en la semana 11 son 3
    try:
        resultado = _put_a_un_datanode(destino, asignacion, ruta)
        break                             # semana 11: quitar este break
    except grpc.RpcError:
        continue
```

Si escribes `datanodes[0]`, la semana 11 reescribes el cliente.

---

# SEMANA 9 — Despliegue y recuperación de deuda

**Esta semana está vacía en el cronograma oficial. Es tu colchón.**

Tiene dos funciones: subir el hito 1 a AWS sin presión, y adelantar
trabajo de la semana 12, que es la más cargada del proyecto.

## Bloque A — Primer despliegue en AWS Academy (5–7 h)

### A.1 Preparar las imágenes (1 h)

No compiles en EC2: el Learner Lab apaga las instancias y perderías el
tiempo cada vez.

```bat
docker build -t <tu-usuario>/dfsha:hito1 .
docker push <tu-usuario>/dfsha:hito1
```

Un solo push, y en EC2 solo haces `docker pull`.

### A.2 Topología mínima (2 h)

Para el hito 1 bastan **dos instancias** `t3.micro`:

| Instancia | Rol | Puerto |
|---|---|---|
| `dfsha-nn` | NameNode | 50051 |
| `dfsha-dn1` | DataNode | 50060 |

Ambas en la misma VPC y subred. Para la semana 10 agregas dos DataNodes
más, así que deja el patrón listo.

### A.3 Security Groups — la causa número uno de fallos (1 h)

Crea **dos** grupos, no uno:

**`sg-dfsha-namenode`**
| Tipo | Puerto | Origen |
|---|---|---|
| SSH | 22 | Tu IP |
| Custom TCP | 50051 | `sg-dfsha-datanode` |
| Custom TCP | 50051 | Tu IP (para el cliente) |

**`sg-dfsha-datanode`**
| Tipo | Puerto | Origen |
|---|---|---|
| SSH | 22 | Tu IP |
| Custom TCP | 50060 | `sg-dfsha-namenode` |
| Custom TCP | 50060 | Tu IP (el cliente habla directo con los DataNodes) |

**Referencia grupos, no IPs.** Cuando agregues DataNodes en la semana
10, heredan el permiso sin tocar nada.

**Y recuerda:** el cliente habla directo con los DataNodes. Si solo
abres el puerto del NameNode, el `ping` funciona y el `put` falla — es
el síntoma clásico de la separación de planos mal desplegada.

### A.4 Arranque (1–2 h)

En cada instancia, tras `deploy/setup.sh`:

```bash
# NameNode
docker run -d --name nn --restart unless-stopped \
  -p 50051:50051 -e NODE_ID=nn-1 -e PORT=50051 \
  <tu-usuario>/dfsha:hito1 python namenode/server.py

# DataNode (IP privada del NameNode)
docker run -d --name dn1 --restart unless-stopped \
  -p 50060:50060 -v dfsha-data:/data \
  -e NODE_ID=dn-1 -e PORT=50060 \
  -e ADVERTISE_ADDR=10.0.1.42:50060 \
  -e NAMENODE_ADDR=10.0.1.41:50051 \
  -e DATA_DIR=/data \
  <tu-usuario>/dfsha:hito1 python datanode/server.py
```

`ADVERTISE_ADDR` es la **IP privada** de la instancia del DataNode. Esa
es la dirección que el NameNode le pasará al cliente, así que el cliente
tiene que poder alcanzarla. Si el cliente corre desde tu portátil y las
instancias solo tienen IP privada, necesitas un bastión o IPs públicas.

**Anota todo lo que hagas a mano en `deploy/README.md`.** Vas a repetir
esto muchas veces.

### A.5 Guardar el trabajo (30 min)

El Learner Lab apaga las instancias al cerrar sesión. Guarda:

- Las AMIs, o mejor: el `setup.sh` + el `docker pull` bastan
- Un `deploy/arrancar.sh` con los `docker run` parametrizados
- Los IDs de los Security Groups, que sí sobreviven

## Bloque B — Adelantar la semana 12 (4–6 h)

La semana 12 tiene tres frentes: Raft, re-replicación y seguridad. Solo,
son unas 30 horas en una semana. **La seguridad es independiente de todo
lo demás**, así que adelántala aquí.

### B.1 `JwtAuth` (2 h)

Si hiciste `FileAuth` en la semana 7, esto es cambiar el formato del
token a JWT real con `PyJWT`. Si aplazaste `FileAuth`, hazlo ahora
directamente en JWT.

### B.2 mTLS entre nodos (2–3 h)

Genera una CA propia y certificados para cada nodo:

```bash
# CA
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout ca.key -out ca.crt -subj "/CN=dfsha-ca"

# Certificado de un nodo
openssl req -newkey rsa:2048 -nodes -keyout nn.key -out nn.csr \
  -subj "/CN=namenode"
openssl x509 -req -in nn.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out nn.crt -days 365
```

En el servidor, cambia `add_insecure_port` por:

```python
credenciales = grpc.ssl_server_credentials(
    [(key_bytes, cert_bytes)],
    root_certificates=ca_bytes,
    require_client_auth=True)
server.add_secure_port("[::]:" + PORT, credenciales)
```

Y en los clientes, `grpc.secure_channel` con `ssl_channel_credentials`.

**Ponlo detrás de una variable de entorno** `DFSHA_TLS=1`, para poder
desarrollar sin certificados y desplegar con ellos.

### B.3 `AesGcmCipher` (1 h)

Las llamadas a `cipher.encrypt()` y `cipher.decrypt()` ya están en el
cliente desde la semana 8. Solo falta la implementación:

```python
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import os


class AesGcmCipher(BlockCipher):
    def __init__(self, clave):   # 32 bytes
        self.aes = AESGCM(clave)

    def encrypt(self, data):
        nonce = os.urandom(12)
        return nonce + self.aes.encrypt(nonce, data, None)

    def decrypt(self, data):
        return self.aes.decrypt(data[:12], data[12:], None)
```

**Cuidado:** cifrar chunk por chunk agrega 28 bytes por chunk (nonce +
tag), así que el bloque cifrado pesa más que el original. El `sha256`
que verifica el cliente debe calcularse sobre el texto **claro**, y el
del DataNode sobre el cifrado. Son dos checksums distintos y hay que
tenerlo claro o las verificaciones fallan.

## Bloque C — Banco de mediciones (2 h) 🔻

Empieza `scripts/benchmark.py` ahora, aunque solo puedas medir con un
DataNode. En la semana 13 vas a necesitar la comparación 1 vs 4 nodos, y
tener la línea base del hito 1 te da el punto de partida.

Mide: tiempo de subida y bajada por tamaño (1 MB, 10 MB, 100 MB, 500 MB),
y throughput en MB/s.

## Presupuesto de la semana 9

| Bloque | Horas |
|---|---|
| A. Despliegue en AWS | 5–7 |
| B. Seguridad adelantada | 4–6 |
| C. Banco de mediciones | 2 🔻 |
| Recuperar lo recortado de la semana 7 | 2–3 |
| **Total** | **13–18** |

---

# SEMANA 10 — Hito 2: sistema distribuido

**Objetivo:** varios DataNodes reales, colocación por hash consistente, y
el segundo entregable del curso.

**Definición de listo:** subes un archivo de 4 bloques con 4 DataNodes
corriendo, y `stat` muestra que los bloques quedaron repartidos entre
nodos distintos. Apagas uno y el NameNode lo detecta en 30 segundos.

## Bloque A — `ConsistentHashPlacer` (4–5 h)

**Archivo:** `common/interfaces.py`

Esta es la pieza técnica de la semana y la que más luce en el informe.

### Por qué no `hash % N`

Con módulo, agregar un DataNode remapea casi **todas** las claves: pasas
de `hash % 4` a `hash % 5` y prácticamente ningún bloque cae donde
estaba. Con un anillo, solo se mueve aproximadamente `1/N` de las
claves.

### El algoritmo

```python
import hashlib
import bisect


class ConsistentHashPlacer(BlockPlacer):
    """Anillo de hash consistente con nodos virtuales.

    Cada nodo físico se replica VNODOS veces en el anillo. Sin nodos
    virtuales, con pocos nodos el reparto queda muy desbalanceado:
    un nodo puede quedarse con el 60% de las claves por pura suerte
    de dónde cayó su hash.
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
            pos = self._hash("{}#{}".format(nodo, i))
            self._anillo[pos] = nodo
        self._posiciones = sorted(self._anillo.keys())

    def quitar(self, nodo):
        for i in range(self.VNODOS):
            pos = self._hash("{}#{}".format(nodo, i))
            self._anillo.pop(pos, None)
        self._posiciones = sorted(self._anillo.keys())

    def place(self, block_id, datanodes, n):
        """Los n primeros nodos FISICOS distintos, en sentido horario."""
        vivos = set(datanodes)
        # sincroniza el anillo con quién está vivo
        for nodo in vivos - set(self._anillo.values()):
            self.agregar(nodo)
        for nodo in set(self._anillo.values()) - vivos:
            self.quitar(nodo)

        if not self._posiciones:
            return []

        inicio = bisect.bisect_left(self._posiciones, self._hash(block_id))
        salida = []
        total = len(self._posiciones)
        for k in range(total):
            pos = self._posiciones[(inicio + k) % total]
            nodo = self._anillo[pos]
            if nodo not in salida:       # nodos FISICOS distintos
                salida.append(nodo)
            if len(salida) == min(n, len(vivos)):
                break
        return salida
```

### Los tres detalles que importan

**Nodos virtuales.** Sin ellos, con 4 nodos el reparto puede quedar en
60/20/15/5 por pura suerte. Con 150 réplicas cada uno, se acerca a
25/25/25/25.

**Nodos físicos distintos.** Al caminar el anillo puedes toparte varias
veces con el mismo nodo físico (son sus vnodos). Si no filtras, las 3
"réplicas" pueden caer todas en la misma máquina — lo cual destruye el
propósito de replicar.

**El NameNode persiste dónde quedó el bloque.** No recalcula el hash al
leer. Eso le permite desviarse del anillo si un nodo está lleno o caído,
sin que el sistema se pierda. **El hash es política de colocación
inicial, no mecanismo de búsqueda.** Escribe esa frase en el informe.

### La prueba que demuestra el valor (1 h)

`tests/test_placer.py` — esta prueba es material directo para el informe:

```python
def test_agregar_nodo_mueve_pocas_claves():
    claves = [uuid.uuid4().hex for _ in range(10000)]
    p = ConsistentHashPlacer()

    antes = {k: p.place(k, ["dn1","dn2","dn3","dn4"], 1)[0] for k in claves}
    despues = {k: p.place(k, ["dn1","dn2","dn3","dn4","dn5"], 1)[0] for k in claves}

    movidas = sum(1 for k in claves if antes[k] != despues[k])
    # Con modulo serian ~80%. Con anillo, cerca de 1/5 = 20%.
    assert movidas / len(claves) < 0.30


def test_reparto_balanceado():
    from collections import Counter
    p = ConsistentHashPlacer()
    nodos = ["dn1","dn2","dn3","dn4"]
    c = Counter(p.place(uuid.uuid4().hex, nodos, 1)[0] for _ in range(10000))
    for n in nodos:
        assert 0.18 < c[n]/10000 < 0.32    # ~25% cada uno
```

Pon el número que te dé en el informe. Un dato medido vale más que una
afirmación.

## Bloque B — Detección de nodos muertos (2 h)

**Archivo:** `namenode/server.py`

El heartbeat ya llega. Falta registrar cuándo y purgar los ausentes.

```python
import time

TIMEOUT_DATANODE = 30   # segundos


def Heartbeat(self, request, context):
    self.nn.datanodes[request.node_id] = {
        "addr": request.addr,
        "free_bytes": request.free_bytes,
        "num_blocks": request.num_blocks,
        "last_seen": time.time(),
    }
    return dfsha_pb2.HeartbeatResponse(
        commands=self.nn.comandos_para(request.node_id))


def vivos(self):
    corte = time.time() - TIMEOUT_DATANODE
    return [nid for nid, info in self.datanodes.items()
            if info["last_seen"] > corte]
```

`Create` debe usar `self.vivos()`, no `self.datanodes.keys()`. Es un
cambio de una línea con consecuencias grandes: sin él, el NameNode
asigna bloques a nodos muertos.

Un hilo de fondo cada 10 s que imprima los nodos que pasaron a muerto te
sirve para la demo del video.

## Bloque C — `BlockReport` y `BlockReceived` (2–3 h)

**`BlockReceived`**: el DataNode avisa al NameNode cuando termina de
recibir un bloque. El NameNode confirma que quedó donde esperaba.

**`BlockReport`**: cada 60 s (o al arrancar), el DataNode manda la lista
completa de bloques que tiene. El NameNode reconstruye el mapa de
ubicaciones con eso.

**Este es el mecanismo que te permite NO persistir las ubicaciones.**
Es lo que hace HDFS: el namespace y el block map van al log durable, pero
`blockID → DataNodes` se reconstruye con los reports al arrancar.
Menciónalo en el informe, es una decisión de diseño no obvia.

## Bloque D — Subida y bajada en paralelo (3–4 h)

**Archivo:** `client/cli.py`

Ahora sí vale la pena: con 4 DataNodes, el paralelismo multiplica el
throughput. Esto es lo que demuestra RNF5.

```python
from concurrent.futures import ThreadPoolExecutor

MAX_PARALELO = 4


def subir_todos(asignaciones, ruta_local):
    with ThreadPoolExecutor(max_workers=MAX_PARALELO) as pool:
        futuros = {pool.submit(subir_bloque, a, ruta_local): a
                   for a in asignaciones}
        checksums = []
        for fut in futuros:
            a = futuros[fut]
            r = fut.result()      # propaga la excepción si falló
            checksums.append(dfsha_pb2.BlockChecksum(
                block_id=a.block_id, sha256=r.sha256))
    return checksums
```

**Cuidado con la bajada:** varios hilos escribiendo al mismo archivo
necesitan cada uno su propio descriptor con su `seek`, o un lock. Lo más
simple: cada hilo abre el archivo en modo `r+b`, hace `seek` a su offset
y escribe solo su tramo. No hay solapamiento porque los bloques no se
pisan.

**Mide antes y después.** La comparación 1 vs 4 DataNodes es dato duro
para el informe.

## Bloque E — Separar el `.proto` (1–2 h)

Ahora sí toca. Cuatro archivos:

```
proto/dfsha_common.proto     Empty, StatusResponse, Entry, FileState, ...
proto/dfsha_namenode.proto   NameNodeService  (import common)
proto/dfsha_datanode.proto   DataNodeService  (import common)
proto/dfsha_control.proto    ControlService   (import common)
```

En cada uno: `import "dfsha_common.proto";` y prefija los tipos con
`dfsha.`. Actualiza `scripts/gen_proto.py` para que compile los cuatro
(el `-I proto` ya resuelve los imports entre ellos) y `common/pb.py`
para exportar los cuatro módulos.

**Hazlo al final de la semana**, cuando todo lo demás funcione. Es
refactor puro y no debe mezclarse con lógica nueva. Un commit propio.

## Bloque F — Entregable: especificación de protocolos (4–5 h) ⚠️

El enunciado pide especificar **los cinco pares de comunicación** por
separado. Para cada uno:

| Sección | Contenido |
|---|---|
| Propósito | Qué información cruza y por qué |
| Protocolo y justificación | gRPC unario / streaming / Raft, y por qué ese |
| Mensajes | Del `.proto`, con el significado de cada campo |
| Secuencia | Diagrama de la interacción típica |
| Manejo de errores | Qué `StatusCode` en qué situación |
| Quién inicia la conexión | Y por qué importa |

**El par NameNode ↔ DataNode merece atención especial.** Explica que el
NameNode nunca inicia la conexión y que los comandos viajan piggyback en
la respuesta al heartbeat. Es un patrón no obvio, resuelve NAT y
firewall, y demuestra que entendiste el problema. Es de lo más
defendible que tienes.

Los diagramas de secuencia salen del HTML de arquitectura.

## Presupuesto de la semana 10

| Bloque | Horas |
|---|---|
| A. `ConsistentHashPlacer` + pruebas | 5–6 |
| B. Detección de nodos muertos | 2 |
| C. `BlockReport` / `BlockReceived` | 2–3 |
| D. Paralelismo en el cliente | 3–4 |
| E. Separar el `.proto` | 1–2 |
| F. Especificación de protocolos | 4–5 |
| **Total** | **17–22** |

---

# Estado de los ganchos de mejora

Actualizado a lo que habrá al final de la semana 10.

## Partición

| | Estado en S10 | Siguiente paso |
|---|---|---|
| Tamaño de bloque | Constante global `BLOCK_SIZE` | Por archivo: `BlockAssignment.size` ya lo permite |
| Lectura del archivo | Generador `leer_bloque()` con `seek` | Reanudación de subidas cortadas |
| Subida | Paralela con `ThreadPoolExecutor` | Ajustar `MAX_PARALELO` según nodos vivos |

**Nada de esto requiere tocar el protocolo.** El `.proto` ya soporta
tamaño variable por bloque.

## Replicación

| | Estado en S10 | Siguiente paso |
|---|---|---|
| Factor | `REPLICATION_FACTOR = 1` | Subir a 3 en S11 |
| Escritura | Bucle sobre `datanodes` con `break` | Quitar el `break` → escritura paralela |
| Pipeline | `BlockHeader.pipeline` vacío | Llenarlo → cliente→DN1→DN2→DN3 |
| Re-replicación | Cola `pendientes_borrado` sin vaciar | Vaciarla por `HeartbeatResponse.commands` |
| Colocación | `ConsistentHashPlacer` con `n=1` | Cambiar `n` a 3, el código ya lo maneja |

**La semana 11 es cambiar un número y quitar un `break`.** Esa es la
prueba de que las costuras funcionaron.

---

# Riesgos y contingencias

| Riesgo | Probabilidad | Mitigación |
|---|---|---|
| El hito 1 se pasa de la semana 8 | **Alta** (vas atrasado) | Recortar `FileAuth` y tests de S7 a S9. El hito 1 es innegociable |
| El streaming de gRPC toma más de lo estimado | Media | Empezar por ahí y probarlo aislado. Si el día 2 no funciona, es la señal de pedir ayuda |
| Docker o AWS falla en la demo final | Media | Por eso el despliegue va en S9, no en S13. Y graba un video de respaldo |
| La semana 12 se desborda | **Alta** (tres frentes, solo) | Adelantar toda la seguridad a S9. Ya está en el plan |
| Perder trabajo sin git | Baja (ya lo arreglas) | Push después de cada bloque |

**El riesgo dominante es el calendario, no la técnica.** El diseño está
resuelto; lo que falta es tiempo de teclado.

---

# Registro de decisiones esperado

Al final de la semana 10 deberías tener de D1 a D12:

| # | Decisión | Semana |
|---|---|---|
| D1 | Opción 1: C/S con distribución S2S | 6 ✅ |
| D2 | WORM en vez de CRUD | 6 ✅ |
| D3 | El cliente particiona y reensambla | 6 ✅ |
| D4 | Un solo `.proto` inicialmente | 6 ✅ |
| D5 | `rmdir` no recursivo por defecto | 7 |
| D6 | `rm` sobre archivo en construcción | 7 |
| D7 | Metadatos en memoria hasta Raft | 8 |
| D8 | Cliente muerto entre `Create` y `Complete` | 8 |
| D9 | Bloques en serie en el hito 1 | 8 |
| D10 | Consistent hashing con 150 nodos virtuales | 10 |
| D11 | Ubicaciones reconstruidas por block report | 10 |
| D12 | Separación del `.proto` en cuatro archivos | 10 |

**D2, D3, D6 y D11 son las de mayor peso evaluativo.** Son las que
explican por qué el sistema es distribuido y no solo un cliente-servidor
con pasos extra.

---

# Resumen de presupuesto

| Semana | Horas | Entregable del curso |
|---|---|---|
| 7 | 13.5–18 | Especificación definitiva ⚠️ |
| 8 | 19–27 | Hito 1 ⚠️ |
| 9 | 13–18 | — |
| 10 | 17–22 | Especificación de protocolos ⚠️ |
| **Total** | **62–85** | |

Solo, en cuatro semanas: entre 15 y 21 horas semanales. Es carga real.
Los recortes marcados con 🔻 bajan la semana 7 en 2.5 horas y la 9
absorbe lo aplazado.

---

# Lo primero, ahora mismo

```bat
cd dfsha
git init
git add .
git commit -m "Semana 6: contrato gRPC, esqueleto de los tres nodos, cinco costuras"
git tag semana-06
```

Veinte minutos. Después de eso, `docker compose up` para cerrar la
semana 6 de verdad, y arrancas con `rmdir`.
