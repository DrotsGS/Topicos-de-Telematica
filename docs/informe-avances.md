# Informe de avances — DFSha

**Proyecto:** Sistema de archivos distribuido por bloques con alta disponibilidad
**Curso:** Sistemas Distribuidos (SI3007) — Universidad EAFIT
**Fecha del informe:** 16 de septiembre de 2026
**Última modificación del código:** 7 de septiembre de 2026
**Método:** lectura completa del código + ejecución real del sistema (NameNode + DataNode + cliente)

---

## 1. Resumen ejecutivo

El proyecto está **al final de la Semana 6** (contratos y esqueleto). El andamiaje
está completo y **verificado en ejecución**: los tres procesos levantan, se
descubren entre sí por heartbeat y el cliente hace operaciones de namespace
contra el NameNode con los códigos de error gRPC correctos.

| Dimensión | Estado |
|---|---|
| Contrato gRPC (`.proto`) | ✅ Completo para las 8 semanas restantes |
| Arquitectura / separación de capas | ✅ Correcta y coherente con el diseño |
| Plano de control (Ping, Mkdir, Ls) | ✅ Funcionando, verificado |
| Plano de datos (PutBlock, GetBlock) | ⬜ `UNIMPLEMENTED` — Semana 8 |
| Namespace completo (rmdir, rm, stat) | ⬜ Semana 7 |
| Persistencia / HA | ⬜ Semana 12 |
| **Control de versiones** | ❌ **No hay repositorio git inicializado** |

**Volumen de código:** 877 líneas (634 de Python + 243 del `.proto`).

**El bloqueante real no es técnico, es de proceso:** no hay git. Todo lo demás
va en tiempo.

---

## 2. Lo que está verificado y funcionando

Se levantó el sistema completo (NameNode en `:51251`, DataNode en `:51260`) y se
ejecutó la batería de comandos del cliente. Salida real:

```
--- ping ---
nn-test   lider=True
--- mkdir /docs ---
ok
--- mkdir /docs/2026 ---
ok
--- ls / ---
d  docs                     0
--- ls /docs ---
d  2026                     0
--- mkdir duplicado (espera ALREADY_EXISTS) ---
error [ALREADY_EXISTS]: ya existe
--- ls /noexiste (espera NOT_FOUND) ---
error [NOT_FOUND]: no existe o no es un directorio
--- mkdir /a/b/c (padre inexistente) ---
error [NOT_FOUND]: el directorio padre no existe
--- token invalido (espera UNAUTHENTICATED) ---
error [UNAUTHENTICATED]: token invalido
```

Log del NameNode en la misma corrida:

```
NameNode nn-test escuchando en el puerto 51251
[Heartbeat] nuevo DataNode: dn-test en localhost:51260
[Mkdir] /docs -> ok
[Mkdir] /docs/2026 -> ok
[Ls] / -> 1 entradas
[Ls] /docs -> 1 entradas
[Mkdir] /docs -> ya existe
[Mkdir] /a/b/c -> el directorio padre no existe
```

### Qué demuestra esto

1. **Los stubs gRPC están bien generados y sincronizados** con `dfsha.proto`
   (regenerados después de la última edición del contrato).
2. **El registro por heartbeat funciona.** El DataNode se anuncia con su propia
   `ADVERTISE_ADDR` y el NameNode lo indexa sin abrir conexión de vuelta.
   La dirección del heartbeat es la que reporta el DataNode, no la que adivina
   el NameNode — es el detalle que hace que el mismo código sirva en Windows,
   Docker y EC2.
3. **El mapeo de errores de dominio a `StatusCode` de gRPC es correcto**
   (`ALREADY_EXISTS`, `NOT_FOUND`, `UNAUTHENTICATED`). Esto es exactamente lo
   que se evalúa como "uso correcto del middleware", no solo que responda.
4. **El namespace jerárquico funciona**: creación anidada, listado por nivel y
   rechazo cuando el padre no existe.

> **Nota metodológica:** la primera corrida de prueba falló con
> `NOT_FOUND: el directorio padre no existe` en **todos** los comandos. No era un
> bug del proyecto: Git Bash convierte `/docs` en `C:/Program Files/Git/docs`
> antes de pasarlo a Python. Con `MSYS_NO_PATHCONV=1` todo pasó. **En `cmd.exe`
> con los `.bat` esto nunca ocurre** — es solo una advertencia si alguna vez
> pruebas desde Git Bash o WSL.

---

## 3. Arquitectura implementada

### Los tres planos están correctamente separados

```
                         CLIENTE  (client/cli.py)
                        /                        \
       plano de control /                          \  plano de datos
       NameNodeService /                            \  DataNodeService
       metadatos       /                              \  bloques de 128 MB
                      v                                v
              NAMENODE  <-------- ControlService -----  DATANODE
           (namenode/server.py)    heartbeat cada 3s   (datanode/server.py)
                                   piggyback commands
```

**Verificado en el código:** el NameNode no importa nada del DataNode ni abre
canales hacia él. La única dirección de conexión es DataNode → NameNode. Esto es
la decisión D1 llevada al código, y es lo que hace posible que los DataNodes
vivan detrás de NAT o en subredes privadas de AWS.

### Estado de cada servicio del contrato

| Servicio | RPC | Estado |
|---|---|---|
| **NameNodeService** | `Ping` | ✅ implementado |
| | `Mkdir` | ✅ implementado |
| | `Ls` | ✅ implementado (filtra `UNDER_CONSTRUCTION`) |
| | `Rmdir` / `Rm` / `Stat` | ⬜ Semana 7 |
| | `Create` / `Complete` / `Abort` / `Open` | ⬜ Semana 8 |
| **DataNodeService** | `PutBlock` | ⬜ devuelve `UNIMPLEMENTED` |
| | `GetBlock` | ⬜ devuelve `UNIMPLEMENTED` |
| | `DeleteBlock` | ⬜ no declarado en el servicer |
| | `ReplicateTo` | ⬜ Semana 11 |
| **ControlService** | `Heartbeat` | ✅ implementado |
| | `BlockReport` / `BlockReceived` | ⬜ Semana 10 |

**El `.proto` ya contempla las 8 semanas restantes.** Los mensajes para lease
(`CreateResponse.lease_id`), pipeline de replicación (`BlockHeader.pipeline`),
tokens de acceso por bloque (`BlockAssignment.access_token`), descubrimiento de
líder Raft (`PingResponse.is_leader` / `leader_addr`) y comandos piggyback
(`HeartbeatResponse.commands`) **ya están definidos**. Esto es la decisión de
diseño más valiosa del proyecto: no vas a tener que rehacer el contrato cuando
llegue Raft en la semana 12.

### Las cinco costuras (`common/interfaces.py`)

Cada punto de extensión futuro está detrás de una clase abstracta con una
implementación v1 deliberadamente simple.

| Interfaz | v1 actual | Estado | Crece hacia |
|---|---|---|---|
| `BlockPlacer` | `RoundRobinPlacer` | ✅ implementada (instanciada, aún sin usar) | `ConsistentHashPlacer` (S10) |
| `MetadataStore` | `Namespace` en memoria | 🟡 parcial (falta cerrar RF1) | `RaftStore` (S12) |
| `Replicator` | `SingleWriteReplicator` | ⬜ lanza `NotImplementedError` | `Parallel` → `Pipeline` (S11) |
| `AuthProvider` | `NoopAuth` | ✅ implementada | `FileAuth` (S7) → `JwtAuth` (S9) |
| `BlockCipher` | `NoopCipher` | ✅ implementada (passthrough) | `AesGcmCipher` (S9/S12) |

**Evaluación:** es la parte más fuerte del proyecto. Cada mejora futura toca
**una** clase, no el resto del sistema. Los `TODO` en el código indican semana y
estrategia concreta, no son marcadores vacíos.

### Configuración

`common/config.py` centraliza toda dirección en variables de entorno. No hay una
sola IP ni puerto escrito en el código de los servidores — verificado por
lectura. Las tres matrices de despliegue (Windows / Docker / EC2) difieren solo
en variables:

| Variable | Windows | Docker | EC2 |
|---|---|---|---|
| `NAMENODE_ADDR` | `localhost:50051` | `namenode:50051` | IP privada |
| `ADVERTISE_ADDR` | `localhost:50060` | `datanode-1:50060` | IP privada |
| `DATA_DIR` | `data\dn-1` | `/data` | `/data` |

`common/pb.py` resuelve de forma limpia el problema clásico de los imports planos
que genera `protoc` — sin `sed`, sin reescribir archivos generados. Funciona
igual en Windows y en Linux.

---

## 4. Inventario de código

| Archivo | Líneas | Rol | Estado |
|---|---|---|---|
| `proto/dfsha.proto` | 243 | Contrato: 3 servicios, 28 mensajes | ✅ completo hasta S12 |
| `common/interfaces.py` | 154 | Las cinco costuras + utilidades | ✅ v1 completa |
| `namenode/server.py` | 117 | `NameNodeService` + `ControlService` | 🟡 3 de 10 RPCs |
| `datanode/server.py` | 94 | Almacén de bloques + heartbeat | 🟡 solo heartbeat |
| `client/cli.py` | 82 | CLI con `argparse` | 🟡 3 subcomandos |
| `namenode/namespace.py` | 77 | Árbol de directorios en memoria | 🟡 `mkdir` + `ls` |
| `scripts/gen_proto.py` | 49 | Generación multiplataforma de stubs | ✅ completo |
| `common/pb.py` | 34 | Punto único de import de stubs | ✅ completo |
| `common/config.py` | 27 | Config por variables de entorno | ✅ completo |

**Infraestructura presente:** `Dockerfile`, `docker-compose.yml` (namenode +
2 datanodes + cliente con perfil `tools`), `deploy/setup.sh` (provisión de EC2
Ubuntu 24.04 con Docker), `Makefile`, `.gitattributes` con normalización de
finales de línea (`*.bat` → CRLF, resto → LF), y seis scripts `.bat` para el
flujo de tres terminales en Windows.

**Entorno local:** venv con Python 3.14.6, `grpcio 1.83.1`, `grpcio-tools
1.83.1`, `protobuf 7.36.1`. Stubs generados y al día.

---

## 5. Estado real por semana

| Semana | Hito | Plan | Estado real |
|---|---|---|---|
| **6** | Contratos y esqueleto | en curso | 🟡 **6 de 8 ítems cerrados** |
| 7 | Namespace completo (RF1) + especificación | — | ⬜ sin empezar |
| 8 | Hito 1: put y get monolítico | — | ⬜ contrato listo, código no |
| 9 | Holgura, primer despliegue AWS | — | 🟡 `deploy/setup.sh` ya escrito |
| 10 | Hito 2: varios DataNodes | — | ⬜ mensajes ya definidos |
| 11 | Replicación factor 3 | — | ⬜ |
| 12 | Hito 3: Raft, re-replicación, seguridad | — | ⬜ campos ya en el `.proto` |
| 13 | Entrega | — | ⬜ |

### Semana 6 — detalle

- [x] Estructura del repositorio
- [x] `proto/dfsha.proto` con los tres servicios
- [x] Generación de stubs multiplataforma
- [x] Las cinco costuras en `common/interfaces.py`
- [x] NameNode responde Ping, Mkdir y Ls — **verificado en ejecución**
- [x] DataNode se registra por heartbeat — **verificado en ejecución**
- [ ] `docker compose up --build` levanta los tres contenedores — **sin verificar**
- [ ] Borrador del documento de definición del servicio — **no existe**

### Registro de decisiones

4 decisiones documentadas (D1–D4), todas de Semana 6, con el formato correcto
para la justificación arquitectónica del informe final: qué decidí, por qué, qué
descarté, y a qué me obliga. **D2 (WORM) y D3 (el cliente particiona) son las de
mayor peso evaluativo** porque explican por qué el NameNode no es cuello de
botella — que es el corazón de RNF1 y RNF5.

---

## 6. Hallazgos

Ordenados por severidad.

### 🔴 Crítico — No hay control de versiones

`git rev-parse` responde `fatal: not a git repository`. El `.gitignore` y el
`.gitattributes` están escritos y son correctos, pero **nunca se inicializó el
repositorio**. El README dice "una sola vez, al clonar", lo que sugiere que se
asumió que ya existía.

**Riesgo:** un proyecto de 8 semanas sin historial. Además, la entrega de un
curso de Sistemas Distribuidos casi con seguridad exige repositorio.

**Acción:** `git init`, commit inicial, y repositorio remoto. Es lo primero que
debería hacerse, antes de escribir una línea de la Semana 7.

### 🟠 Medio — Docker sin verificar

`docker` no está instalado en esta máquina, así que el último ítem funcional de
la Semana 6 no se puede marcar. Según el plan Docker no hace falta hasta la
Semana 10, pero **el `docker-compose.yml` ya está escrito y sin probar**: si
tiene un error, lo vas a descubrir en la semana más cargada del proyecto, no
ahora.

Revisión por lectura: la composición se ve correcta. El servicio `cliente` usa
`profiles: ["tools"]`, así que no arranca con `docker compose up` — bien. Los
dos DataNodes usan volúmenes separados (`dn1`, `dn2`) y el mismo puerto interno
`50060` en redes distintas — correcto.

**Punto a vigilar:** el `Dockerfile` usa `python:3.12-slim` y el venv local
Python 3.14.6. No rompe nada hoy, pero es una diferencia de entorno que conviene
tener consciente.

### 🟡 Menor — `Ls` no valida el token

`Mkdir` llama a `self._check(request.token, context)` pero `Ls` no. Con `NoopAuth`
da igual, pero cuando entre `FileAuth` en la Semana 7 esto se vuelve un hueco de
autorización real, y el `.proto` ya tiene el campo `token` en `PathRequest`.

**Acción:** al cerrar RF1 en la Semana 7, pasar la validación por las cinco RPCs
de namespace de forma uniforme.

### 🟡 Menor — `DeleteBlock` no está en el servicer

El `.proto` declara `DataNodeService.DeleteBlock`, pero `datanode/server.py` no
lo define ni siquiera como stub `UNIMPLEMENTED`. gRPC responderá `UNIMPLEMENTED`
por defecto, así que no rompe — pero `PutBlock` y `GetBlock` sí tienen stub
explícito con mensaje ("llega en la semana 8"), y la asimetría confunde.

### 🟡 Menor — `docs/pendientes.md` desfasado

`deploy/setup.sh` figura como pendiente de la Semana 9, pero ya está escrito y
completo. Conviene que el checklist refleje la realidad, porque es el documento
con el que mides el avance.

### ⚪ Observación — No hay pruebas automatizadas

No existe carpeta de tests. Para la Semana 8 la prueba clave está bien definida
en el plan ("subir 500 MB, borrar local, bajar, comparar hash"), pero hoy no hay
nada que corra sola. Un par de pruebas del `Namespace` (que es lógica pura, sin
red) costarían muy poco y te protegen cuando llegue `rm` con lease activo.

---

## 7. Próximos pasos — Semana 7

En orden de ejecución sugerido:

1. **`git init` + commit inicial + remoto.** Antes que nada.
2. **Cerrar RF1 en `namespace.py`:** `rmdir`, `rm`, `stat`.
3. **Conectar `Rmdir`, `Rm`, `Stat` en `namenode/server.py`** con sus
   `StatusCode` (`FAILED_PRECONDITION` para directorio no vacío).
4. **Subcomandos `rmdir`, `rm`, `stat` en el cliente.**
5. **`FileAuth`** reemplazando a `NoopAuth`, y validación de token uniforme en
   las cinco RPCs de namespace.
6. **Entregable: especificación definitiva del proyecto.**

### Las dos decisiones de diseño que hay que tomar esta semana

El código ya las tiene marcadas como preguntas abiertas, y ambas van al registro
de decisiones (D5, D6):

**a) `rmdir` sobre un directorio no vacío: ¿error o borrado recursivo?**
HDFS exige `-r` explícito. La opción conservadora (error con
`FAILED_PRECONDITION`) es más defendible y más fácil de justificar.

**b) `rm` sobre un archivo `UNDER_CONSTRUCTION` con lease activo.**
Esta es, como dice el propio comentario en el código, **la primera decisión real
de consistencia del proyecto**. Las opciones son rechazar mientras el lease esté
vivo, o invalidar el lease y dejar los bloques huérfanos para que el recolector
los limpie vía comando piggyback. La segunda es la que hace HDFS y la que mejor
conecta con el `HeartbeatResponse.commands` que ya está en el contrato.

Esta segunda decisión merece una entrada extensa en `docs/decisiones.md`: es
exactamente el tipo de razonamiento (consistencia bajo concurrencia, no solo
"funciona") que distingue un proyecto de Sistemas Distribuidos de una tarea de
programación.

---

## 8. Valoración

**Lo que está muy bien hecho:**

- El contrato gRPC anticipa 8 semanas de trabajo. Raft, pipeline de replicación
  y tokens de bloque ya tienen sus campos. No habrá que rehacer el `.proto`.
- Las cinco costuras son abstracciones reales, no ceremonia: cada mejora futura
  toca una clase.
- Cero direcciones escritas en el código. Es lo que permite que Windows, Docker
  y EC2 corran el mismo binario.
- El manejo de errores mapea el dominio a `StatusCode` de gRPC correctamente
  — se evalúa el uso del middleware, no solo que responda.
- El registro de decisiones tiene el formato adecuado desde el día uno.

**Lo que hay que corregir ya:**

- `git init`. Es el único hallazgo crítico.
- Probar `docker compose` antes de la Semana 10.
- Uniformar la validación de token al cerrar RF1.

El proyecto está donde debe estar en el calendario, con una base de diseño mejor
que la típica a esta altura. El riesgo no es técnico, es de proceso.
