# DFSha

Sistema de archivos distribuido por bloques, con alta disponibilidad,
colocacion por hash y semantica WORM.

**Curso:** Sistemas Distribuidos (SI3007) — Universidad EAFIT
**Opcion de arquitectura:** 1 (cliente/servidor con distribucion S2S)
**Referencia:** HDFS

---

## El diseno en una frase

El cliente le pregunta al NameNode **donde** estan los bloques, y despues
habla directo con los DataNodes para mover los bytes. Los datos nunca
atraviesan el NameNode.

```
                    CLIENTE
                   /        \
      metadatos   /          \   bloques de 128 MB
      (pequeno)  /            \  (alto volumen)
                v              v
          NameNode  <---  DataNode  DataNode  DataNode
                  heartbeat
```

---

## Requisitos en Windows

- **Python 3.10 o superior.** Al instalarlo marca *Add Python to PATH*.
- **Git.**
- **Docker Desktop** (con backend WSL2). Solo hace falta a partir de la
  semana 10; para empezar no lo necesitas.

Verifica que Python quedo bien:

```bat
python --version
```

Si no responde, cierra y vuelve a abrir la terminal.

---

## Puesta en marcha

Una sola vez, al clonar:

```bat
setup.bat
```

Eso crea el entorno virtual, instala `grpcio` y genera los stubs.

Luego abre **tres terminales en VS Code** (`Ctrl+Shift+ñ` abre otra) y
ejecuta una cosa en cada una:

| Terminal | Comando | Que hace |
|---|---|---|
| 1 | `1-namenode.bat` | Levanta el NameNode en el puerto 50051 |
| 2 | `2-datanode.bat` | Levanta un DataNode que se registra por heartbeat |
| 3 | `dfsha.bat ping`  | El cliente |

Prueba el cliente:

```bat
dfsha.bat ping
dfsha.bat mkdir /docs
dfsha.bat mkdir /docs/2026
dfsha.bat ls /
dfsha.bat ls /docs
```

Y comprueba el manejo de errores, que es la mitad de la gracia de gRPC:

```bat
dfsha.bat mkdir /docs        REM  ALREADY_EXISTS
dfsha.bat ls /noexiste       REM  NOT_FOUND
```

Si quieres un segundo DataNode, abre una cuarta terminal con
`3-datanode2.bat`. Corre en el puerto 50061 y guarda sus bloques en
`data\dn-2`, asi que no se pisa con el primero.

**Cada vez que edites `proto\dfsha.proto`** vuelve a generar los stubs:

```bat
proto.bat
```

---

## Estructura

```
proto/dfsha.proto        el contrato: los tres servicios gRPC
proto/gen/               stubs generados (no se versionan)
scripts/gen_proto.py     generacion multiplataforma, sin sed
common/pb.py             punto unico de importacion de los stubs
common/config.py         configuracion por variables de entorno
common/interfaces.py     las cinco costuras del sistema
namenode/namespace.py    el arbol de directorios, en memoria
namenode/server.py       NameNodeService y ControlService
datanode/server.py       almacenamiento de bloques y heartbeat
client/cli.py            la CLI
deploy/                  scripts para AWS
docs/decisiones.md       registro de decisiones de diseno
docs/pendientes.md       checklist por sprint
```

---

## Las cinco costuras

Todo lo que sabemos que va a cambiar esta detras de una interfaz, en
`common/interfaces.py`. Cuando llegues con una idea nueva, cambias una
clase y nada mas.

| Interfaz | v1 (ahora) | Hacia donde crece |
|---|---|---|
| `BlockPlacer`   | round-robin      | consistent hashing, balanceo por carga |
| `MetadataStore` | en memoria       | persistido, luego log Raft |
| `Replicator`    | copia unica      | escritura paralela, luego pipeline |
| `AuthProvider`  | todo pasa        | archivo, JWT, tokens de bloque, 2FA |
| `BlockCipher`   | passthrough      | AES-GCM en el cliente |

---

## Nunca escribas una direccion en el codigo

Toda direccion sale de una variable de entorno. Es lo que permite que el
mismo codigo corra sin tocarlo en Windows, en Docker y en EC2.

| Variable | Windows | Docker | EC2 |
|---|---|---|---|
| `NAMENODE_ADDR`  | `localhost:50051` | `namenode:50051` | IP privada |
| `ADVERTISE_ADDR` | `localhost:50060` | `datanode-1:50060` | IP privada |
| `DATA_DIR`       | `data\dn-1` | `/data` | `/data` |

El DataNode reporta su propia `ADVERTISE_ADDR` en el heartbeat, porque
solo el sabe por donde lo alcanzan los clientes.

---

## Docker (desde la semana 10)

```bat
docker compose up --build
docker compose run --rm cliente python client/cli.py ls /
docker compose down -v
```

---

## Estado

| Semana | Hito | Estado |
|---|---|---|
| 6  | Contratos y esqueleto                    | en curso |
| 7  | Namespace completo (RF1) + especificacion | |
| 8  | Hito 1: put y get monolitico             | |
| 9  | Holgura, primer despliegue en AWS        | |
| 10 | Hito 2: varios DataNodes                 | |
| 11 | Replicacion factor 3                     | |
| 12 | Hito 3: Raft, re-replicacion, seguridad  | |
| 13 | Entrega                                  | |

Ver `docs/pendientes.md` para el detalle.
