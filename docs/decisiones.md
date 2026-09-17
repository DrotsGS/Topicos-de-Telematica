# Registro de decisiones

Una entrada por decision. Esto se convierte solo en la seccion de
justificacion arquitectonica del informe final, que vale nota.

Formato: que decidi, por que, que descarte, y a que me obliga.

---

## D1 - Opcion 1: C/S con distribucion S2S

**Semana:** 6
**Decision:** arquitectura cliente/servidor con el servicio distribuido por
composicion, no P2P con SuperPeers.
**Por que:** el diseno tiene un NameNode central con autoridad unica sobre los
metadatos, que es master-worker puro. La opcion 2 habria exigido metadatos
distribuidos, lookup por DHT y red overlay entre organizaciones.
**Descartado:** P2P con SuperPeers.
**Me obliga a:** replicar el NameNode, porque un master unico es punto unico
de falla y RNF2 pide alta disponibilidad.

## D2 - WORM en vez de CRUD

**Semana:** 6
**Decision:** los bloques son inmutables una vez escritos. CRUD a nivel de
archivo (crear, leer, borrar), WORM a nivel de bloque.
**Por que:** con bloques de 128 MB, actualizar un byte obliga a reescribir el
bloque completo y sus tres replicas. Bloques inmutables eliminan la
reconciliacion entre replicas.
**Descartado:** CRUD con escritura aleatoria.
**Me obliga a:** modelar el estado UNDER_CONSTRUCTION -> COMMITTED, y explicar
como se cubre RF3: `lock()` pasa a ser un lease de escritor unico durante la
creacion, y `write()` solo existe mientras el archivo esta en construccion.

## D3 - El cliente particiona y reensambla

**Semana:** 6
**Decision:** el cliente parte el archivo en bloques y los reensambla. El
NameNode nunca ve un byte de datos.
**Por que:** saca al NameNode del camino de los datos. Si los bytes pasaran por
el, el ancho de banda del sistema estaria limitado por una sola maquina y se
caen RNF1 y RNF5.
**Descartado:** que el servidor particione.
**Me obliga a:** relajar la transparencia de acceso. Se usa CLI y SDK propio
en vez de montar en el sistema operativo, igual que HDFS. La transparencia de
localizacion si es total: el cliente pide las ubicaciones en cada operacion.

## D4 - Un solo .proto en la semana 6

**Semana:** 6
**Decision:** los tres servicios en un solo archivo.
**Por que:** menos friccion de generacion de codigo mientras el contrato
todavia se mueve.
**Descartado:** tres archivos separados desde el inicio.
**Me obliga a:** separarlo en la semana 10, cuando haya varios DataNodes.

## D5 - `rmdir` no recursivo por defecto

**Semana:** 7
**Decision:** `rmdir` sobre un directorio con hijos falla con
`FAILED_PRECONDITION`. El borrado recursivo existe en la firma
(`rmdir(path, recursivo=False)`) pero el protocolo todavia no tiene como
pedirlo.
**Por que:** HDFS exige `-r` explicito por la misma razon: el borrado
recursivo silencioso es la forma mas rapida de perder datos. Ademas evita
tocar el `.proto` en la semana 7, cuando el tiempo estaba contra el hito 1.
**Descartado:** borrado recursivo implicito.
**Me obliga a:** agregar el flag a `PathRequest` cuando se quiera exponer.
La logica del namespace no cambia, solo el mensaje.

## D6 - `rm` sobre un archivo en construccion

**Semana:** 7
**Decision:** el `rm` siempre gana. Borra el nodo, el lease muere con el y
los bloques ya escritos se agendan para borrado. El cliente que estuviera
subiendo se entera cuando su `Complete` falle con `FAILED_PRECONDITION`.
**Por que:** tres razones. (1) Es lo que hace HDFS. (2) Conecta con
`HeartbeatResponse.commands`, que ya estaba en el contrato desde la semana
6: le da razon de ser a un mecanismo ya disenado. (3) Un path no puede
quedar bloqueado para siempre por un cliente que se murio, y eso importa
mas que dejar bloques huerfanos un rato.
**Descartado:** rechazar el `rm` mientras el lease este vivo. Es mas simple
y no deja huerfanos, pero si el cliente muere y el lease nunca expira, ese
path queda preso hasta que exista expiracion de leases o un `abort` manual.
**Me obliga a:** reconocer la deuda: los bloques huerfanos ocupan disco
hasta que el recolector corra en la semana 11. La cola
`pendientes_borrado` ya los acumula y `BlockReport` tambien detecta los que
ningun archivo reclama.

## D7 - Metadatos en memoria hasta Raft

**Semana:** 8
**Decision:** el namespace y el block map viven solo en memoria. No hay
persistencia a disco.
**Por que:** en la semana 12 los reemplaza el log replicado de Raft.
Escribir ahora un formato de persistencia que se va a botar es trabajo
perdido en las semanas mas cargadas del proyecto.
**Descartado:** volcar el namespace a JSON en cada `Complete`. Si llega a
hacer falta antes de la semana 12, es una hora de trabajo, no seis.
**Me obliga a:** decir claramente que reiniciar el NameNode hoy pierde el
namespace, aunque no pierde los bloques: siguen en disco de los DataNodes
y `BlockReport` los vuelve a anunciar (ver D11).

## D8 - Cliente muerto entre `Create` y `Complete`

**Semana:** 8
**Decision:** el archivo queda `UNDER_CONSTRUCTION`, invisible para `ls`, y
un `rm` lo limpia (D6). El cliente, si sigue vivo, llama a `Abort` el.
**Por que:** es la consecuencia natural de D6 y no necesita maquinaria
nueva. El cliente ya hace `Abort` cuando una subida falla a mitad.
**Descartado:** expiracion automatica de leases por tiempo.
**Me obliga a:** implementar esa expiracion en la semana 12, cuando el
NameNode tenga a Raft debajo y un reloj comun entre replicas.

## D9 - Bloques en serie en el hito 1

**Semana:** 8
**Decision:** el cliente sube y baja los bloques uno por uno.
**Por que:** con un solo DataNode el paralelismo no compra nada, y en serie
es mucho mas facil de depurar mientras el streaming todavia es nuevo.
**Descartado:** paralelismo desde el principio.
**Me obliga a:** volver sobre esto en la semana 10, que es justo lo que se
hizo: `ThreadPoolExecutor` con `MAX_PARALELO`. Medido con 500 MB, la bajada
paso de 44.8 MB/s con un DataNode a 102.2 MB/s con cuatro.

## D10 - Hash consistente con 150 nodos virtuales

**Semana:** 10
**Decision:** `ConsistentHashPlacer`: anillo de hash con 150 vnodos por
nodo fisico, filtrando para que las replicas caigan en maquinas distintas.
**Por que:** con `hash % N`, agregar un DataNode remapea casi todas las
claves. Medido con 10000 claves, al pasar de 4 a 5 nodos el anillo movio el
**17.4%** (con modulo seria ~80%), y el reparto entre 4 nodos quedo en
22.1 / 27.0 / 26.2 / 24.7 por ciento. Sin vnodos ese reparto se
desbalancea por pura suerte de donde cae cada hash.
**Descartado:** `hash % N` y round robin (que fue la v1 de la semana 6).
**Me obliga a:** tener claro que **el hash es politica de colocacion
inicial, no mecanismo de busqueda**: el NameNode persiste donde quedo cada
bloque y no recalcula el hash al leer, asi puede desviarse del anillo si un
nodo esta lleno o caido sin que el sistema se pierda.

## D11 - Ubicaciones reconstruidas por block report

**Semana:** 10
**Decision:** el mapa `blockID -> DataNodes` no se persiste. Cada DataNode
manda su lista completa de bloques al arrancar y cada 60 s
(`BlockReport`), y avisa uno a uno con `BlockReceived`.
**Por que:** es lo que hace HDFS y lo que permite que D7 sea sostenible: lo
que hay que hacer durable es el namespace, no las ubicaciones. El que sabe
que bloques tiene un disco es el disco, no un registro que puede quedar
desactualizado.
**Descartado:** persistir las ubicaciones junto con el namespace.
**Me obliga a:** aceptar una ventana de hasta 60 s tras reiniciar el
NameNode en la que un `Open` puede responder `UNAVAILABLE` porque todavia
no sabe donde esta el bloque. Tambien da gratis la deteccion de bloques
huerfanos: los que ningun archivo reclama van a la cola de borrado.

## D12 - Separacion del `.proto` en cuatro archivos

**Semana:** 10
**Decision:** `dfsha_common.proto` con los tipos compartidos y uno por par
de comunicacion: `dfsha_namenode.proto`, `dfsha_datanode.proto` y
`dfsha_control.proto`.
**Por que:** cierra D4. Con los tres servicios ya implementados, el archivo
unico mezclaba tres contratos con ciclos de vida distintos. Separados, cada
par se documenta y evoluciona por su lado.
**Descartado:** seguir con un solo archivo.
**Me obliga a:** nada nuevo. Fue refactor puro, en su propio commit y con
las 98 pruebas pasando antes y despues.

---

## Plantilla para las siguientes

## Dn - Titulo

**Semana:**
**Decision:**
**Por que:**
**Descartado:**
**Me obliga a:**
