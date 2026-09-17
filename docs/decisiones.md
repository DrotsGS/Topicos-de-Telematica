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

---

## Plantilla para las siguientes

## Dn - Titulo

**Semana:**
**Decision:**
**Por que:**
**Descartado:**
**Me obliga a:**
