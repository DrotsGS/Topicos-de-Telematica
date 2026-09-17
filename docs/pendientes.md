# Pendientes por sprint

Marca lo que vas cerrando. Lo que traigas nuevo cada semana se agrega aqui.

## Semana 6 - Contratos y esqueleto

- [x] Estructura del repositorio
- [x] proto/dfsha.proto con los tres servicios
- [x] Generacion de stubs multiplataforma
- [x] Las cinco costuras en common/interfaces.py
- [x] NameNode responde Ping, Mkdir y Ls
- [x] DataNode se registra por heartbeat
- [ ] `docker compose up --build` levanta los tres contenedores
      (pendiente: Docker no esta instalado en la maquina de desarrollo)
- [ ] Borrador del documento de definicion del servicio

## Semana 7 - Namespace completo y especificacion

- [ ] `rmdir` en namespace.py  (decidir: recursivo o error si no esta vacio)
- [ ] `rm` en namespace.py     (decidir: que pasa con un lease activo)
- [ ] `stat` en namespace.py
- [ ] Conectar Rmdir, Rm y Stat en namenode/server.py con su StatusCode
- [ ] Subcomandos rmdir, rm y stat en el cliente
- [ ] FileAuth: usuarios en archivo, reemplaza a NoopAuth
- [ ] **Entregable: especificacion definitiva del proyecto**

## Semana 8 - Hito 1: put y get

- [ ] Cliente parte el archivo en bloques de BLOCK_SIZE
- [ ] DataNode.PutBlock: recibe el stream, escribe a disco, calcula sha256
- [ ] DataNode.GetBlock: lee y hace yield de chunks
- [ ] NameNode.Create: asigna blockIDs y ubicaciones, otorga lease
- [ ] NameNode.Complete: commit a COMMITTED
- [ ] NameNode.Open: devuelve la lista ordenada de bloques
- [ ] Cliente reensambla POR OFFSET a disco, nunca acumulando en memoria
- [ ] Prueba: subir 500 MB, borrar local, bajar, comparar hash

## Semana 9 - Holgura

- [ ] Primer despliegue en AWS Academy
- [x] deploy/setup.sh
- [ ] Adelantar mTLS y JWT (para aliviar la semana 12)
- [ ] Integrar ideas nuevas

## Semana 10 - Hito 2: varios DataNodes

- [ ] ConsistentHashPlacer
- [ ] BlockReport y BlockReceived
- [ ] Lectura y escritura en paralelo
- [ ] Separar el .proto en tres archivos
- [ ] **Entregable: especificacion de protocolos**

## Semana 11 - Replicacion

- [ ] ParallelReplicator (v1)
- [ ] Deteccion de nodo muerto
- [ ] Lectura con failover a otra replica
- [ ] Mejora pendiente a proposito: PipelineReplicator

## Semana 12 - Hito 3: HA, consistencia y seguridad

- [ ] Raft en el NameNode (usar biblioteca, no implementarlo)
- [ ] Re-replicacion automatica por piggyback
- [ ] mTLS, block access tokens, cifrado AES-GCM en el cliente, ACL

## Semana 13 - Entrega

- [ ] Mediciones: throughput 1 vs 4 DataNodes, tiempo de recuperacion
- [ ] Informe tecnico
- [ ] Video de 10 a 15 minutos
- [ ] README reproducible
