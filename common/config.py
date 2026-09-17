"""Configuracion por variables de entorno.

Nunca escribas una IP o un puerto dentro del codigo. Este patron es el
que hace que el mismo codigo corra sin cambios en Windows, en Docker y
en EC2 (es el REMOTE_HOST del laboratorio de la calculadora).
"""

import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def data_dir(node_id: str) -> str:
    """Carpeta de bloques del DataNode.

    En Docker apunta a /data (un volumen por nodo).
    En Windows cae en <repo>/data/<node_id>, para que dos DataNodes
    locales no se pisen los bloques.
    """
    d = os.environ.get("DATA_DIR")
    if d:
        return d
    return os.path.join(ROOT, "data", node_id)
