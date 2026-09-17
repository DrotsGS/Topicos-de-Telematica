"""
Punto unico de importacion de los stubs generados.

El codigo que genera protoc usa imports planos ('import dfsha_pb2'), que
no funcionan si importas proto.gen como paquete. En vez de reescribir el
archivo generado con sed (que no existe en Windows), agregamos proto/gen
al sys.path y lo importamos plano desde aqui.

Todo el proyecto hace:

    from common.pb import dfsha_pb2, dfsha_pb2_grpc
"""

import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_GEN = os.path.join(_ROOT, "proto", "gen")

if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
if _GEN not in sys.path:
    sys.path.insert(0, _GEN)

try:
    import dfsha_pb2
    import dfsha_pb2_grpc
except ImportError as e:
    raise ImportError(
        "No se encontraron los stubs de gRPC.\n"
        "Ejecuta primero:  python scripts/gen_proto.py"
    ) from e

__all__ = ["dfsha_pb2", "dfsha_pb2_grpc"]
