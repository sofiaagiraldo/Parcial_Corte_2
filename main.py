"""
Parcial Final Corte 2 — Cafetería U. Sabana.

Orquesta generación de CSV sucios, limpieza, exportación, SQLite y CRUD.
"""

from __future__ import annotations

from pathlib import Path

from base_datos import MigracionCafeteria, OperacionesVentas
from datos_sucios import GeneradorDatosSucios
from limpieza import OrquestadorLimpieza

BASE = Path(__file__).resolve().parent
DB_PATH = BASE / "parcial_2_cafeteria.db"


def main() -> None:
    print("Fase 1: generando CSV sucios…")
    GeneradorDatosSucios(BASE).generar_todos()

    print("Fase 2: limpieza y exportación de CSV limpios…")
    dfs = OrquestadorLimpieza(BASE).ejecutar()

    print("Fase 3: migración a SQLite con to_sql…")
    mig = MigracionCafeteria(DB_PATH)
    mig.migrar_dataframes(dfs["productos"], dfs["clientes"], dfs["proveedores"])

    print("Fase 4: tabla ventas y CRUD…")
    mig.crear_tabla_ventas()
    ops = OperacionesVentas(DB_PATH)
    ops.insertar_cinco_ejemplos()

    print("\n--- READ: INNER JOIN (cliente, producto, total) ---")
    for fila in ops.consulta_join():
        print(fila)

    ops.actualizar_venta_uno(cantidad=5, total_venta=12500.0)
    print("\nUPDATE aplicado: id_venta = 1 (cantidad=5, total_venta=12500).")

    ops.eliminar_venta_tres()
    print("DELETE aplicado: id_venta = 3 eliminado.\n")

    print("--- READ después de UPDATE y DELETE ---")
    for fila in ops.consulta_join():
        print(fila)

    print(f"\nBase de datos lista: {DB_PATH}")


if __name__ == "__main__":
    main()
