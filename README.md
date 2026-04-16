# Parcial Final — Corte 2 · Cafetería U. Sabana

**Estudiante:** *Sofia Giraldo*

---

## Entrega modular:

El proyecto usa varios archivos `.py` con **programación orientada a objetos** para encapsular generación de datos, limpieza con Pandas y migración/CRUD en SQLite.

| Archivo | Rol |
| :--- | :--- |
| `datos_sucios.py` | Clase `GeneradorDatosSucios`: crea los tres CSV sucios del enunciado. |
| `limpieza.py` | Clases `LimpiezaProductos`, `LimpiezaClientes`, `LimpiezaProveedores` y `OrquestadorLimpieza` (1NF–3NF, nulos, tipos, títulos). |
| `base_datos.py` | `MigracionCafeteria` (`df.to_sql`, PKs y tabla `ventas` con FK) y `OperacionesVentas` (CRUD en SQL). |
| `main.py` | Ejecuta las fases en orden. |

## Cómo ejecutar

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

Se generan o actualizan: los seis CSV (tres sucios y tres limpios) y `parcial_2_cafeteria.db`.

---

## Paso a paso realizado

### Fase 1 — Extracción (CSV sucios)

- Se escriben `parcial_2_productos_sucios.csv`, `parcial_2_clientes_sucios.csv` y `parcial_2_proveedores_sucios.csv` a partir de los datos del enunciado.

### Fase 2 — Limpieza y normalización (Pandas)

- **Nulos:** `stock` sin valor se reemplaza por `0`; teléfonos, contactos y correos ausentes por `"No Registra"`; fechas inválidas o faltantes por la fecha actual (ISO).
- **Estandarización:** precios sin `$`, tipo `float`; textos con `.str.title()`.
- **1NF:** se separan columnas compuestas: `producto_categoria` → `nombre_producto` y `categoria`; `cliente_tipo` → `nombre` y `tipo`; `empresa_ciudad` → `empresa` y `ciudad`.
- **2NF/3NF:** se elimina la columna `edad` en clientes por redundancia frente a `fecha_nacimiento`. El `id_proveedor` limpio coincide con el NIT para usarlo como clave en `ventas`.
- **Exportación:** `parcial_2_*_limpios.csv` para cada dominio.

### Fase 3 — SQLite

- Base `parcial_2_cafeteria.db`; `PRAGMA foreign_keys = ON`.
- Los tres DataFrames limpios se cargan con **`DataFrame.to_sql`**. Luego se reemplazan las tablas por versiones con **PRIMARY KEY** (necesario para que las llaves foráneas de `ventas` sean válidas en SQLite).

### Fase 4 — Tabla `ventas` y CRUD

- Se crea `ventas` con `id_venta` autoincremental y FK a `clientes`, `productos` y `proveedores`.
- **Create:** cinco `INSERT` con datos coherentes con las FK.
- **Read:** `SELECT` con `INNER JOIN` sobre cliente y producto mostrando totales.
- **Update:** se ajustan `cantidad` y `total_venta` para `id_venta = 1`.
- **Delete:** se elimina la fila con `id_venta = 3`.

---

## Salidas esperadas en el repositorio

- Código documentado (`*.py` como en esta entrega modular).
- Tres CSV sucios y tres CSV limpios.
- `parcial_2_cafeteria.db` con las tablas `productos`, `clientes`, `proveedores` y `ventas` (después del script, cuatro filas en `ventas` tras el `DELETE` del enunciado).
