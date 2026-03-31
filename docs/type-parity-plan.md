# Spark Type Parity Plan

## Current State

Strongbow has 16 Column variants covering all Spark 4.1 primitive,
temporal, binary, and decimal types. All use unboxed primitive arrays
where possible. BinaryColumn uses a flat Arrow-style layout. DecimalColumn
stores unscaled Long values (precision ≤ 18). Only `AnyColumn` boxes.

## Implemented

### Primitive Types (no opaque wrapper)

| Spark Type | Column Variant | Storage | Status |
|---|---|---|---|
| `IntegerType` | `IntColumn` | `Array[Int]` | Done |
| `LongType` | `LongColumn` | `Array[Long]` | Done |
| `DoubleType` | `DoubleColumn` | `Array[Double]` | Done |
| `FloatType` | `FloatColumn` | `Array[Float]` | Done |
| `ShortType` | `ShortColumn` | `Array[Short]` | Done |
| `ByteType` | `ByteColumn` | `Array[Byte]` | Done |
| `StringType` | `StringColumn` | `Array[String\|Null]` | Done |
| `BooleanType` | `BooleanColumn` | `Array[Boolean]` | Done |

### Temporal Types (opaque wrappers in `types/`)

| Spark Type | Column Variant | Storage | Opaque Type | Status |
|---|---|---|---|---|
| `DateType` | `DateColumn` | `Array[Int]` | `types.Date` | Done |
| `TimestampType` | `TimestampColumn` | `Array[Long]` | `types.Timestamp` | Done |
| `TimestampNTZType` | `TimestampNTZColumn` | `Array[Long]` | `types.TimestampNTZ` | Done |
| `YearMonthIntervalType` | `YearMonthIntervalColumn` | `Array[Int]` | `types.YearMonthInterval` | Done |
| `DayTimeIntervalType` | `DayTimeIntervalColumn` | `Array[Long]` | `types.DayTimeInterval` | Done |

### Binary (flat columnar layout)

| Spark Type | Column Variant | Storage | Opaque Type | Status |
|---|---|---|---|---|
| `BinaryType` | `BinaryColumn` | `Array[Byte]` + `Array[Int]` offsets | `types.Binary` | Done |

### Decimal (unboxed Long, precision ≤ 18)

| Spark Type | Column Variant | Storage | Opaque Type | Status |
|---|---|---|---|---|
| `DecimalType(p, s)` p≤18 | `DecimalColumn` | `Array[Long]` + precision/scale metadata | `types.Decimal` | Done |
| `DecimalType(p, s)` p>18 | `AnyColumn` | `Array[Any\|Null]` (boxed BigDecimal) | — | Fallback |

### String Variants (metadata only, maps to StringColumn)

| Spark Type | ColumnType | Status |
|---|---|---|
| `CharType(n)` | `ColumnType.CharType(n)` | Done |
| `VarcharType(n)` | `ColumnType.VarcharType(n)` | Done |

### Semi-structured (metadata only, maps to AnyColumn)

| Spark Type | ColumnType | Status |
|---|---|---|
| `VariantType` | `ColumnType.VariantType` | Done |

## Planned (separate work)

### Typed Variant Access API

Scala 3 typed extraction layer on top of stored Variant data:

- **Union type GADT**: `Variant` enum with typed cases
- **Inline extraction**: Match types resolve the result type at compile time
- **Schema-driven derivation**: `Mirror` derivation for typed extraction
- **Zero-overhead access**: Direct field extraction, no runtime type dispatch

This is a typed API, not a storage format. VariantType storage and Spark
round-trip are already implemented via AnyColumn.

### Nested StructType

Recursive `Column` support for nested structs as column values. This is
an architecture change to the Column abstraction — child columns, recursive
slice/take/concat, and a new type parameter story. Separate design required.

### DecimalType precision > 18

Native `DecimalBigDecimalColumn` for precision > 18. Additive — add a
second variant without changing DecimalColumn. Currently falls to AnyColumn.

## Architecture

Comparison logic centralized in `Column.compareAt` — both interpreters
delegate. Adding a new Column variant requires:
- `column/Column.scala`: enum case + branches in `length`, `columnType`,
  `nullSet`, `getValue`, `take`, `concat`, `slice`, `empty`, `fromValues`,
  `compareAt`, `sortIndicesByColumn`
- `column/ColumnType.scala`: new case
- `Schema.scala`: given instance
- `spark/SchemaConverter.scala`: bidirectional mapping
- `spark/RowConverter.scala`: one-liner via `extract` helper

No changes needed in interpreters — they delegate to `Column.compareAt`
and `Column.sortIndicesByColumn`.

## Guiding Principle

Strongbow offers stronger guarantees on every type than Spark does natively.
Typed decimals with compile-time precision. Typed variant extraction with
compile-time schema validation. Typed timestamps that cannot be confused
with longs. That is not parity — it is an advantage.
