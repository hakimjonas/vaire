# Spark Type Parity Plan

## Current State

Strongbow has 15 Column variants covering all Spark 4.1 primitive and
temporal types. All use unboxed primitive arrays. BinaryColumn uses a flat
Arrow-style layout (contiguous `Array[Byte]` + offset array). Only
`AnyColumn` boxes.

## Implemented (Tier 1 + Tier 2)

### Primitive Types (no opaque wrapper)

| Spark Type | Column Variant | Storage | Status |
|---|---|---|---|
| `IntegerType` | `IntColumn` | `Array[Int]` | Original |
| `LongType` | `LongColumn` | `Array[Long]` | Original |
| `DoubleType` | `DoubleColumn` | `Array[Double]` | Original |
| `FloatType` | `FloatColumn` | `Array[Float]` | Done |
| `ShortType` | `ShortColumn` | `Array[Short]` | Done |
| `ByteType` | `ByteColumn` | `Array[Byte]` | Done |
| `StringType` | `StringColumn` | `Array[String\|Null]` | Original |
| `BooleanType` | `BooleanColumn` | `Array[Boolean]` | Original |

### Temporal Types (opaque wrappers in `types/`)

| Spark Type | Column Variant | Storage | Opaque Type | Status |
|---|---|---|---|---|
| `DateType` | `DateColumn` | `Array[Int]` | `types.Date` | Original |
| `TimestampType` | `TimestampColumn` | `Array[Long]` | `types.Timestamp` | Done |
| `TimestampNTZType` | `TimestampNTZColumn` | `Array[Long]` | `types.TimestampNTZ` | Done |
| `YearMonthIntervalType` | `YearMonthIntervalColumn` | `Array[Int]` | `types.YearMonthInterval` | Done |
| `DayTimeIntervalType` | `DayTimeIntervalColumn` | `Array[Long]` | `types.DayTimeInterval` | Done |

### Binary (flat columnar layout)

| Spark Type | Column Variant | Storage | Opaque Type | Status |
|---|---|---|---|---|
| `BinaryType` | `BinaryColumn` | `Array[Byte]` + `Array[Int]` offsets | `types.Binary` | Done |

### String Variants (metadata only, maps to StringColumn)

| Spark Type | ColumnType | Status |
|---|---|---|
| `CharType(n)` | `ColumnType.CharType(n)` | Done |
| `VarcharType(n)` | `ColumnType.VarcharType(n)` | Done |

## Remaining (Tier 3)

### DecimalType(precision, scale)

Match types choose storage at compile time:

- `precision <= 18`: `Array[Long]` (unboxed, covers most financial data)
- `precision > 18`: `Array[java.math.BigDecimal]` (boxed, correct)

Spark makes this choice at runtime in `Decimal.fromLong`. Strongbow can
push it to compile time. Precision and scale carried as literal type
parameters: `Decimal[P <: Int, S <: Int]`.

### VariantType

Spark 4's semi-structured type. Strongbow can use Scala 3 to make this
type-safe:

- **Union type GADT**: `Variant` enum with typed cases
- **Inline extraction**: Match types resolve the result type at compile time
- **Schema-driven derivation**: `Mirror` derivation for typed extraction
- **Zero-overhead access**: Direct field extraction, no runtime type dispatch

### Nested StructType

Recursive `Column` support for nested structs as column values. Design TBD.

## TODO

- [ ] Tests for all new Column types (Float, Short, Byte, Timestamp,
      TimestampNTZ, YearMonthInterval, DayTimeInterval, Binary, Char, Varchar)
      covering: factory methods, fromValues round-trip, slice, take, concat,
      sortIndicesByColumn, compareAt, Schema encode/decode, Spark SchemaConverter
      + RowConverter round-trip
- [ ] DecimalType design and implementation
- [ ] VariantType design and implementation
- [ ] Nested StructType design

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
