# Spark Type Parity Plan

## Current State

Strongbow has 7 Column variants: `IntColumn`, `LongColumn`, `DoubleColumn`,
`StringColumn`, `BooleanColumn`, `DateColumn`, `AnyColumn`. All use unboxed
primitive arrays except `AnyColumn` (boxed fallback). Types not matching a
specific variant fall to `AnyColumn` via `SchemaConverter.fromSparkType`.

## Missing Types (vs Spark 4.1)

### Tier 1: Unboxed Primitive Extensions

Mechanical extensions of the existing `DateColumn` pattern: opaque type over
a JVM primitive, new Column variant, one-line additions to pattern match helpers.

| Spark Type | JVM Storage | Opaque Type | Notes |
|---|---|---|---|
| `FloatType` | `Array[Float]` | `types.Float` | 4 bytes/element |
| `ShortType` | `Array[Short]` | `types.Short` | 2 bytes/element |
| `ByteType` | `Array[Byte]` | `types.Byte` | 1 byte/element |
| `TimestampType` | `Array[Long]` | `types.Timestamp` | Epoch micros |
| `TimestampNTZType` | `Array[Long]` | `types.TimestampNTZ` | Epoch micros, no TZ |
| `YearMonthIntervalType` | `Array[Int]` | `types.YearMonthInterval` | Months as int |
| `DayTimeIntervalType` | `Array[Long]` | `types.DayTimeInterval` | Micros as long |
| `BinaryType` | `Array[Array[Byte]]` | -- | Only boxed type in this tier |

### Tier 2: String Variants (Metadata Only)

Map to `StringColumn` with metadata preserved in `ColumnType`.

| Spark Type | Storage | Notes |
|---|---|---|
| `CharType(n)` | `StringColumn` | Fixed-length, `ColumnType.CharType(n)` |
| `VarcharType(n)` | `StringColumn` | Max-length, `ColumnType.VarcharType(n)` |

### Tier 3: Typed Advantage (Exceeds Spark Safety)

These leverage Scala 3 type-level features to provide guarantees Spark cannot.

#### DecimalType(precision, scale)

Match types choose storage at compile time:

- `precision <= 18`: `Array[Long]` (unboxed, covers most financial data)
- `precision > 18`: `Array[java.math.BigDecimal]` (boxed, correct)

Spark makes this choice at runtime in `Decimal.fromLong`. Strongbow pushes it
to compile time. Precision and scale can be carried as literal type parameters:
`Decimal[P <: Int, S <: Int]`.

#### VariantType

Spark 4's semi-structured type. Spark treats it as an opaque blob with runtime
type-checked extraction (`variant_get(col, "$.price", "double")`). The type
name is a string argument, checked and fails at runtime.

Strongbow can use Scala 3 to make this type-safe:

- **Union type GADT**: `Variant` enum with typed cases (`VInt`, `VString`,
  `VArray`, `VObject`)
- **Inline extraction**: Match types resolve the result type at compile time
- **Schema-driven derivation**: When variant schema is known (Parquet, Delta),
  `Mirror` derivation enables `variantDataset.extractAs[Price]` with
  compile-time verification against the variant schema
- **Zero-overhead access**: Direct field extraction, no runtime type dispatch

This is an area where Strongbow can offer stronger guarantees than Spark/Databricks.

#### Nested StructType

Strongbow handles top-level structs via `Schema`. Nested structs as column
values need recursive `Column` support. Design TBD — interacts with how
`ExprInterpreter` accesses nested fields.

## Architecture

The pattern is proven by `DateColumn`: opaque type + primitive array + GADT
refinement = zero-cost type safety. Today's refactoring (IArray sort helper,
nullSafe compare, traverseEither) ensures each new variant adds one line per
match site, not a block.

Key files to extend:
- `types/` — new opaque types
- `Column.scala` — new enum cases
- `ColumnType.scala` — new enum cases
- `DatasetInterpreter.scala` — `sortIndicesByColumn`, `compareColumnValues`
- `SparkInterpreter.scala` — `sortIndicesByColumn`
- `ExprInterpreter.scala` — expression evaluation dispatch
- `SchemaConverter.scala` — Spark DataType mapping
- `RowConverter.scala` — Spark Row conversion

## Guiding Principle

The reviewer framed this as "13 types to add for parity." The reframe:
Strongbow can offer stronger guarantees on every one of these types than Spark
does natively. Typed decimals with compile-time precision. Typed variant
extraction with compile-time schema validation. Typed timestamps that cannot
be confused with longs. That is not parity — it is an advantage.
