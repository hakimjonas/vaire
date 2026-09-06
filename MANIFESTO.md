# The Vairë Manifesto

> **"The dichotomies of yesterday are the synergies of tomorrow"**
>
> A principled, type-safe dataset library embracing modern Scala 3

---

### Vision

Vairë is a **logically pure, execution-flexible dataset library** that treats datasets as **immutable descriptions of computation**, not eager collections. Transformations are represented as data, allowing multiple interpreters to execute the same logical plan on different backends (in-memory columnar, Spark).

**Core Insight:** Separate description (what) from execution (how). The logical plan is pure Scala 3 ADTs. Interpreters provide execution strategies.

---

### Principles

#### I. Arda Ecosystem Native

- **Core depends on Rumil and Sarati** — the Arda ecosystem's parser and codec libraries
- **No third-party production dependencies** — everything is built in-house (test-only libraries aside)
- **Logical plan as data**: Dataset[T], Expr[Row, A], Column[+A] are sealed enums
- **Integration modules**: Spark backend is optional, core works standalone

**Principle:** Own your dependencies. Build on libraries you control.

#### II. Modern Scala 3 All The Way

- **Native enums**: For Dataset, Expr, Column, ColumnType
- **GADTs**: Type parameters refined through pattern matching
- **Opaque types**: Zero-cost wrappers (RowIndex, ColumnIndex, Date)
- **Inline metaprogramming** (quotes/splices): compile-time lambda → expression compilation
- **Extension methods**: Fluent API without inheritance
- **Prelude pattern**: Single canonical import
- **`-Yexplicit-nulls`**: Compiler-enforced null safety
- **`-language:strictEquality`**: No accidental equality
- **`-Werror`**: Warnings are errors

**Principle:** The type system is our ally. Use its full power.

#### III. Correctness Through Types

- **GADT expressions**: Expr[Row, A] carries type evidence — pattern matching refines types automatically
- **GADT columnar storage**: Column[+A] — IntColumn extends Column[Int], pattern matching gives typed arrays
- **Invariant Dataset**: Dataset[T] enables GADT equality refinement in pattern matching
- **Compiler-enforced null safety**: String | Null for nullable, BitSet for SQL NULL tracking
- **Schema as evidence**: Compile-time derivation via Mirror
- **Smart constructors**: Invalid states unrepresentable

**Principle:** If it compiles, the plan is structurally valid. Runtime errors come from data quality, not type mistakes.

**Cast Philosophy:**
- Zero `asInstanceOf` in interpreters — all type safety comes from GADT refinement
- Casts exist only at documented erasure boundaries — the quote/splice boundary of inline metaprogramming (a compiler limitation) and erased storage access in the interpreter (AnyColumn reads where type erasure makes pattern matching impossible)
- No silent defaults, no unchecked casts, no type erasure workarounds

#### IV. Separation of Description and Execution

- **Lazy plan construction**: Building Dataset[T] performs zero computation
- **Explicit interpretation**: Choose execution strategy at the boundary
- **Inspectable plans**: Plans are first-class values for analysis and explanation
- **Either for errors**: Core uses Either[ExecutionError, A] consistently

**Principle:** Separate what you want (the plan) from how you get it (the interpreter). Description is pure.

#### V. Columnar Evaluation

- **All evaluation is columnar**: evalColumn is the single evaluation path
- **Typed arrays flow through**: Array[Int], Array[Long], Array[Double] — no boxing
- **Array.tabulate for construction**: Functional array building, not var + while
- **foldLeft for accumulation**: Functional reduction, not mutable accumulators
- **Zero var, zero return, zero throw** in the entire production codebase

**Principle:** Mutation is never the answer. Correctness and performance come from the right abstractions.

#### VI. Correctness First, Performance Later

- Build it right, then make it fast
- Measure before optimizing — optimization without a measurement is guesswork
- A slow correct implementation beats a fast broken one
- Benchmarks prove claims, not assumptions

**Principle:** Correctness is the foundation for great software.

---

*Designed and developed by Hakim Jonas Ghoula.*
