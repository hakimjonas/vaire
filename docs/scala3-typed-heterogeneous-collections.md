# Scala 3 Typed Heterogeneous Collections — Research Findings

## Context

**Project:** strongbow — a type-safe, zero-dependency columnar query engine for Scala 3
**Scala version:** 3.7.4
**Problem:** The GADT `Expr[Row, A]` preserves type parameter `A` through pattern matching (the "one-cast-at-boundary" principle). For single-element operations this works perfectly:

```scala
case SortByExpr[T, K](parent: Dataset[T], expr: Expr[T, K],
    keyType: ColumnType, ordering: Ordering[K]) extends Dataset[T]
```

For N-ary operations (multi-column ORDER BY, GROUP BY, window functions), we need collections of `Expr[Row, A]` where `A` varies per element, while preserving the per-element type evidence (Ordering, ColumnType, etc.).

The naive approach `Vector[(Expr[T, Any], ColumnType, Boolean)]` erases `A` to `Any`, losing all GADT type refinement and forcing compensating runtime hacks (`Comparable[Any].compareTo`, `throw`, `null`).

**Date of research:** February 2026
**Codebase references:** strongbow, Eru (effect system), Valar (validation library) — all by the same author, all Scala 3.7.4

---

## 1. Scala 3 Tuple Fundamentals

### No Arity Limit
Scala 3 tuples have **no arity limit** (unlike Scala 2's Tuple22). They use:
- `Tuple1` through `Tuple22` for small tuples (unboxed where possible)
- `TupleXXL` (array-backed) for tuples with more than 22 elements

### Core Types
```scala
// Cons type — right-associative
type *:[H, T <: Tuple] // e.g., Int *: String *: EmptyTuple = (Int, String)

// Base types
EmptyTuple  // the empty tuple
NonEmptyTuple  // any tuple with at least one element

// Algebraic: every Tuple is either EmptyTuple or h *: t
```

### Type-Level Operations (from scala.Tuple companion)
| Operation | Description |
|-----------|-------------|
| `Tuple.Map[T, F]` | Apply type constructor F to each element |
| `Tuple.Zip[T, U]` | Zip two tuples at type level |
| `Tuple.Concat[T, U]` | Concatenate two tuple types |
| `Tuple.Elem[T, N]` | Element type at index N |
| `Tuple.Head[T]` | First element type |
| `Tuple.Tail[T]` | All but first element type |
| `Tuple.Size[T]` | Size as a literal Int type |
| `Tuple.Union[T]` | Union of all element types |
| `Tuple.InverseMap[T, F]` | Reverse of Map — extract inner types |

### Tuple.Map with Type Lambdas
```scala
type ExprTypes = (Int, String, Double)

// Simple type constructor:
type AllOptions = Tuple.Map[ExprTypes, Option]
// = (Option[Int], Option[String], Option[Double])

// Type lambda for multi-param constructors:
type AllExprs = Tuple.Map[ExprTypes, [A] =>> Expr[Row, A]]
// = (Expr[Row, Int], Expr[Row, String], Expr[Row, Double])

type AllSortKeys = Tuple.Map[ExprTypes, [A] =>> SortKey[Row, A]]
// = (SortKey[Row, Int], SortKey[Row, String], SortKey[Row, Double])
```

### Building Tuples — Type Preservation
```scala
// Direct construction — compiler infers precise type:
val keys = (SortKey(e1, intOrd, true), SortKey(e2, strOrd, false))
// Type: (SortKey[Row, Int], SortKey[Row, String])

// *: cons — also preserves types:
val keys2 = SortKey(e1, intOrd, true) *: SortKey(e2, strOrd, false) *: EmptyTuple
// Type: (SortKey[Row, Int], SortKey[Row, String])

// CRITICAL: Never annotate as Tuple or NonEmptyTuple — this widens away all precision!
```

---

## 2. summonAll — Typeclass Evidence for Tuple Elements

```scala
import scala.compiletime.summonAll

// Summon Ordering for each element type:
val orderings = summonAll[Tuple.Map[(Int, String, Double), Ordering]]
// Returns: (Ordering[Int], Ordering[String], Ordering[Double])

// Generic version:
inline def summonOrderings[T <: Tuple]: Tuple.Map[T, Ordering] =
  summonAll[Tuple.Map[T, Ordering]]

val ords = summonOrderings[(Long, Float)]
// Returns: (Ordering[Long], Ordering[Float])
```

**Behavior:** All-or-nothing — if ANY element type lacks a given instance, the entire `summonAll` fails at compile time.

---

## 3. inline match on Tuples — The Critical Pattern

### What Does NOT Work: Value-Level inline match
```scala
// THIS FAILS:
inline def broken[T <: Tuple](keys: T) = inline keys match {
  case cons: (SortKey[row, k] *: tail) =>
    cons.head.ordering  // ERROR: Tuple.Head doesn't reduce to SortKey[row, k]
}
```

The problem: `cons.head` has type `Tuple.Head[T & SortKey[row, k] *: tail]`, which the compiler **cannot reduce** to `SortKey[row, k]`.

### What DOES Work: erasedValue Type-Level Recursion
```scala
import scala.compiletime.erasedValue

inline def processKeys[Keys <: Tuple](keys: Tuple, idx: Int): Unit =
  inline erasedValue[Keys] match
    case _: EmptyTuple => ()
    case _: (SortKey[row, k] *: tail) =>
      val sk = keys.productElement(idx).asInstanceOf[SortKey[row, k]]
      // sk.ordering is Ordering[k], sk.expr is Expr[row, k] — linked!
      // ... process sk ...
      processKeys[tail](keys, idx + 1)
```

The `asInstanceOf` here is **safe** — the type parameter `Keys` tracks the exact structure at compile time, and `productElement` retrieves what's already there. This is the same pattern used by Scala 3's own derivation machinery and by the Valar library's macro.

### Compile-Time Unrolled Comparator (The Payoff)
```scala
inline def buildComparator[T, Keys <: Tuple](
  keys: Tuple, idx: Int
): (T, T) => Int =
  inline erasedValue[Keys] match
    case _: EmptyTuple => (_, _) => 0
    case _: (SortKey[T, k] *: tail) =>
      val sk = keys.productElement(idx).asInstanceOf[SortKey[T, k]]
      val rest = buildComparator[T, tail](keys, idx + 1)
      (a, b) =>
        val cmp = if sk.ascending then sk.ordering.compare(evalKey(sk.expr, a), evalKey(sk.expr, b))
                  else sk.ordering.compare(evalKey(sk.expr, b), evalKey(sk.expr, a))
        if cmp != 0 then cmp else rest(a, b)
```

This unrolls at compile time into typed comparisons — no `Comparable[Any]`, no runtime reflection.

### Compile-Time Recursion Limits
- Default: 32 successive inline expansions
- Configurable via `-Xmax-inlines N`
- 8-10 elements: easily within limits
- 20+: should work within default limit

---

## 4. Match Types

### Basics
```scala
type Elem[X] = X match
  case String      => Char
  case Array[t]    => t
  case Iterable[t] => t
```

Reduction is sequential. For each case:
1. If scrutinee `S <: P` → reduce
2. If S is provably disjoint from P → skip
3. Otherwise → STOP (stuck)

### Recursive Match Types on Tuples — Works
```scala
type ExtractKeys[T <: Tuple] <: Tuple = T match
  case EmptyTuple             => EmptyTuple
  case SortKey[_, k] *: tail  => k *: ExtractKeys[tail]

// ExtractKeys[(SortKey[Row, Int], SortKey[Row, String])]
// reduces to (Int, String)
```

Can decompose parameterized types inside tuple elements. Upper bound required for recursive match types.

### Match Types + GADTs
Can **extract** GADT type parameters:
```scala
type ExprResult[E] = E match
  case Expr[_, a] => a

// ExprResult[Expr[Row, Int]] reduces to Int
```

Cannot **branch on GADT constructors** (IntLit, StrLit, etc.) — match type patterns must be types, not value-level constructors.

### Match Types vs Tuple.Map
`Tuple.Map[T, F]` is itself a match type internally. Use it for uniform application. Use custom match types when you need decomposition (`SortKey[_, k] *: tail => k *: ...`).

### Critical Limitations
1. **Abstract scrutinee stays stuck.** `ExtractKeys[T]` where T is abstract doesn't reduce — need `inline` to force concrete types.
2. **No distribution over unions.** `IsString[Int | String]` stays stuck. Intentional (unlike TypeScript).
3. **`Tuple.Tail` inside inline match doesn't reduce.** `x.tail` has type `Tuple.Tail[T & h *: ts]` which doesn't simplify to `ts`. Requires `asInstanceOf[ts]`.
4. **No exhaustivity checking.** Unmatched types stay stuck silently.
5. **Non-final traits never provably disjoint.** `Dog` and `Cat` traits aren't disjoint because `class Chimera extends Dog with Cat` could exist.

---

## 5. Union Types

### Basics
```scala
A | B  // value is either A or B
// Commutative, associative, idempotent
// A <: A | B, B <: A | B
// Erases to Object at JVM level
```

### Error Composition Pattern (from Eru)
```scala
case Zip[E0, E1, A0, B0](left: Eru[E0, A0], right: Eru[E1, B0])
  extends Eru[E0 | E1, (A0, B0)]

def flatMap[E2, B](f: A => Result[E2, B]): Result[E | E2, B]
```

Errors compose naturally. Each operation declares its own error type; the compiler infers the union.

### NOT Useful for Heterogeneous Collections
`Vector[Expr[T, Int] | Expr[T, String]]` is barely better than `Vector[Expr[T, Any]]`:
- Erases to Object at JVM level
- Must enumerate every possible type
- No generic Ordering requirement across the union
- No exhaustivity checking

### Union + Match Types: No Distribution
`IsString[Int | String]` stays stuck. Scala 3 deliberately avoids TypeScript's distributive conditional types.

---

## 6. PolyFunction and tuple.map

### Works but with Constraints
```scala
val data = (1, "hello", 3.14)
val wrapped = data.map([t] => (x: t) => Option(x))
// Type: (Option[Int], Option[String], Option[Double])
```

Signature: `[t] => t => F[t]` — must map each element of type `t` to `F[t]`.

### Does NOT Work for Heterogeneous Per-Element Processing
```scala
// FAILS: output type isn't F[t]
data.map([t] => (x: t) => x.toString)

// FAILS: type variable binds to whole element, not inner param
keys.map([K] => (sk: SortKey[Row, K]) => sk.ascending)
```

For heterogeneous per-element processing, use inline erasedValue recursion instead.

---

## 7. The Existential Bundling Pattern

### Case Class with Linked Type Parameter (Best for Tuples)
```scala
case class SortKey[T, K](expr: Expr[T, K], ordering: Ordering[K], ascending: Boolean)
// K links expr and ordering — compiler enforces consistency
```

### Trait with Abstract Type Member (Best for Vectors)
```scala
trait SortSpec[T]:
  type K
  val expr: Expr[T, K]
  val ordering: Ordering[K]
  val ascending: Boolean

// Compiler enforces: sk.ordering: Ordering[sk.K] and sk.expr: Expr[T, sk.K]
// Can be stored in Vector[SortSpec[T]]
```

### Wildcard Existential (BROKEN — Don't Use)
```scala
case class BrokenSortKey[T](expr: Expr[T, ?], ordering: Ordering[?], ascending: Boolean)
// NO link between expr's ? and ordering's ? — they could be different types
```

---

## 8. Eru's GADT Patterns — Zero-Cast Reference

### Core Design
Eru's effect GADT `Eru[+E, +A]` achieves **zero `asInstanceOf` in its entire interpreter** through:

1. **Pure ADT pattern matching refines types.** Each `case` arm in `runFiberLoop` automatically narrows `E` and `A`.

2. **Continuation[+E, -In, +Out] chains types without casts:**
```scala
case Step[+E1, In1, Mid1, +Out1](
  f: In1 => Eru[E1, Mid1],
  next: Continuation[E1, Mid1, Out1]
) extends Continuation[E1, In1, Out1]
```
When the interpreter matches `Step(f, next)`, the compiler knows `f` takes `In1`, produces `Mid1`, and `next` expects `Mid1`.

3. **Union types for error composition:** `Zip` returns `Eru[E0 | E1, (A0, B0)]`, composing errors automatically.

4. **Heterogeneous composition via pairwise Zip, not collections:** Eru combines exactly 2 values at a time, producing `(A0, B0)` tuples. No HLists, no `summonAll`, no inline metaprogramming.

5. **Variance annotations control type flow:** `+E`, `+A` (covariant), `-In` (contravariant on Continuation).

### Key Insight for Strongbow
Eru avoids the N-ary heterogeneous collection problem entirely by chaining pairwise. This is an alternative to the tuple approach.

---

## 9. Valar's Macro Patterns — Tuple Iteration at Compile Time

### Mirror-Based Derivation
```scala
inline def derive[T](using m: Mirror.ProductOf[T]): Validator[T] =
  ${ deriveImpl[T, m.MirroredElemTypes, m.MirroredElemLabels]('m) }
```

### Tuple Type Walking in Macros
```scala
// Inside the macro (scala.quoted space):
def generateAllValidations[E <: Tuple: Type](
  aExpr: Expr[T], index: Int, labels: List[String]
): List[Expr[ValidationResult[Any]]] =
  Type.of[E] match
    case '[EmptyTuple] => Nil
    case '[h *: t] =>
      val validatorExpr = Expr.summon[Validator[h]].get
      val fieldValidation = generateFieldValidation[h](aExpr, label, index, validatorExpr)
      fieldValidation :: generateAllValidations[t](aExpr, index + 1, labels.tail)
```

### Key Pattern: Manual Recursion with Expr.summon (NOT summonAll)
Valar does not use `summonAll`. It manually recurses through the tuple type, calling `Expr.summon[Validator[h]]` for each element. This gives better error messages (collects ALL missing validators before reporting).

### Three-Tier Field Access (zero casts for case classes and regular tuples)
1. Regular tuples: `Select.unique(aExpr.asTerm, s"_${index + 1}")`
2. Case classes: `Select.unique(aExpr.asTerm, label)`
3. Named tuples: `productElement(index).asInstanceOf[H]` (matches stdlib pattern)

---

## 10. Viable Approaches for Strongbow

### Approach A: Eru-Style Pairwise Chaining
Avoid N-ary collections entirely. Chain operations pairwise:
```scala
case SortByExpr[T, K](parent: Dataset[T], expr: Expr[T, K],
    keyType: ColumnType, ordering: Ordering[K]) extends Dataset[T]
case ThenSortBy[T, K](parent: Dataset[T], expr: Expr[T, K],
    keyType: ColumnType, ordering: Ordering[K]) extends Dataset[T]
```
**Pros:** Zero casts, simplest type safety, follows Eru's proven pattern.
**Cons:** Dataset enum grows. The interpreter must collect the chain. Less natural API for users.

### Approach B: Typed Tuples + erasedValue Inline Recursion
```scala
case class SortKey[T, K](expr: Expr[T, K], ordering: Ordering[K], ascending: Boolean)

case SortByExprs[T, Keys <: Tuple](
  parent: Dataset[T],
  sortKeys: Keys
) extends Dataset[T]
```
Process with erasedValue inline match (one safe asInstanceOf per element).
**Pros:** Natural N-ary API, flexible, matches Valar/stdlib patterns.
**Cons:** One `asInstanceOf` per element (sound but present). Inline expansion.

### Approach C: Existential Type Member for Runtime Iteration
```scala
trait SortSpec[T]:
  type K
  val expr: Expr[T, K]
  val ordering: Ordering[K]
  val ascending: Boolean
```
Store as `Vector[SortSpec[T]]`.
**Pros:** Works with runtime iteration. Internal K link preserved.
**Cons:** Existential types are harder to work with. Construction requires wrapping.

### Hybrid Approach (Recommended)
- **Public API:** Typed tuples (Approach B) — users build `(SortKey(...), SortKey(...))` with full type safety
- **Dataset AST:** `SortByExprs[T, Keys <: Tuple]` carries the typed tuple
- **In-memory interpreter:** erasedValue inline recursion builds a typed comparator at compile time
- **Spark interpreter:** Convert to `Vector[SortSpec[T]]` at the boundary for runtime DataFrame column iteration

---

## 11. Summary Table

| Feature | Works | Doesn't Work |
|---------|-------|-------------|
| Typed tuples with different K per element | ✓ (SortKey[Row, Int], SortKey[Row, String]) | |
| Tuple.Map with type lambdas | ✓ `[A] =>> SortKey[Row, A]` | |
| summonAll for evidence across tuple | ✓ `summonAll[Tuple.Map[T, Ordering]]` | All-or-nothing |
| inline match on tuple values | | ✗ .head doesn't refine |
| erasedValue type-level recursion | ✓ (one safe asInstanceOf per element) | |
| Match types decomposing parameterized tuple elements | ✓ `SortKey[_, k] *: tail` | |
| Match types on abstract type params | | ✗ stays stuck |
| Match types distributing over unions | | ✗ intentionally not supported |
| Union types for error composition | ✓ (Eru pattern) | |
| Union types for heterogeneous vectors | | ✗ no better than Any |
| PolyFunction tuple.map for uniform transform | ✓ `[t] => t => F[t]` | Cannot change output shape |
| Compile-time recursion depth (8-10 elements) | ✓ well within 32 default | |
| Tuple.Tail inside inline match | | ✗ needs asInstanceOf |

---

## 12. Eru's GADT Patterns — Deep Dive

### The Effect GADT
```scala
enum Eru[+E, +A] {
  private case Succeed(value: A) extends Eru[Nothing, A]
  private case Fail(error: E) extends Eru[E, Nothing]
  private case Chain[E0, From, +To](source: Eru[E0, From], cont: Continuation[E0, From, To])
    extends Eru[E0, To]
  private case RecoverWith[E0, A0, +E2, +A1 >: A0](source: Eru[E0, A0],
    pf: PartialFunction[E0, Eru[E2, A1]]) extends Eru[E0 | E2, A1]
  private case Zip[E0, E1, A0, B0](left: Eru[E0, A0], right: Eru[E1, B0])
    extends Eru[E0 | E1, (A0, B0)]
  // ... more cases
}
```

### The Continuation GADT — Type-Safe FlatMap Chains
```scala
enum Continuation[+E, -In, +Out] {
  case End[A]() extends Continuation[Nothing, A, A]           // In = Out (identity)
  case Step[+E1, In1, Mid1, +Out1](
    f: In1 => Eru[E1, Mid1],
    next: Continuation[E1, Mid1, Out1]
  ) extends Continuation[E1, In1, Out1]                       // In1 -> Mid1 -> Out1
  case Compose[+E1, In1, Mid1, +Out1](
    first: Continuation[E1, In1, Mid1],
    g: Mid1 => Eru[E1, Out1]
  ) extends Continuation[E1, In1, Out1]
}
```

### Zero-Cast Interpreter
The interpreter (`runFiberLoop` and `runFiberContinuation`) pattern-matches directly on the GADT cases. The compiler refines type parameters in each match arm without any `asInstanceOf`:

```scala
private def runFiberContinuation[E, In, Out](
  cont: Continuation[E, In, Out], input: In, fins: List[Finalizer], ...
): TailRec[(Either[E, Out], List[Finalizer])] =
  cont match {
    case Continuation.End() =>
      done((Right(input), fins))           // input: In, but In =:= Out from End's definition
    case Continuation.Step(f, next) =>
      // f: In1 => Eru[E1, Mid1], next: Continuation[E1, Mid1, Out1]
      tailcall(runFiberLoop(f(input), ...)).flatMap {
        case (Right(intermediate), fs) =>  // intermediate: Mid1
          tailcall(runFiberContinuation(next, intermediate, ...))  // next expects Mid1
        case (Left(error), fs) => done((Left(error), fs))
      }
    case Continuation.Compose(first, g) =>
      tailcall(runFiberContinuation(first, input, ...)).flatMap {
        case (Right(intermediate), fs) =>  // intermediate: Mid1
          tailcall(runFiberLoop(g(intermediate), ...))
        case (Left(error), fs) => done((Left(error), fs))
      }
  }
```

### Union Types for Error Composition
```scala
// RecoverWith: can produce errors from original OR recovery path
extends Eru[E0 | E2, A1]

// Zip: can produce errors from either side
extends Eru[E0 | E1, (A0, B0)]

// Practical usage — errors compose automatically:
val result: Eru[NotFound | ParseError, Int] =
  findUser("1").flatMap(_ => parseAge("25"))
```

### Key Design Decisions
- **No match types** in the core — pure GADT pattern matching suffices
- **No summonAll/inline** in the interpreter — type safety from ADT structure alone
- **Pairwise composition only** — Zip combines exactly 2 at a time, no N-ary
- **Variance annotations** drive automatic widening at composition points
- **Macros only for compile-time hints** (validated, optimize), not for type computation

---

## 13. Match Types — Deep Dive

### Reduction Algorithm
For each case in a match type, the compiler checks:
1. If scrutinee `S <: P` → **reduce** to result type
2. If S is **provably disjoint** from P → **skip** to next case
3. Otherwise → **STOP** (type stays abstract/stuck)

"Provably disjoint" requires: different final classes, distinct singleton types, or single class inheritance. Non-final traits are NEVER provably disjoint.

### Recursive Match Types on Tuples (Confirmed Working)
```scala
// Extract inner type parameter from parameterized types in tuple elements:
case class SortKey[D, K](dir: D, key: K)

type ExtractKeys[T <: Tuple] <: Tuple = T match
  case EmptyTuple             => EmptyTuple
  case SortKey[_, k] *: tail  => k *: ExtractKeys[tail]

// ExtractKeys[(SortKey[Asc, Int], SortKey[Desc, String])]
// reduces to (Int, String)
```

Upper bound (`<: Tuple`) required for recursive match types.

### Match Types + GADTs — Partial Support
**Can extract type params:**
```scala
type ExprResult[E] = E match
  case Expr[_, a] => a
// ExprResult[Expr[Row, Int]] reduces to Int
```

**Cannot branch on GADT constructors** — match type patterns must be types, not value-level constructors (`IntLit`, `StrLit`, etc.).

**GADT witnesses don't propagate into match types.** In value-level code, `case IntLit(v) =>` tells the compiler `T = Int`. No type-level equivalent exists.

### Tuple.Tail Inside inline match — Known Limitation
```scala
inline def showAll[T <: Tuple](tup: T): AllShow[T] = inline tup match
  case _: EmptyTuple => EmptyTuple
  case x: (h *: ts) =>
    x.head.toString *: showAll[ts](x.tail.asInstanceOf[ts])  // asInstanceOf required!
```

`x.tail` has type `Tuple.Tail[T & h *: ts]` which doesn't reduce to `ts`. The cast is sound because the match verified the structure. This is a widely known limitation.

### Match Types Do NOT Distribute Over Unions
```scala
type IsString[X] = X match
  case String => true
  case Int    => false

// IsString[String | Int] STAYS STUCK — intentional, unlike TypeScript
```

---

## 14. Union Types — Deep Dive

### Subtyping Rules
- `A <: A | B` and `B <: A | B` (always)
- If `A <: C` and `B <: C`, then `A | B <: C`
- Commutative, associative, idempotent
- Erases to `Object` at JVM level

### Not Useful for Heterogeneous Collections
`Vector[Expr[T, Int] | Expr[T, String]]` is barely better than `Vector[Expr[T, Any]]`:
- Erases to Object at JVM
- Must enumerate every possible type at definition site
- Cannot express "for any A in the union, require Ordering[A]" generically
- No exhaustivity checking on union pattern matches
- Adding a new type requires updating the union everywhere

### Verdict
Union types are excellent for **error composition** (Eru's pattern) but **not useful** for our typed heterogeneous collection problem.

---

## 15. dwh-core — Heterogeneous Collection Patterns (Scala 2 & 3)

### Project Context
dwh-core is a data warehouse toolkit. The master branch uses Scala 2.13 with Shapeless; the scala3-migration branch uses native Scala 3 features.

### Scala 2: Shapeless HList Approach

**ProductOrdering via HList recursion:**
```scala
// Base case
implicit val hnilOrdering: ProductOrdering[HNil] = ...

// Recursive case — per-element Ordering evidence
implicit def hListOrdering[H, T <: HList](implicit
  hOrd: Ordering[H],           // Ordering for THIS element
  tOrd: ProductOrdering[T]     // Recursive ordering for rest
): ProductOrdering[H :: T] = ...

// Entry point: Generic.Aux converts case class to HList
implicit def productOrdering[T, Repr <: HList](implicit
  gen: Generic.Aux[T, Repr],
  ord: ProductOrdering[Repr]
): ProductOrdering[T] = ...
```

**Projector via LabelledGeneric + HList intersection:**
```scala
implicit def usingShapless[MainModel, ProjectedModel,
  Main <: HList, Projection <: HList, Intersected <: HList
](implicit
  genMain: LabelledGeneric.Aux[MainModel, Main],
  genProjected: LabelledGeneric.Aux[ProjectedModel, Projection],
  intersection: IntersectAndAlignByKeys.Aux[Main, Projection, Intersected],
  transform: InnerProjections[Intersected, Projection]
): ShapelessDerived[MainModel, ProjectedModel]
```

**Expression with Tuple1-22 exhaustive specialization:**
```scala
trait Tuple[TupleType] extends Expression[TupleType] with Product {
  def elements: Seq[Expression[_]] = productIterator.map(_.asInstanceOf[Expression[_]]).toSeq
}

object Tuple {
  final case class Tuple1[T0](e0: Expression[T0]) extends Tuple[scala.Tuple1[T0]]
  final case class Tuple2[T0, T1](e0: Expression[T0], e1: Expression[T1])
    extends Tuple[scala.Tuple2[T0, T1]]
  // ... up to Tuple22
}
```

**Codec with existential packing + factory function:**
```scala
final case class Product[M <: scala.Product: ClassTag](fields: Seq[FieldCodec])(
  factory: Seq[Any] => M
) extends AbstractCodec[M]
```

### Scala 3: Native Inline + erasedValue Approach

**ProductOrdering via inline tuple recursion:**
```scala
inline given derived[T](using m: Mirror.ProductOf[T]): ProductOrdering[T] = {
  val fieldOrderings = summonOrderings[m.MirroredElemTypes]

  new ProductOrdering[T] {
    def compare(x: T, y: T): Int = {
      val xProduct = x.asInstanceOf[Product]
      val yProduct = y.asInstanceOf[Product]
      var i = 0; var result = 0
      while (i < fieldOrderings.length && result == 0) {
        val ordering = fieldOrderings(i).asInstanceOf[Ordering[Any]]
        result = ordering.compare(xProduct.productElement(i), yProduct.productElement(i))
        i += 1
      }
      result
    }
  }
}

private inline def summonOrderings[T <: Tuple]: List[Ordering[?]] =
  inline erasedValue[T] match {
    case _: EmptyTuple => Nil
    case _: (h *: t) => summonOrdering[h] :: summonOrderings[t]
  }
```

**AbstractCodec derivation via Mirror:**
```scala
inline given derived[T](using m: Mirror.Of[T], ct: ClassTag[T]): AbstractCodec[T] =
  inline m match {
    case p: Mirror.ProductOf[T] => deriveProduct(p, ct)
    case s: Mirror.SumOf[T] => deriveSum(s, ct)
  }

private inline def summonCodecs[T <: Tuple]: List[AbstractCodec[?]] =
  inline erasedValue[T] match {
    case _: EmptyTuple => Nil
    case _: (h *: t) => summonCodec[h] :: summonCodecs[t]
  }
```

**Projector via Mirror (no Shapeless):**
```scala
inline given derived[From, To](using
  mFrom: Mirror.ProductOf[From], mTo: Mirror.ProductOf[To]
): Projector[From, To] = {
  val fromFieldNames = getFieldNames[mFrom.MirroredElemLabels]
  val toFieldNames = getFieldNames[mTo.MirroredElemLabels]
  val fieldIndices = getFieldIndices(fromFieldNames, toFieldNames)
  new Projector[From, To] {
    def apply(from: From): To = {
      val fromProduct = from.asInstanceOf[Product]
      val values = fieldIndices.map(fromProduct.productElement)
      mTo.fromProduct(Tuple.fromArray(values.toArray))
    }
  }
}
```

### Key Design Pattern: Dual Erasure Strategy
dwh-core maintains **compile-time type precision** (via `Expression[T]` GADTs and `SortKey[T, K]`-style case classes) while using **existential packing** (`Expression[_]`, `AbstractCodec[?]`, `List[Ordering[?]]`) for runtime heterogeneous traversal.

This is the same hybrid pattern identified as "Recommended" for strongbow: typed tuples at compile time, existential vectors at runtime boundaries.

### Key Observation: Ordering[Any] Cast in Scala 3
dwh-core's `ProductOrdering.derived` uses `fieldOrderings(i).asInstanceOf[Ordering[Any]]` for runtime comparison. The cast is safe because the Ordering was summoned at compile time for the correct type. This is the **same trade-off** as the erasedValue + productElement + asInstanceOf pattern: compile-time proves the types match, runtime uses a sound cast because JVM erases generics.

This validates that the "one safe asInstanceOf at the interpreter boundary" pattern is pragmatically accepted even in production Scala 3 FP codebases.

---

## 16. Cross-Project Pattern Comparison

| Aspect | Eru | Valar | dwh-core (Scala 2) | dwh-core (Scala 3) | Strongbow (current) | Strongbow (proposed) |
|--------|-----|-------|--------------------|--------------------|--------------------|--------------------|
| Heterogeneous strategy | Pairwise Zip | Mirror + macro tuple walk | Shapeless HList | inline erasedValue | Vector[..., Any] | Typed tuples |
| Type evidence | GADT pattern match | Expr.summon per element | Implicit resolution | summonOrdering[h] | LOST (Any) | SortKey[T,K] bundle |
| Runtime casts | Zero | productElement + asInstanceOf (named tuples only) | asInstanceOf in Expression[_] | Ordering[Any] cast | Comparable[Any] | Safe existential cast |
| N-ary support | Chain pairwise | Arbitrary (tuple arity) | Tuple1-22 cases | Arbitrary (tuple arity) | Vector (erased) | Tuple (typed) |
| Error handling | Either + union types | ValidationResult enum | Exceptions | Exceptions | Mixed throw/Either | Either throughout |

---

## References

- Scala 3 Tuple documentation: https://docs.scala-lang.org/scala3/reference/other-new-features/tuples.html
- Scala 3 Match Types: https://docs.scala-lang.org/scala3/reference/new-types/match-types.html
- Scala 3 Union Types: https://docs.scala-lang.org/scala3/reference/new-types/union-types.html
- Eru project: /home/hakim/examples/eru (zero-cast GADT interpreter reference)
- Valar project: /home/hakim/examples/valar (macro tuple iteration reference)
- dwh-core project: /home/hakim/dwh-core (production Scala 2→3 migration reference)
- strongbow project: /home/hakim/examples/strongbow (the project being improved)
