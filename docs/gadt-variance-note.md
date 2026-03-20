# GADT Variance and Pattern Matching in Scala 3

## The Problem

When pattern matching on a sealed enum with covariant type parameters,
Scala 3's GADT solver gives subtype bounds instead of type equality.
This prevents returning typed values without `asInstanceOf` casts.

```scala
enum Dataset[+T] {
  case InnerJoin[A, B](left: Dataset[A], right: Dataset[B])
    extends Dataset[(A, B)]
}

def execute[T](ds: Dataset[T]): Result[T] = ds match {
  case jn: Dataset.InnerJoin[a, b] =>
    // Compiler knows: T >: (a, b)  (subtype bound)
    // Compiler does NOT know: T = (a, b)  (equality)
    // So Result[(a, b)] does NOT typecheck as Result[T]
    result.asInstanceOf[Result[T]]  // cast needed
}
```

## Why Subtype Bounds Are Correct

With `Dataset[+T]`, a `Dataset[Any]` can hold a `Dataset.InnerJoin[Int, String]`
because `Dataset[(Int, String)] <: Dataset[Any]`. If the solver assumed
`T = (Int, String)` when matching, it would be unsound — `T` is `Any`, not
`(Int, String)`.

The solver correctly gives `T >: (a, b)` because that's the only sound
constraint under covariance.

## The Fix

Make the enum invariant:

```scala
enum Dataset[T] {  // no +
  case InnerJoin[A, B](left: Dataset[A], right: Dataset[B])
    extends Dataset[(A, B)]
}
```

With invariant `T`, holding an `InnerJoin[A, B]` means `T` is exactly
`(A, B)`. The solver gives type equality. Pattern matching refines `T`
to `(A, B)`. No cast needed.

## When This Applies

This matters when:
- You have a sealed enum/ADT with type parameters
- Cases extend the parent with specific type arguments
- You pattern match and need the refined type for the return value

It does NOT matter when:
- You only read values from the matched case (field access is always typed)
- The return type doesn't depend on the refined type parameter

## Practical Impact

In strongbow, changing `Dataset[+T]` to `Dataset[T]` eliminated 18
`asInstanceOf` casts from DatasetInterpreter. The covariance was never
used — no code in the codebase passed `Dataset[Subtype]` where
`Dataset[Supertype]` was expected. Removing it cost nothing.

## References

- scala/scala3#11956: "You'll get more precise type constraints in your
  pattern match if you align the variance."
- Scala 3 spec: GADT constraints are sound — covariant parameters give
  subtype bounds, invariant parameters give equality.
