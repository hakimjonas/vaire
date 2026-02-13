# Range Approach Benchmark Results

## WithRange Implementation

```scala
def buildNullSet(nulls: BitSet, indices: IndexedSeq[Int]): BitSet = {
  val builder = BitSet.newBuilder
  indices.indices.foreach { dstIdx =>  // Range, not zipWithIndex!
    if (nulls.contains(indices(dstIdx))) builder += dstIdx
  }
  builder.result()
}
```

## Results - buildNullSet (500K elements)

| Nulls | WithVars Time | WithRange Time | Speedup | WithVars Memory | WithRange Memory | Memory Ratio |
|-------|---------------|----------------|---------|-----------------|------------------|--------------|
| 0%    | 7.46 ms      | 6.32 ms       | 0.85x ✅ | 0 MB           | 764 MB          | ∞ ❌ |
| 10%   | 7.74 ms      | 7.25 ms       | 0.94x ✅ | 12 MB          | 852 MB          | 71x ❌ |
| 50%   | 10.09 ms     | 9.54 ms       | 0.95x ✅ | 12 MB          | 1158 MB         | 97x ❌ |

## Results - buildNullSet (50K elements)

| Nulls | WithVars Time | WithRange Time | Speedup | WithVars Memory | WithRange Memory | Memory Ratio |
|-------|---------------|----------------|---------|-----------------|------------------|--------------|
| 0%    | 0.33 ms      | 0.26 ms       | 0.78x ✅ | 0 MB           | 76 MB           | ∞ ❌ |
| 10%   | 0.61 ms      | 0.52 ms       | 0.85x ✅ | 0 MB           | 84 MB           | ∞ ❌ |
| 50%   | 0.80 ms      | 0.76 ms       | 0.95x ✅ | 0 MB           | 116 MB          | ∞ ❌ |

## Conclusion

**WithRange is NOT acceptable:**
- ✅ Performance: 5-22% faster
- ❌ Memory: **64-97x more memory** at 500K scale
- ❌ Memory: **Still allocating hundreds of MB vs single-digit MB**

The Range approach eliminates zipWithIndex overhead but:
- BitSet.newBuilder + foreach still creates intermediate allocations
- Memory usage is MUCH better than WithBuilderIterator (192x) or WithIterator (192x)
- But **still completely unacceptable** for columnar interpreter

## Why the Memory Difference?

The while loop with var:
```scala
var i = 0
while (i < indices.size) {
  if (nulls.contains(indices(i))) builder += i
  i += 1
}
```

Is **dramatically more memory efficient** than functional alternatives because:
1. No intermediate Range object (even though Range is O(1), it has overhead)
2. No closure allocations for foreach
3. Pure imperative loop - zero allocations except builder operations
4. JVM can optimize while loops better than higher-order functions

**The columnar interpreter MUST be memory efficient.** 64-97x memory overhead is unacceptable.
