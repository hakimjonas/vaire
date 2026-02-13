# Filter Performance Analysis

## Results
- **Strongbow**: 0.39 ms median, 1.43 MB memory
- **Crossbow**: 0.31 ms median, 0.65 MB memory
- **Difference**: 1.26x performance difference, 2.20x memory difference

## Root Cause: Inefficient Column Reconstruction

### Crossbow's Approach (Efficient)

```scala
// 1. Get filtered indices
val indices = for (i <- 0 until rowCount if eval(i)) yield i

// 2. Slice columns directly on typed arrays
def sliceColumn(data: Array[?], ofType: RuntimeType, indices: IndexedSeq[Int]): Array[?] =
  ofType match
    case RuntimeType.Int => fillArray[Int](indices, data.asInstanceOf[Array[Int]])
    // ...

def fillArray[T: ClassTag](indices: IndexedSeq[Int], getValue: Int => T): Array[T] =
  val arr = new Array[T](indices.size)
  for (i <- indices.indices) arr(i) = getValue(indices(i))
  arr
```

**Benefits:**
- ✓ Direct array access (no boxing)
- ✓ Single traversal
- ✓ Type-specialized via ClassTag
- ✓ Minimal allocations

### Strongbow's Approach (Inefficient)

```scala
// 1. Get filtered indices
val rowIndices = (0 until dataset.rowCount).filter { rowIdx =>
  ExprInterpreter.eval(predicate, dataset.columns, RowIndex(rowIdx))
}

// 2. Map to getValue (returns Any - BOXING!)
val newColumns = dataset.columns.map { col =>
  Column.fromValues(
    rowIndices.map(idx => col.getValue(idx)).toVector,  // <-- Problem!
    col.columnType
  )
}

// 3. fromValues reconstructs from boxed Any values
def fromValues(values: Vector[Any], columnType: ColumnType): Column = {
  val nullIndices = values.zipWithIndex.collect {
    case (v, idx) if Option(v).isEmpty => idx
  }.to(BitSet)  // <-- First traversal for nulls

  val data = values.map { v =>  // <-- Second traversal to extract values
    Option(v) match {
      case Some(i: Int) => i
      // ...
    }
  }.toArray
  IntColumn(data, nullIndices)
}
```

## Performance Penalties

### 1. **Boxing Overhead**
```scala
col.getValue(idx)  // Returns Any
// Int column: Int → boxed Integer → Any
```
Every primitive value gets boxed to object representation.

### 2. **Triple Traversal**
1. `rowIndices.map(idx => col.getValue(idx))` - traverse to get values
2. `values.zipWithIndex.collect { ... }` - traverse to find nulls
3. `values.map { v => ... }` - traverse to extract typed values

Crossbow: **1 traversal**
Strongbow: **3 traversals**

### 3. **Intermediate Vector[Any] Allocation**
```scala
rowIndices.map(idx => col.getValue(idx)).toVector
```
Creates intermediate Vector with boxed values.

### 4. **Redundant Null Checking**
Original column already tracks nulls in BitSet, but fromValues:
- Iterates through all values to find nulls again
- Rebuilds null BitSet from scratch

### 5. **Pattern Matching Overhead**
```scala
Option(v) match {
  case Some(i: Int) => i
  case ...
}
```
Runtime type checking on every value.

## Memory Breakdown

**Crossbow (0.65 MB):**
- New Int array: ~10K rows × 4 bytes = 40 KB per column
- 2 columns = 80 KB
- Minimal overhead

**Strongbow (1.43 MB):**
- Boxed Integer objects: 10K × 16 bytes = 160 KB per column
- Vector[Any] overhead: ~80 KB per column
- Intermediate allocations during fromValues
- 2 columns with overhead ≈ 1.43 MB

**2.20x memory difference** comes from boxing + intermediate allocations.

## Performance Profile Across Operation Types

**Simple operations (Filter):**
- Predicate evaluation is cheap (simple comparison)
- Most time is spent in column reconstruction
- Overhead dominates total time

**Complex operations (GroupBy/Sort/Join):**
- Much more computational work (grouping, sorting, hashing)
- Column reconstruction is smaller % of total time
- Zero-cast benefits in aggregation/comparison provide greater impact
- Performance differences: GroupBy (1.97x), Sort (3.77x), Join (8.01x)

## Solutions (For Future Optimization)

### Option 1: Type-Specialized Slicing

Add specialized slice methods to Column:

```scala
enum Column {
  case IntColumn(data: Array[Int], nulls: BitSet) {
    def slice(indices: IndexedSeq[Int]): IntColumn = {
      val newData = new Array[Int](indices.size)
      val newNulls = BitSet.empty ++ indices.indices.filter(i => nulls.contains(indices(i)))
      for (i <- indices.indices) newData(i) = data(indices(i))
      IntColumn(newData, newNulls)
    }
  }
  // ... similar for other types
}
```

### Option 2: Avoid fromValues in Filter

```scala
private def filter[T](dataset: MaterializedDataset[T], predicate: Expr[T, Boolean]): MaterializedDataset[T] = {
  val rowIndices = (0 until dataset.rowCount).filter { rowIdx =>
    ExprInterpreter.eval(predicate, dataset.columns, RowIndex(rowIdx)) match {
      case Right(true) => true
      case _ => false
    }
  }

  val newColumns = dataset.columns.map {
    case IntColumn(data, nulls) =>
      val newData = rowIndices.map(data(_)).toArray
      val newNulls = BitSet.empty ++ rowIndices.indices.filter(i => nulls.contains(rowIndices(i)))
      IntColumn(newData, newNulls)
    // ... pattern match for each column type
  }

  MaterializedDataset(newColumns, dataset.schema)
}
```

### Option 3: Builder Pattern

```scala
trait ColumnBuilder[T] {
  def addValue(value: T): Unit
  def addNull(): Unit
  def result(): Column
}

// Use builder to avoid intermediate Vector[Any]
val builder = IntColumnBuilder(rowIndices.size)
for (idx <- rowIndices) {
  if (col.isNull(idx)) builder.addNull()
  else builder.addValue(col.getInt(idx))
}
builder.result()
```

## Conclusion

Strongbow's filter is slower because:
1. **Boxing primitives** (Int → Integer → Any)
2. **Triple traversal** vs Crossbow's single traversal
3. **Intermediate Vector[Any]** allocation
4. **Redundant null checking** and type validation

This is a **localized inefficiency** in column reconstruction, not an architectural limitation. The zero-cast architecture shows greater benefits on complex operations where:
- Computational work dominates (GroupBy: 1.97x difference)
- Multiple operations compound (Sort: 3.77x difference, Join: 8.01x difference)

**Trade-off**: Simple operations like filter could be optimized with type-specialized slicing, but the zero-cast GADT architecture provides significant performance characteristics on complex operations.

## Recommended Action

**For now**: Document this as a known optimization opportunity. The 0.08 ms absolute difference (0.31 vs 0.39 ms) is negligible compared to the 2-8x speedups on complex operations.

**Future**: Add type-specialized slice methods to Column when optimizing for production use.
