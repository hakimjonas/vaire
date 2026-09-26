package net.ghoula.vaire.internal

import scala.annotation.tailrec
import scala.collection.mutable

import net.ghoula.vaire.column.{Column, ColumnType}
import net.ghoula.vaire.types.RowIndex

/** Build-side key index for the keyed-join family.
  *
  * The keyed-join path reads every key through `Column.getValue`, which boxes each primitive (`Int`
  * → `Integer`, `Long` → `Long`, ...) and then hashes and compares the boxed `Any`. For the
  * primitive storage types in [[KeyIndex]]'s fast set, this index reads the column's raw array
  * directly (no `getValue`, no box) and stores each key as an unboxed `Long`. Other key types
  * (String, Decimal, structs, arrays, maps, ...) fall back to the object path.
  *
  * The fast path is used only when the indexed and probed key columns have the same `ColumnType`,
  * which the interpreter enforces at the keyed-join boundary before building the index. Matched
  * build-side rows are reported ascending via the probe callback, the same order the prior boxed
  * index produced.
  *
  * Null keys are not indexed and never match, matching Spark 4.2's `EqualTo` (`spark.sql.ansi` on
  * by default), whose comparison is null-intolerant: a null on either side yields no match. This is
  * a deliberate departure from the prior boxed index, which treated null as a single shared key and
  * matched null to null.
  *
  * The index is a warm-up-then-quench cache: it mutates internally while it is built and then
  * serves reads only. It is `private[vaire]`, created per join inside the interpreter, and never
  * exposed to caller code, so the mutation is confined to construction and the functional surface
  * ([[KeyIndex.probe]] and [[KeyIndex.build]]) stays pure. The mutable internals are the point of
  * the structure — flat primitive arrays keep key reads unboxed and allocation-free, which is why
  * no immutable hash structure is used here.
  */
private[vaire] sealed trait KeyIndex {

  /** Report each build-side row whose key matches the probe key at `row`; true when any matched.
    *
    * The probe column must have the same `ColumnType` as the column the index was built from, and
    * the interpreter guarantees this before calling. A null probe key never matches.
    */
  def probe(probeKeyCol: Column[?], row: Int)(report: Int => Unit): Boolean
}

private[vaire] object KeyIndex {

  /** Raw-storage types whose keys normalize losslessly to a primitive Long. */
  private[vaire] val FastTypes: Set[ColumnType] = Set(
    ColumnType.IntType,
    ColumnType.LongType,
    ColumnType.ShortType,
    ColumnType.ByteType,
    ColumnType.FloatType,
    ColumnType.DoubleType,
    ColumnType.BooleanType,
    ColumnType.DateType,
    ColumnType.TimestampType,
    ColumnType.TimestampNTZType
  )

  /** Largest row count the open-addressing table can hold with a free slot guaranteed.
    *
    * The table indexes slots with an `Int`, so the largest power-of-two capacity is 2^30. Keeping
    * `rowCount < 2^30` guarantees at least one free slot, so a miss or insert always terminates.
    * Larger inputs fall back to [[ObjectIndex]]; at that size the primitive table would not fit in
    * memory anyway, and the fallback avoids a probe that can never find a free slot.
    */
  private val MaxFastRows: Int = 1 << 30

  /** Build an index over the first `rowCount` keys of `keyCol`. */
  def build(keyCol: Column[?], rowCount: Int): KeyIndex =
    if (FastTypes(keyCol.columnType) && rowCount < MaxFastRows) new PrimitiveIndex(keyCol, rowCount)
    else new ObjectIndex(keyCol, rowCount)

  /** A key-to-head slot layout, filled during construction and read afterwards. */
  private sealed trait Slots {

    /** Prepend row `i` to `k`'s chain, returning the previous head (-1 when `k` was absent). */
    def prepend(k: Long, i: Int): Int

    /** The current head row for `k`, or -1 when absent. */
    def head(k: Long): Int
  }

  private final class EmptySlots extends Slots {
    def prepend(k: Long, i: Int): Int = -1
    def head(k: Long): Int = -1
  }

  /** Direct-address slots for dense keys: `heads(key - minKey)`.
    *
    * A probe is one array access with no hashing and no collisions, and the array is exactly the
    * key range, so it is compact for the common join-key shapes (ids, dates, enums, the uniform
    * sweep).
    */
  private final class DenseSlots(minKey: Long, size: Int) extends Slots {
    private val heads: Array[Int] = Array.fill(size)(-1)

    def prepend(k: Long, i: Int): Int = {
      val idx = k - minKey
      if (idx < 0 || idx >= size) -1
      else {
        val prev = heads(idx.toInt)
        heads(idx.toInt) = i
        prev
      }
    }

    def head(k: Long): Int = {
      val idx = k - minKey
      if (idx < 0 || idx >= size) -1 else heads(idx.toInt)
    }
  }

  /** Open-addressing slots for sparse keys, sized to 2× the row count. */
  private final class HashSlots(rowCount: Int) extends Slots {
    private val cap: Int = {
      val want = math.max(16L, math.min(rowCount.toLong * 2, (1 << 30).toLong)).toInt
      if ((want & (want - 1)) == 0) want else Integer.highestOneBit(want) << 1
    }
    private val mask: Int = cap - 1
    private val shift: Int = 64 - Integer.numberOfTrailingZeros(cap)
    private val slotKeys: Array[Long] = new Array[Long](cap)
    private val slotHeads: Array[Int] = Array.fill(cap)(-1)

    private def hash(k: Long): Int = {
      val h = (k ^ (k >>> 32)).toInt * -0x7ee3623b
      h ^ (h >>> 16)
    }

    private def slot(k: Long): Int = (hash(k) * 0x9e3779b1L >>> shift).toInt

    private def findSlot(k: Long): Int = {
      def go(s: Int): Int =
        if (slotHeads(s) >= 0 && slotKeys(s) != k) go((s + 1) & mask) else s
      go(slot(k))
    }

    def prepend(k: Long, i: Int): Int = {
      val s = findSlot(k)
      val prev = slotHeads(s)
      slotKeys(s) = k
      slotHeads(s) = i
      prev
    }

    def head(k: Long): Int = slotHeads(findSlot(k))
  }

  /** Unboxed-key index for the primitive storage types.
    *
    * The build reads each key raw (no `getValue`, no box) and stores it in one of two slot layouts,
    * chosen from the key range measured in a first pass:
    *
    *   - dense keys (`max - min + 1` within 4× the distinct count and under 2^26): [[DenseSlots]],
    *     a direct-address array indexed by `key - min`. This is the common join-key shape and it is
    *     where the uniform sweep lives.
    *   - sparse keys: [[HashSlots]], a hand-rolled open-addressing table.
    *
    * Both map a key to the head row of a flat shared `next` array, so no per-key object is
    * allocated and the chains lay out ascending (the build iterates rows from the end). Null rows
    * are skipped. The structure is immutable after construction; the mutable slot arrays are the
    * point.
    */
  private final class PrimitiveIndex(keyCol: Column[?], rowCount: Int) extends KeyIndex {
    private val next: Array[Int] = Array.fill(rowCount)(-1)

    private val bounds: (Long, Long, Int) = keyBounds(0, 0L, 0L, 0)

    /** Tail-recursive first pass: min key, max key, and non-null count. */
    @tailrec
    private def keyBounds(i: Int, lo: Long, hi: Long, n: Int): (Long, Long, Int) =
      if (i >= rowCount) (lo, hi, n)
      else if (keyCol.isNull(RowIndex(i))) keyBounds(i + 1, lo, hi, n)
      else {
        val k = rawKey(keyCol, i)
        if (n == 0) keyBounds(i + 1, k, k, n + 1)
        else keyBounds(i + 1, math.min(lo, k), math.max(hi, k), n + 1)
      }

    private val minKey: Long = bounds._1
    private val keyCount: Int = bounds._3

    private val slots: Slots = {
      val span = bounds._2 - bounds._1
      if (keyCount == 0) new EmptySlots
      else if (span >= 0 && span < (1L << 26) && span + 1 <= 4L * keyCount) new DenseSlots(minKey, (span + 1).toInt)
      else new HashSlots(rowCount)
    }

    (0 until rowCount).reverse.foreach { i =>
      if (!keyCol.isNull(RowIndex(i))) {
        next(i) = slots.prepend(rawKey(keyCol, i), i)
      }
    }

    override def probe(probeKeyCol: Column[?], row: Int)(report: Int => Unit): Boolean =
      if (probeKeyCol.isNull(RowIndex(row))) false
      else {
        val head = slots.head(rawKey(probeKeyCol, row))
        if (head < 0) false
        else if (next(head) < 0) {
          report(head)
          true
        } else {
          Iterator.iterate(head)(i => next(i)).takeWhile(_ >= 0).foreach(report)
          true
        }
      }
  }

  /** Boxed-key index for the non-primitive storage types. */
  private final class ObjectIndex(keyCol: Column[?], rowCount: Int) extends KeyIndex {
    private val buckets: mutable.HashMap[Any, Vector[Int]] = {
      val index = mutable.HashMap.empty[Any, Vector[Int]]
      (0 until rowCount).foreach { i =>
        if (!keyCol.isNull(RowIndex(i))) {
          val key = keyCol.getValue(i)
          index.updateWith(key) {
            case Some(existing) => Some(existing :+ i)
            case None => Some(Vector(i))
          }
        }
      }
      index
    }

    override def probe(probeKeyCol: Column[?], row: Int)(report: Int => Unit): Boolean =
      if (probeKeyCol.isNull(RowIndex(row))) false
      else {
        val rows = buckets.getOrElse(probeKeyCol.getValue(row), Vector.empty)
        val matched = rows.nonEmpty
        rows.foreach(report)
        matched
      }
  }

  /** Long-normalized key for a fast primitive column row.
    *
    * Only reachable through `build`/`probe` for columns whose `columnType` is in `FastTypes`, so
    * the fast cases below cover every reachable variant; a non-fast column here is a programming
    * error and fails hard rather than seeding a bogus key.
    */
  private def rawKey(col: Column[?], row: Int): Long =
    col match {
      case Column.IntColumn(data, _) => data(row).toLong
      case Column.LongColumn(data, _) => data(row)
      case Column.ShortColumn(data, _) => data(row).toLong
      case Column.ByteColumn(data, _) => data(row).toLong
      case Column.BooleanColumn(data, _) => if (data(row)) 1L else 0L
      case Column.DateColumn(data, _) => data(row).toLong
      case Column.TimestampColumn(data, _) => data(row)
      case Column.TimestampNTZColumn(data, _) => data(row)
      case Column.FloatColumn(data, _) => java.lang.Float.floatToIntBits(data(row)).toLong
      case Column.DoubleColumn(data, _) => java.lang.Double.doubleToLongBits(data(row))
      case other => throw new MatchError(other) // scalafix:ok DisableSyntax.throw
    }
}
