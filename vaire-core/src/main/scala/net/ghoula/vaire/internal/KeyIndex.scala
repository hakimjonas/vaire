package net.ghoula.vaire.internal

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import net.ghoula.vaire.column.{Column, ColumnType}
import net.ghoula.vaire.types.RowIndex

/** Build-side key index for the keyed-join family.
  *
  * The keyed-join path reads every key through `Column.getValue`, which boxes each primitive (`Int`
  * → `Integer`, `Long` → `Long`, ...) and then hashes and compares the boxed `Any`. For the
  * primitive storage types in [[KeyIndex]]'s fast set, this index reads the column's raw array
  * directly (no `getValue`, no box) and stores each key as an unboxed `Long`, with null rows in a
  * dedicated bucket. Other key types (String, Decimal, structs, arrays, maps, ...) fall back to the
  * object path, which stays behavior-identical with the prior `HashMap[Any, Vector[Int]]` index.
  *
  * The fast path is used only when the indexed and probed key columns have the same `ColumnType`,
  * so Long-normalized equality reproduces the boxed-`Any` equality exactly, including null keys
  * matching null keys. Matched build-side rows are reported ascending via the probe callback, the
  * same order the boxed index produced.
  */
private[vaire] sealed trait KeyIndex {

  /** Report each build-side row whose key matches the probe key at `row`; true when any matched.
    *
    * Probes with a column type different from the indexed key column match nothing, reproducing the
    * boxed behavior where `Integer` and `Long` keys never equal each other.
    */
  def probe(probeKeyCol: Column[?], row: Int)(report: Int => Unit): Boolean
}

private[vaire] object KeyIndex {

  /** Raw-storage types whose keys normalize losslessly to a primitive Long. */
  private val FastTypes: Set[ColumnType] = Set(
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

  /** Build an index over the first `rowCount` keys of `keyCol`. */
  def build(keyCol: Column[?], rowCount: Int): KeyIndex =
    if (FastTypes(keyCol.columnType)) new PrimitiveIndex(keyCol, rowCount)
    else new ObjectIndex(keyCol, rowCount)

  /** Unboxed-key hash index for the primitive storage types.
    *
    * A hand-rolled open-addressing table maps each key, unboxed, to the head row of a flat shared
    * `next` array, so the build allocates no per-key object. The table stores the head row in
    * [[slotHeads]] with -1 as the empty marker, so empty slots read as no head; the build iterates
    * rows from the end so each key's chain lays out ascending, the same order the boxed index
    * produced.
    */
  private final class PrimitiveIndex(keyCol: Column[?], rowCount: Int) extends KeyIndex {
    private val indexType: ColumnType = keyCol.columnType
    private val nullBucket: ArrayBuffer[Int] = ArrayBuffer.empty[Int]
    private val next: Array[Int] = Array.fill(rowCount)(-1)

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

    (0 until rowCount).foreach { i =>
      if (keyCol.isNull(RowIndex(i))) nullBucket += i
    }
    (0 until rowCount).reverse.foreach { i =>
      if (!keyCol.isNull(RowIndex(i))) {
        val k = rawKey(keyCol, i)
        val s = findSlot(k)
        next(i) = if (slotHeads(s) >= 0) slotHeads(s) else -1
        slotKeys(s) = k
        slotHeads(s) = i
      }
    }

    override def probe(probeKeyCol: Column[?], row: Int)(report: Int => Unit): Boolean =
      if (probeKeyCol.isNull(RowIndex(row))) {
        val matched = nullBucket.nonEmpty
        nullBucket.foreach(report)
        matched
      } else if (probeKeyCol.columnType != indexType) false
      else {
        val head = {
          val s = findSlot(rawKey(probeKeyCol, row))
          if (slotHeads(s) >= 0) slotHeads(s) else -1
        }
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

  /** Boxed-key index preserving the prior `HashMap[Any, Vector[Int]]` behavior. */
  private final class ObjectIndex(keyCol: Column[?], rowCount: Int) extends KeyIndex {
    private val buckets: mutable.HashMap[Any, Vector[Int]] = {
      val index = mutable.HashMap.empty[Any, Vector[Int]]
      (0 until rowCount).foreach { i =>
        val key = keyCol.getValue(i)
        index.updateWith(key) {
          case Some(existing) => Some(existing :+ i)
          case None => Some(Vector(i))
        }
      }
      index
    }

    override def probe(probeKeyCol: Column[?], row: Int)(report: Int => Unit): Boolean = {
      val rows = buckets.getOrElse(probeKeyCol.getValue(row), collection.Seq.empty)
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
