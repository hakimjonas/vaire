package net.ghoula.strongbow.spark

import org.apache.spark.sql.{Column => SparkColumn, DataFrame, Row => SparkRow}
import org.apache.spark.sql.functions.{array, get_json_object, json_tuple}
import org.apache.spark.sql.types.{StringType => SparkStringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.BitSet

import net.ghoula.strongbow.Schema
import net.ghoula.strongbow.column.{Column, ColumnType}
import net.ghoula.sarati.ast.json.JsonValue
import parsers.json.parseJson

/** json_tuple representation benchmark — the measurement behind the representation decision.
  *
  * Compares, per the handover:
  *   - In-memory: candidate A = per-row `Seq` of extracted values stored boxed in `AnyColumn`;
  *     candidate B = a `StructColumn` with one StringColumn per requested key (dynamic Schema).
  *     Timed separately: evaluation (parse + extraction) and row access (getValue over all rows).
  *     Retained heap measured around a fresh build (ZGC: indicative, not exact).
  *   - Spark: native `json_tuple(...)` (struct output, legal only as a lone top-level generator) vs
  *     `array(...)` of per-key `get_json_object`. The handover's `array(json_tuple(...).getField)`
  *     unwrapping is *illegal* — Spark rejects generators nested in any expression
  *     (UNSUPPORTED_GENERATOR.NESTED_IN_EXPRESSIONS) — so `get_json_object` is the only mapping
  *     that composes with Strongbow's single-projection `df.select(columns*)` model.
  *
  * Shapes: 1/3/8 requested keys over narrow 8-field objects, 3 keys over a 64-field wide object,
  * and 3 keys over an 8-level nested document. ~1% null rows.
  *
  * Diagnostic results (2026-08-30, JDK 25, ZGC, 4G heap, local[2]):
  * {{{
  *   In-memory (ms eval / ms access / MB retained, seq vs struct):
  *     narrow 1M rows,  1 key   9200 /   6.9 / 117  vs  9213 /  10.1 /  63
  *     narrow 1M rows,  3 keys  9467 /  31.3 / 254  vs  9377 /  16.8 / 210
  *     narrow 1M rows,  8 keys  9457 /  35.7 / 587  vs  9601 /  32.6 / 541
  *     wide-64 200K rows, 3 keys 12998 /  4.3 /  48  vs 13193 /   3.4 /  38
  *     deep-8  200K rows, 3 keys  1028 /  1.6 /   8  vs  1049 /   3.7 /  ~0 (GC noise)
  *   Evaluation is parse-dominated and identical; access is <0.4% of eval for both; struct
  *   retains 10-45% less but needs a dynamic-shape Schema (foreign to Strongbow's compile-time
  *   typed struct model).
  *   Spark (plan + exec, noop sink; ms):
  *     narrow 1M rows:   1 key  json_tuple  841 vs gjo-array  857 | 3 keys  984 vs 1700 |
  *                       8 keys 1140 vs 3949
  *     wide-64 200K rows, 3 keys  412 vs  684;  deep-8 200K rows, 3 keys  408 vs  852
  *   Native json_tuple parses once (1-3.5x faster at n keys) but cannot compose with Strongbow's
  *   select model; an alternative single-parse mapping via from_json would diverge from native
  *   json_tuple on compound values (null vs JSON text).
  * }}}
  *
  * Decision (recorded for the parity handover): representation A — `Expr[Row, Seq[String]]` with
  * values boxed in AnyColumn, Spark mapping `array(get_json_object(col, "$['key']"))`. Criterion
  * (b): the common case is a handful of keys where in-memory eval is parse-dominated and the two
  * representations are indistinguishable. Criterion (c): no new column variants, no dynamic-shape
  * Schema machinery, and the Spark mapping is byte-equal to native json_tuple on all value kinds
  * (verified against Spark 4.2: scalars, compounds -> JSON text, JSON null, absent, dotted keys,
  * invalid JSON, null rows).
  */
class JsonTupleBench extends AnyFlatSpec with Matchers with SparkTestBase {

  private val NarrowRows = 1_000_000
  private val WideRows = 200_000
  private val EvalWarmup = 2
  private val EvalMeasured = 5
  private val SparkWarmup = 1
  private val SparkMeasured = 3

  private val anchor = new java.util.concurrent.atomic.AtomicReference[Column[?]]()

  private def scalar(i: Long, j: Int): String =
    (i + j) % 7 match {
      case 0 => s"s${i}_$j"
      case 1 => ((i + j) % 1000).toString
      case 2 => "true"
      case 3 => s"${(i + j) % 100}.$j"
      case _ => s"v${i}_$j"
    }

  private def narrowDoc(nFields: Int, i: Long): String = {
    val sb = new java.lang.StringBuilder(48 * nFields)
    sb.append('{')
    (0 until nFields).foreach { j =>
      if (j > 0) sb.append(',')
      sb.append("\"k").append(j).append("\":\"").append(scalar(i, j)).append('"')
    }
    sb.append('}')
    sb.toString
  }

  private def wideDoc(i: Long): String = {
    val sb = new java.lang.StringBuilder(2048)
    sb.append('{')
    (0 until 64).foreach { j =>
      if (j > 0) sb.append(',')
      sb.append("\"k").append(j).append("\":\"").append(scalar(i, j)).append('"')
    }
    sb.append('}')
    sb.toString
  }

  private def deepDoc(i: Long): String = {
    val sb = new java.lang.StringBuilder(1024)
    sb.append("{\"k0\":\"")
      .append(scalar(i, 0))
      .append("\",\"k1\":\"")
      .append(scalar(i, 1))
      .append("\",\"k2\":\"")
      .append(scalar(i, 2))
      .append("\",\"blob\":")
    (7 to 1 by -1).foreach(l => sb.append("{\"l").append(l).append(":{"))
    sb.append("\"leaf\":[")
    (0 until 32).foreach { j =>
      if (j > 0) sb.append(',')
      sb.append('"').append(scalar(i, j)).append('"')
    }
    sb.append("]}")
    (1 until 7).foreach(_ => sb.append('}'))
    sb.append('}')
    sb.toString
  }

  private def docs(gen: Long => String, n: Int): Array[String | Null] = {
    val rng = new java.util.Random(42)
    Array.tabulate[String | Null](n)(i => if (rng.nextInt(100) == 0) null else gen(i.toLong))
  }

  private def requested(n: Int): Vector[String] = (0 until n).map(i => s"k$i").toVector

  private def medianMs(times: Seq[Long]): Double = {
    val sorted = times.sorted
    sorted(sorted.length / 2) / 1e6
  }

  private def probe(col: Column[?]): Long = {
    var sum = 0L
    (0 until col.length).foreach { i =>
      col.getValue(i) match {
        case s: Seq[?] => sum += s.size
        case _ => ()
      }
    }
    sum
  }

  private def timedEval(build: Column[?] => Long, make: () => Column[?]): Double = {
    val times = (0 until EvalWarmup + EvalMeasured).map { _ =>
      val t0 = System.nanoTime()
      val p = probe(make())
      require(p >= 0)
      System.nanoTime() - t0
    }.drop(EvalWarmup)
    medianMs(times)
  }

  private def timedAccess(col: Column[?]): Double = {
    val times = (0 until EvalWarmup + EvalMeasured).map { _ =>
      val t0 = System.nanoTime()
      val p = probe(col)
      require(p >= 0)
      System.nanoTime() - t0
    }.drop(EvalWarmup)
    medianMs(times)
  }

  private def gc(): Unit = {
    (1 to 3).foreach { _ =>
      System.gc()
      Thread.sleep(50)
    }
  }

  private def usedHeap: Long = Runtime.getRuntime.totalMemory - Runtime.getRuntime.freeMemory

  /** Heap growth around a fresh build; the result is anchored so ZGC cannot reclaim it. */
  private def retainedBytes(make: () => Column[?]): Long = {
    gc()
    val before = usedHeap
    anchor.set(make())
    gc()
    val after = usedHeap
    anchor.set(null) // scalafix:ok DisableSyntax.null
    after - before
  }

  private def extractScalar(v: JsonValue, key: String): Any | Null = v match {
    case JsonValue.Object(fields) =>
      fields.get(key) match {
        case Some(JsonValue.Str(s)) => s
        case Some(JsonValue.Bool(b)) => b.toString
        case Some(JsonValue.Number(n)) =>
          if (n == n.toLong.toDouble) n.toLong.toString else n.toString
        case _ => null // scalafix:ok DisableSyntax.null
      }
    case _ => null // scalafix:ok DisableSyntax.null
  }

  /** Candidate A: one boxed Seq per row in AnyColumn (Seq[String] with null elements). */
  private def evalSeq(jsonData: Array[String | Null], keys: Vector[String]): Column[?] = {
    val out = Array.tabulate[Any | Null](jsonData.length) { i =>
      jsonData(i) match {
        case s: String =>
          parseJson(s) match {
            case parser.core.Result.Success(v: JsonValue, _) => keys.map(k => extractScalar(v, k))
            case _ => keys.map(_ => null: Any | Null) // scalafix:ok DisableSyntax.null
          }
        case null => null // scalafix:ok DisableSyntax.null
      }
    }
    val nulls =
      jsonData.indices.toVector.filter(jsonData(_) == null) // scalafix:ok DisableSyntax.null
    Column.any(out, BitSet.empty ++ nulls)
  }

  /** Candidate B: one StringColumn per key under a dynamic-shape StructColumn. */
  private def evalStruct(jsonData: Array[String | Null], keys: Vector[String]): Column[?] = {
    val n = jsonData.length
    val datas = Vector.fill(keys.size)(new Array[String | Null](n))
    val fieldNulls = Vector.fill(keys.size)(scala.collection.mutable.BitSet.empty)
    val rowNulls = scala.collection.mutable.BitSet.empty
    (0 until n).foreach { i =>
      jsonData(i) match {
        case s: String =>
          parseJson(s) match {
            case parser.core.Result.Success(v: JsonValue, _) =>
              keys.indices.foreach { j =>
                extractScalar(v, keys(j)) match {
                  case str: String => datas(j)(i) = str
                  case _ => fieldNulls(j) += i
                }
              }
            case _ => keys.indices.foreach(j => fieldNulls(j) += i)
          }
        case null =>
          rowNulls += i
          keys.indices.foreach(j => fieldNulls(j) += i)
      }
    }
    val dynSchema: Schema[Vector[Any | Null]] = new Schema[Vector[Any | Null]] {
      def columnCount: Int = keys.size
      def columnNames: Vector[String] = keys
      def columnTypes: Vector[ColumnType] = Vector.fill(keys.size)(ColumnType.StringType)
      def encode(value: Vector[Any | Null]): Vector[Any] = value
      def decode(values: Vector[Any]): Either[net.ghoula.strongbow.errors.DecodeError, Vector[Any | Null]] =
        Right(values)
    }
    Column.struct(
      keys.indices.map(j => Column.string(datas(j), BitSet.empty ++ fieldNulls(j))).toVector,
      dynSchema,
      BitSet.empty ++ rowNulls
    )
  }

  private def benchInMemory(label: String, docArr: Array[String | Null], keyCounts: Vector[Int]): Unit = {
    keyCounts.foreach { n =>
      val keys = requested(n)
      val evalSeqMs = timedEval(probe, () => evalSeq(docArr, keys))
      val evalStructMs = timedEval(probe, () => evalStruct(docArr, keys))
      val seqCol = evalSeq(docArr, keys)
      val structCol = evalStruct(docArr, keys)
      val accessSeqMs = timedAccess(seqCol)
      val accessStructMs = timedAccess(structCol)
      val seqHeap = retainedBytes(() => evalSeq(docArr, keys))
      val structHeap = retainedBytes(() => evalStruct(docArr, keys))
      println(
        f"$label%-22s ($n keys) eval: seq=$evalSeqMs%9.1f struct=$evalStructMs%9.1f ms | " +
          f"access: seq=$accessSeqMs%9.1f struct=$accessStructMs%9.1f ms | " +
          f"retained: seq=${seqHeap / 1e6}%9.1f struct=${structHeap / 1e6}%9.1f MB"
      )
    }
  }

  private def toDF(docArr: Array[String | Null]): DataFrame = {
    val rows = java.util.Arrays.asList(docArr.map(d => SparkRow(d))*)
    val schema = StructType(Seq(StructField("j", SparkStringType, nullable = true)))
    spark.createDataFrame(rows, schema).cache()
  }

  private def planMs(df: DataFrame, build: SparkColumn): Double = {
    val times = (0 until SparkWarmup + SparkMeasured).map { _ =>
      val t0 = System.nanoTime()
      df.select(build).write.format("noop").mode("overwrite").save()
      System.nanoTime() - t0
    }.drop(SparkWarmup)
    medianMs(times)
  }

  private def benchSpark(label: String, docArr: Array[String | Null], keyCounts: Vector[Int]): Unit = {
    val df = toDF(docArr)
    df.count()
    keyCounts.foreach { n =>
      val keys = requested(n)
      val jsonCol = df.col("j")
      val nativeStructMs = planMs(df, json_tuple(jsonCol, keys*))
      val gjoArrayMs = planMs(df, array(keys.map(k => get_json_object(jsonCol, s"$$.$k"))*))
      println(
        f"$label%-22s ($n keys) json_tuple struct: $nativeStructMs%9.1f | " +
          f"get_json_object x$n -> array: $gjoArrayMs%9.1f ms"
      )
    }
    df.unpersist()
  }

  "json_tuple representation" should "measure in-memory seq vs struct candidates" taggedAs Benchmark in {
    benchInMemory(s"narrow (${NarrowRows} rows)", docs(i => narrowDoc(8, i), NarrowRows), Vector(1, 3, 8))
    benchInMemory(s"wide-64 (${WideRows} rows)", docs(i => wideDoc(i), WideRows), Vector(3))
    benchInMemory(s"deep-8 (${WideRows} rows)", docs(i => deepDoc(i), WideRows), Vector(3))
  }

  it should "measure spark json_tuple vs get_json_object plans" taggedAs Benchmark in {
    benchSpark(s"narrow (${NarrowRows} rows)", docs(i => narrowDoc(8, i), NarrowRows), Vector(1, 3, 8))
    benchSpark(s"wide-64 (${WideRows} rows)", docs(i => wideDoc(i), WideRows), Vector(3))
    benchSpark(s"deep-8 (${WideRows} rows)", docs(i => deepDoc(i), WideRows), Vector(3))
  }
}
