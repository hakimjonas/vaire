package net.ghoula.strongbow.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

import net.ghoula.strongbow.{
  Column => SBColumn,
  Dataset,
  Expr,
  Grouped,
  Interpreter,
  MaterializedDataset,
  Schema
}
import net.ghoula.strongbow.errors.ExecutionError

/** Spark-based interpreter for Strongbow Dataset plans.
  *
  * Executes the same Dataset[T] AST on Apache Spark. Operations expressible as Expr translate to
  * native Spark Column expressions for Catalyst optimization. Function-based operations (Map,
  * FlatMap, joins with opaque predicates) collect to driver, apply the function, and recreate the
  * DataFrame.
  */
class SparkInterpreter(spark: SparkSession) extends Interpreter {

  /** Internal plan representation: a Spark DataFrame paired with the Strongbow Schema for decoding. */
  private case class SparkPlan[T](df: DataFrame, schema: Schema[T])

  override def execute[T](dataset: Dataset[T]): Either[ExecutionError, MaterializedDataset[T]] = {
    try {
      val plan = buildPlan(dataset)
      val rows = plan.df.collect()
      val values = rows.iterator.map(r => RowConverter.fromRowUnsafe(r, plan.schema)).toVector
      MaterializedDataset.fromVector(values)(using plan.schema)
    } catch {
      case e: Exception =>
        Left(ExecutionError.InvalidValue(s"Spark execution failed: ${e.getMessage}"))
    }
  }

  private def buildPlan[T](dataset: Dataset[T]): SparkPlan[T] = {
    (dataset: @unchecked) match {

      case root: Dataset.Root[T] =>
        val df = DataFrameBuilder.fromColumns(spark, root.columns, root.schema)
        SparkPlan(df, root.schema)

      case filt: Dataset.Filter[T] =>
        val parent = buildPlan(filt.parent)
        ExprToColumn.convert(filt.predicate) match {
          case Right(sparkCol) =>
            SparkPlan(parent.df.filter(sparkCol), parent.schema)
          case Left(_) =>
            applyFilterViaMap(parent, filt.predicate)
        }

      case m: Dataset.Map[a, T] =>
        val parent = buildPlan(m.parent)
        applyMapFunction[a, T](parent, m.func, m.schema)

      case fm: Dataset.FlatMap[a, T] =>
        val parent = buildPlan(fm.parent)
        applyFlatMapFunction[a, T](parent, fm.func, fm.schema)

      case sel: Dataset.Select[a, T] =>
        val parent = buildPlan(sel.parent)
        applyMapFunction[a, T](parent, sel.projection, sel.schema)

      case selectExprs: Dataset.SelectExprs[T] =>
        val parent = buildPlan(selectExprs.parent)
        val columns = selectExprs.exprs.map { case (name, expr, _) =>
          ExprToColumn.convert(expr) match {
            case Right(sparkCol) => sparkCol.as(name)
            case Left(err) => throw new RuntimeException(s"Expr conversion failed: $err") // scalafix:ok DisableSyntax.throw
          }
        }
        val newSchema = new Schema[T] {
          def columnCount: Int = selectExprs.exprs.length
          def columnNames: Vector[String] = selectExprs.exprs.map(_._1)
          def columnTypes: Vector[net.ghoula.strongbow.ColumnType] = selectExprs.exprs.map(_._3)
          def encode(value: T): Vector[Any] = parent.schema.encode(value)
          def decode(values: Vector[Any]): Either[net.ghoula.strongbow.errors.DecodeError, T] =
            parent.schema.decode(values)
        }
        SparkPlan(parent.df.select(columns*), newSchema)

      case dist: Dataset.Distinct[T] =>
        val parent = buildPlan(dist.parent)
        SparkPlan(parent.df.distinct(), parent.schema)

      case lim: Dataset.Limit[T] =>
        val parent = buildPlan(lim.parent)
        SparkPlan(parent.df.limit(lim.n), parent.schema)

      case un: Dataset.Union[T] =>
        val left = buildPlan(un.left)
        val right = buildPlan(un.right)
        SparkPlan(left.df.union(right.df), left.schema)

      case inter: Dataset.Intersect[T] =>
        val left = buildPlan(inter.left)
        val right = buildPlan(inter.right)
        SparkPlan(left.df.intersect(right.df), left.schema)

      case exc: Dataset.Except[T] =>
        val left = buildPlan(exc.left)
        val right = buildPlan(exc.right)
        SparkPlan(left.df.except(right.df), left.schema)

      case srt: Dataset.Sort[T] =>
        val parent = buildPlan(srt.parent)
        applySortViaCollect(parent, srt.ordering)

      case srtBy: Dataset.SortBy[T, _] =>
        val parent = buildPlan(srtBy.parent)
        applySortByViaCollect(parent, srtBy.key, srtBy.ordering)

      case samp: Dataset.Sample[T] =>
        val parent = buildPlan(samp.parent)
        SparkPlan(
          parent.df.sample(samp.withReplacement, samp.fraction, samp.seed),
          parent.schema
        )

      case zip: Dataset.ZipWithIndex[a] =>
        val parent = buildPlan(zip.parent)
        applyZipWithIndex[a, T](parent)

      case jn: Dataset.InnerJoin[a, b] =>
        val left = buildPlan(jn.left)
        val right = buildPlan(jn.right)
        applyInnerJoin[a, b, T](left, right, jn.condition)

      case jn: Dataset.LeftJoin[a, b] =>
        val left = buildPlan(jn.left)
        val right = buildPlan(jn.right)
        applyLeftJoin[a, b, T](left, right, jn.condition)

      case jn: Dataset.RightJoin[a, b] =>
        val left = buildPlan(jn.left)
        val right = buildPlan(jn.right)
        applyRightJoin[a, b, T](left, right, jn.condition)

      case jn: Dataset.FullJoin[a, b] =>
        val left = buildPlan(jn.left)
        val right = buildPlan(jn.right)
        applyFullJoin[a, b, T](left, right, jn.condition)

      case jn: Dataset.LeftAntiJoin[a, b] =>
        val left = buildPlan(jn.left)
        val right = buildPlan(jn.right)
        applyLeftAntiJoin[a, b, T](left, right, jn.condition)

      case grp: Dataset.GroupedToPairs[k, v] =>
        applyGroupedToPairs[k, v, T](grp.grouped, grp.schemaK, grp.schemaV)

      case keys: Dataset.GroupedKeys[k, v] =>
        applyGroupedKeys[k, v, T](keys.grouped, keys.schemaK)

      case values: Dataset.GroupedValues[k, v] =>
        applyGroupedValues[k, v, T](values.grouped, values.schemaV)
    }
  }

  // --- Helper to collect rows from a DataFrame ---

  private def collectValues[T](plan: SparkPlan[T]): Vector[T] = {
    plan.df.collect().iterator.map(r => RowConverter.fromRowUnsafe(r, plan.schema)).toVector
  }

  // --- Function-based operations using collect -> transform -> recreate ---

  private def applyFilterViaMap[T](parent: SparkPlan[T], predicate: Expr[T, Boolean]): SparkPlan[T] = {
    val values = collectValues(parent)
    val filtered = values.filter { v =>
      import net.ghoula.strongbow.ExprInterpreter
      import net.ghoula.strongbow.types.RowIndex
      val encoded = parent.schema.encode(v)
      val cols = encoded.zipWithIndex.map { case (value, idx) =>
        SBColumn.fromValues(Vector(value), parent.schema.columnTypes(idx))
      }.collect { case Right(c) => c }.toVector
      ExprInterpreter.eval(predicate, cols, RowIndex(0)) == Right(true)
    }
    SparkPlan(createDataFrame(filtered, parent.schema), parent.schema)
  }

  private def applyMapFunction[A, B](parent: SparkPlan[A], func: A => B, schema: Schema[B]): SparkPlan[B] = {
    val values = collectValues(parent)
    val mapped = values.map(func)
    SparkPlan(createDataFrame(mapped, schema), schema)
  }

  private def applyFlatMapFunction[A, B](parent: SparkPlan[A], func: A => Iterable[B], schema: Schema[B]): SparkPlan[B] = {
    val values = collectValues(parent)
    val mapped = values.flatMap(func)
    SparkPlan(createDataFrame(mapped, schema), schema)
  }

  private def applySortViaCollect[T](parent: SparkPlan[T], ord: Ordering[T]): SparkPlan[T] = {
    val values = collectValues(parent)
    val sorted = values.sorted(using ord)
    SparkPlan(createDataFrame(sorted, parent.schema), parent.schema)
  }

  private def applySortByViaCollect[T, K](parent: SparkPlan[T], key: T => K, ord: Ordering[K]): SparkPlan[T] = {
    val values = collectValues(parent)
    val sorted = values.sortBy(key)(using ord)
    SparkPlan(createDataFrame(sorted, parent.schema), parent.schema)
  }

  // --- Joins (collect both sides, cross-product with condition) ---

  private def applyInnerJoin[A, B, T](
    left: SparkPlan[A],
    right: SparkPlan[B],
    condition: (A, B) => Boolean
  ): SparkPlan[T] = {
    val leftValues = collectValues(left)
    val rightValues = collectValues(right)

    val resultRows = for {
      l <- leftValues
      r <- rightValues
      if condition(l, r)
    } yield (l, r)

    val tupleSchema = Schema.tuple2Schema[A, B](using left.schema, right.schema)
    val df = createDataFrame(resultRows, tupleSchema)
    SparkPlan(df, tupleSchema.asInstanceOf[Schema[T]]) // scalafix:ok DisableSyntax.asInstanceOf
  }

  private def applyLeftJoin[A, B, T](
    left: SparkPlan[A],
    right: SparkPlan[B],
    condition: (A, B) => Boolean
  ): SparkPlan[T] = {
    val leftValues = collectValues(left)
    val rightValues = collectValues(right)

    val resultRows = leftValues.flatMap { l =>
      val matches = rightValues.filter(r => condition(l, r))
      if (matches.isEmpty) Vector((l, None))
      else matches.map(r => (l, Some(r)))
    }

    val rightOptionSchema = Schema.optionSchema[B](using right.schema)
    val resultSchema = Schema.tuple2Schema[A, Option[B]](using left.schema, rightOptionSchema)
    val df = createDataFrame(resultRows, resultSchema)
    SparkPlan(df, resultSchema.asInstanceOf[Schema[T]]) // scalafix:ok DisableSyntax.asInstanceOf
  }

  private def applyRightJoin[A, B, T](
    left: SparkPlan[A],
    right: SparkPlan[B],
    condition: (A, B) => Boolean
  ): SparkPlan[T] = {
    val leftValues = collectValues(left)
    val rightValues = collectValues(right)

    val resultRows = rightValues.flatMap { r =>
      val matches = leftValues.filter(l => condition(l, r))
      if (matches.isEmpty) Vector((None, r))
      else matches.map(l => (Some(l), r))
    }

    val leftOptionSchema = Schema.optionSchema[A](using left.schema)
    val resultSchema = Schema.tuple2Schema[Option[A], B](using leftOptionSchema, right.schema)
    val df = createDataFrame(resultRows, resultSchema)
    SparkPlan(df, resultSchema.asInstanceOf[Schema[T]]) // scalafix:ok DisableSyntax.asInstanceOf
  }

  private def applyFullJoin[A, B, T](
    left: SparkPlan[A],
    right: SparkPlan[B],
    condition: (A, B) => Boolean
  ): SparkPlan[T] = {
    val leftValues = collectValues(left)
    val rightValues = collectValues(right)

    val leftMatches = scala.collection.mutable.HashSet.empty[A]
    val rightMatches = scala.collection.mutable.HashSet.empty[B]

    val innerResults = for {
      l <- leftValues
      r <- rightValues
      if condition(l, r)
    } yield {
      leftMatches.add(l)
      rightMatches.add(r)
      (Some(l), Some(r))
    }

    val unmatchedLeft = leftValues.filterNot(leftMatches.contains).map(l => (Some(l), None))
    val unmatchedRight = rightValues.filterNot(rightMatches.contains).map(r => (None, Some(r)))
    val resultRows = innerResults ++ unmatchedLeft ++ unmatchedRight

    val leftOptionSchema = Schema.optionSchema[A](using left.schema)
    val rightOptionSchema = Schema.optionSchema[B](using right.schema)
    val resultSchema = Schema.tuple2Schema[Option[A], Option[B]](using leftOptionSchema, rightOptionSchema)
    val df = createDataFrame(resultRows, resultSchema)
    SparkPlan(df, resultSchema.asInstanceOf[Schema[T]]) // scalafix:ok DisableSyntax.asInstanceOf
  }

  private def applyLeftAntiJoin[A, B, T](
    left: SparkPlan[A],
    right: SparkPlan[B],
    condition: (A, B) => Boolean
  ): SparkPlan[T] = {
    val leftValues = collectValues(left)
    val rightValues = collectValues(right)

    val resultRows = leftValues.filter { l =>
      !rightValues.exists(r => condition(l, r))
    }

    val df = createDataFrame(resultRows, left.schema)
    SparkPlan(df, left.schema.asInstanceOf[Schema[T]]) // scalafix:ok DisableSyntax.asInstanceOf
  }

  // --- ZipWithIndex ---

  private def applyZipWithIndex[A, T](parent: SparkPlan[A]): SparkPlan[T] = {
    val values = collectValues(parent)
    val indexed = values.zipWithIndex.map { case (v, i) => (v, i.toLong) }

    val tupleSchema = Schema.tuple2Schema[A, Long](using parent.schema, Schema.longSchema)
    val df = createDataFrame(indexed, tupleSchema)
    SparkPlan(df, tupleSchema.asInstanceOf[Schema[T]]) // scalafix:ok DisableSyntax.asInstanceOf
  }

  // --- Grouped operations: delegate to SparkGroupedInterpreter ---

  private def applyGroupedToPairs[K, V, T](
    grouped: Grouped[K, V],
    schemaK: Schema[K],
    schemaV: Schema[V]
  ): SparkPlan[T] = {
    val interpreter = SparkGroupedInterpreter(this)
    val pairs = interpreter.execute(grouped)

    val tupleSchema = Schema.tuple2Schema[K, V](using schemaK, schemaV)
    val df = createDataFrame(pairs, tupleSchema)
    SparkPlan(df, tupleSchema.asInstanceOf[Schema[T]]) // scalafix:ok DisableSyntax.asInstanceOf
  }

  private def applyGroupedKeys[K, V, T](
    grouped: Grouped[K, V],
    schemaK: Schema[K]
  ): SparkPlan[T] = {
    val interpreter = SparkGroupedInterpreter(this)
    val pairs = interpreter.execute(grouped)
    val keys = pairs.map(_._1)

    val df = createDataFrame(keys, schemaK)
    SparkPlan(df, schemaK.asInstanceOf[Schema[T]]) // scalafix:ok DisableSyntax.asInstanceOf
  }

  private def applyGroupedValues[K, V, T](
    grouped: Grouped[K, V],
    schemaV: Schema[V]
  ): SparkPlan[T] = {
    val interpreter = SparkGroupedInterpreter(this)
    val pairs = interpreter.execute(grouped)
    val values = pairs.map(_._2)

    val df = createDataFrame(values, schemaV)
    SparkPlan(df, schemaV.asInstanceOf[Schema[T]]) // scalafix:ok DisableSyntax.asInstanceOf
  }

  // --- Helper: create DataFrame from values ---

  private[spark] def createDataFrame[T](values: Vector[T], schema: Schema[T]): DataFrame = {
    val structType = SchemaConverter.toStructType(schema)
    val rows = values.map(v => RowConverter.toRow(v, schema))
    val javaRows = java.util.Arrays.asList(rows*)
    spark.createDataFrame(javaRows, structType)
  }
}
