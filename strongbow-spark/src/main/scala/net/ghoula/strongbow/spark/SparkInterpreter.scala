package net.ghoula.strongbow.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

import net.ghoula.strongbow.{Column => SBColumn, Dataset, Expr, Grouped, Interpreter, MaterializedDataset, Schema}
import net.ghoula.strongbow.errors.ExecutionError
import net.ghoula.strongbow.specs.{AggSpec, KeySpec, SortSpec, WindowExprSpec}

/** Spark-based interpreter for Strongbow Dataset plans.
  *
  * Executes the same Dataset[T] AST on Apache Spark. Operations expressible as Expr translate to
  * native Spark Column expressions for Catalyst optimization. Function-based operations (Map,
  * FlatMap, joins with opaque predicates) collect to driver, apply the function, and recreate the
  * DataFrame.
  */
class SparkInterpreter(spark: SparkSession) extends Interpreter {

  private case class SparkPlan[T](df: DataFrame, schema: Schema[T])

  override def execute[T](dataset: Dataset[T]): Either[ExecutionError, MaterializedDataset[T]] = {
    try {
      val plan = buildPlan(dataset)
      val rows = plan.df.collect()
      RowConverter.toMaterialized(rows, plan.schema)
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
          case Right((sparkCol, _)) =>
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

      case selectExprs: Dataset.SelectExprs[_, T] =>
        val parent = buildPlan(selectExprs.parent)
        val columns = selectExprs.exprs.map { case (name, expr, _) =>
          ExprToColumn.convert(expr) match {
            case Right((sparkCol, _)) => sparkCol.as(name)
            case Left(err) =>
              throw new RuntimeException(s"Expr conversion failed: $err") // scalafix:ok DisableSyntax.throw
          }
        }
        SparkPlan(parent.df.select(columns*), selectExprs.schema)

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

      case srtExpr: Dataset.SortByExpr[T, _] =>
        val parent = buildPlan(srtExpr.parent)
        ExprToColumn.convert(srtExpr.keyExpr) match {
          case Right((sparkCol, _)) =>
            SparkPlan(parent.df.sort(sparkCol), parent.schema)
          case Left(_) =>
            applySortByExprViaCollect(parent, srtExpr.keyExpr, srtExpr.keyType, srtExpr.ordering)
        }

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

      case jn: Dataset.InnerJoinOn[a, b, _] =>
        applyJoinOnExpr[a, b, T](jn.left, jn.right, jn.leftKey, jn.rightKey, "inner")

      case jn: Dataset.LeftJoinOn[a, b, _] =>
        applyJoinOnExpr[a, b, T](jn.left, jn.right, jn.leftKey, jn.rightKey, "left")

      case jn: Dataset.RightJoinOn[a, b, _] =>
        applyJoinOnExpr[a, b, T](jn.left, jn.right, jn.leftKey, jn.rightKey, "right")

      case jn: Dataset.FullJoinOn[a, b, _] =>
        applyJoinOnExpr[a, b, T](jn.left, jn.right, jn.leftKey, jn.rightKey, "full")

      case jn: Dataset.LeftAntiJoinOn[a, b, _] =>
        applyJoinOnExpr[a, b, T](jn.left, jn.right, jn.leftKey, jn.rightKey, "left_anti")

      case grp: Dataset.GroupedToPairs[k, v] =>
        applyGroupedToPairs[k, v, T](grp.grouped, grp.schemaK, grp.schemaV)

      case keys: Dataset.GroupedKeys[k, v] =>
        applyGroupedKeys[k, v, T](keys.grouped, keys.schemaK)

      case values: Dataset.GroupedValues[k, v] =>
        applyGroupedValues[k, v, T](values.grouped, values.schemaV)

      case gba: Dataset.GroupByAgg[_, T] =>
        val parent = buildPlan(gba.parent)
        applyGroupByAgg(parent, gba.keySpecs, gba.aggSpecs, gba.schema)

      case srtExprs: Dataset.SortByExprs[T] =>
        val parent = buildPlan(srtExprs.parent)
        applySortByExprs(parent, srtExprs.sortKeys)

      case jn: Dataset.LeftSemiJoinOn[a, b, _] =>
        applyJoinOnExpr[a, b, T](jn.left, jn.right, jn.leftKey, jn.rightKey, "left_semi")

      case ww: Dataset.WithWindow[_, T] =>
        val parent = buildPlan(ww.parent)
        applyWithWindow(parent, ww.windowExprs, ww.windowSpec, ww.schema)
    }
  }

  private def collectValues[T](plan: SparkPlan[T]): Vector[T] = {
    plan.df.collect().iterator.map(r => RowConverter.fromRowUnsafe(r, plan.schema)).toVector
  }

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

  private def applyFlatMapFunction[A, B](
    parent: SparkPlan[A],
    func: A => Iterable[B],
    schema: Schema[B]
  ): SparkPlan[B] = {
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

  private def applySortByExprViaCollect[T, K](
    parent: SparkPlan[T],
    keyExpr: Expr[T, K],
    keyType: net.ghoula.strongbow.ColumnType,
    ord: Ordering[K]
  ): SparkPlan[T] = {
    val values = collectValues(parent)
    import net.ghoula.strongbow.MaterializedDataset
    MaterializedDataset.fromVector(values)(using parent.schema) match {
      case Right(materialized) =>
        net.ghoula.strongbow.ExprInterpreter.evalColumn(keyExpr, materialized.columns, keyType) match {
          case Right(keyCol) =>
            val rowCount = materialized.rowCount
            val decoded = materialized.toVectorUnsafe
            val keys =
              (0 until rowCount).map(i => keyCol.getValue(i).asInstanceOf[K]) // scalafix:ok DisableSyntax.asInstanceOf
            val sorted = decoded.indices.sortWith((a, b) => ord.lt(keys(a), keys(b))).map(decoded)
            SparkPlan(createDataFrame(sorted.toVector, parent.schema), parent.schema)
          case Left(_) =>
            parent
        }
      case Left(_) => parent
    }
  }

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

  private def applyJoinOnExpr[A, B, T](
    leftDs: Dataset[A],
    rightDs: Dataset[B],
    leftKey: Expr[A, ?],
    rightKey: Expr[B, ?],
    joinType: String
  ): SparkPlan[T] = {
    val left = buildPlan(leftDs)
    val right = buildPlan(rightDs)

    val leftColName = getColName(leftKey)
    val rightColName = getColName(rightKey)

    val leftAlias = left.df.alias("_l")
    val rightAlias = right.df.alias("_r")

    import org.apache.spark.sql.functions.col
    val joinCondition = col(s"_l.$leftColName") === col(s"_r.$rightColName")
    val joinedDf = leftAlias.join(rightAlias, joinCondition, joinType)

    val leftColNames = left.df.columns.map(c => col(s"_l.$c"))
    val rightColNames = right.df.columns.map(c => col(s"_r.$c"))

    val selectedDf = joinType match {
      case "left_anti" | "left_semi" =>
        joinedDf.select(leftColNames*)
      case _ =>
        joinedDf.select((leftColNames ++ rightColNames)*)
    }

    val resultSchema = buildJoinSchema[A, B, T](left.schema, right.schema, joinType)
    SparkPlan(selectedDf, resultSchema)
  }

  private def getColName[Row, A](expr: Expr[Row, A]): String = expr match {
    case cell: Expr.Cell[_, _] => cell.name
    case named: Expr.Named[_, _] => named.name
    case _ => "_expr"
  }

  private def buildJoinSchema[A, B, T](leftSchema: Schema[A], rightSchema: Schema[B], joinType: String): Schema[T] = {
    val schema = joinType match {
      case "inner" =>
        Schema.tuple2Schema[A, B](using leftSchema, rightSchema)
      case "left" =>
        val rightOpt = Schema.optionSchema[B](using rightSchema)
        Schema.tuple2Schema[A, Option[B]](using leftSchema, rightOpt)
      case "right" =>
        val leftOpt = Schema.optionSchema[A](using leftSchema)
        Schema.tuple2Schema[Option[A], B](using leftOpt, rightSchema)
      case "full" =>
        val leftOpt = Schema.optionSchema[A](using leftSchema)
        val rightOpt = Schema.optionSchema[B](using rightSchema)
        Schema.tuple2Schema[Option[A], Option[B]](using leftOpt, rightOpt)
      case "left_anti" | "left_semi" =>
        leftSchema
      case other =>
        throw new RuntimeException(s"Unknown join type: $other") // scalafix:ok DisableSyntax.throw
    }
    schema.asInstanceOf[Schema[T]] // scalafix:ok DisableSyntax.asInstanceOf
  }

  private def applyZipWithIndex[A, T](parent: SparkPlan[A]): SparkPlan[T] = {
    val values = collectValues(parent)
    val indexed = values.zipWithIndex.map { case (v, i) => (v, i.toLong) }

    val tupleSchema = Schema.tuple2Schema[A, Long](using parent.schema, Schema.longSchema)
    val df = createDataFrame(indexed, tupleSchema)
    SparkPlan(df, tupleSchema.asInstanceOf[Schema[T]]) // scalafix:ok DisableSyntax.asInstanceOf
  }

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

  private def applyGroupByAgg[In, T](
    parent: SparkPlan[In],
    keySpecs: Vector[KeySpec[In]],
    aggSpecs: Vector[AggSpec[In]],
    schema: Schema[T]
  ): SparkPlan[T] = {
    val keyCols = keySpecs.map { spec =>
      ExprToColumn.convert(spec.expr) match {
        case Right((sparkCol, _)) => sparkCol.as(spec.name)
        case Left(err) =>
          throw new RuntimeException(s"Key expr conversion failed: $err") // scalafix:ok DisableSyntax.throw
      }
    }

    val aggCols = aggSpecs.map { spec =>
      ExprToColumn.convert(spec.expr) match {
        case Right((sparkCol, _)) => sparkCol.as(spec.name)
        case Left(err) =>
          throw new RuntimeException(s"Agg expr conversion failed: $err") // scalafix:ok DisableSyntax.throw
      }
    }

    val grouped = parent.df.groupBy(keyCols*)
    val aggDf = grouped.agg(aggCols.head, aggCols.tail*)

    // Rename output columns to match the output schema's column names
    val outputColNames = schema.columnNames
    val currentColNames = aggDf.columns.toVector
    val renamedDf = currentColNames.zip(outputColNames).foldLeft(aggDf) { case (df, (current, target)) =>
      if (current != target) df.withColumnRenamed(current, target) else df
    }
    SparkPlan(renamedDf, schema)
  }

  private def applySortByExprs[T](
    parent: SparkPlan[T],
    sortKeys: Vector[SortSpec[T]]
  ): SparkPlan[T] = {
    val sparkSortCols = sortKeys.map { spec =>
      ExprToColumn.convert(spec.expr) match {
        case Right((sparkCol, _)) =>
          if (spec.ascending) sparkCol.asc else sparkCol.desc
        case Left(_) =>
          throw new RuntimeException("Sort expr conversion failed") // scalafix:ok DisableSyntax.throw
      }
    }
    SparkPlan(parent.df.sort(sparkSortCols*), parent.schema)
  }

  private def applyWithWindow[In, T](
    parent: SparkPlan[In],
    windowExprs: Vector[WindowExprSpec[In]],
    windowSpec: net.ghoula.strongbow.WindowSpec[In],
    schema: Schema[T]
  ): SparkPlan[T] = {
    import org.apache.spark.sql.functions.*
    import org.apache.spark.sql.expressions.Window

    // Build Spark WindowSpec
    val partCols = windowSpec.partitionBy.map { spec =>
      ExprToColumn.convert(spec.expr) match {
        case Right((sparkCol, _)) => sparkCol
        case Left(err) =>
          throw new RuntimeException(s"Window partition expr conversion failed: $err") // scalafix:ok DisableSyntax.throw
      }
    }

    val orderSparkCols = windowSpec.orderBy.map { spec =>
      ExprToColumn.convert(spec.expr) match {
        case Right((sparkCol, _)) =>
          if (spec.ascending) sparkCol.asc else sparkCol.desc
        case Left(err) =>
          throw new RuntimeException(s"Window order expr conversion failed: $err") // scalafix:ok DisableSyntax.throw
      }
    }

    val w = {
      val partitioned = if (partCols.nonEmpty) Window.partitionBy(partCols*) else Window.partitionBy()
      if (orderSparkCols.nonEmpty) partitioned.orderBy(orderSparkCols*) else partitioned
    }

    // Add window columns
    var df = parent.df // scalafix:ok DisableSyntax.var
    windowExprs.foreach { spec =>
      val windowCol: org.apache.spark.sql.Column = spec.expr match {
        case _: Expr.RowNumber[_] => row_number().over(w)
        case _: Expr.Rank[_] => rank().over(w)
        case _: Expr.DenseRank[_] => dense_rank().over(w)
        case lagExpr: Expr.Lag[_, _] =>
          val innerCol = ExprToColumn.convert(lagExpr.expr) match {
            case Right((sc, _)) => sc
            case Left(err) =>
              throw new RuntimeException(s"Lag expr conversion failed: $err") // scalafix:ok DisableSyntax.throw
          }
          lagExpr.default match {
            case Some(d) => lag(innerCol, lagExpr.offset, d).over(w)
            case scala.None => lag(innerCol, lagExpr.offset).over(w)
          }
        case leadExpr: Expr.Lead[_, _] =>
          val innerCol = ExprToColumn.convert(leadExpr.expr) match {
            case Right((sc, _)) => sc
            case Left(err) =>
              throw new RuntimeException(s"Lead expr conversion failed: $err") // scalafix:ok DisableSyntax.throw
          }
          leadExpr.default match {
            case Some(d) => lead(innerCol, leadExpr.offset, d).over(w)
            case scala.None => lead(innerCol, leadExpr.offset).over(w)
          }
        case other =>
          throw new RuntimeException(s"Unsupported window expr: $other") // scalafix:ok DisableSyntax.throw
      }
      df = df.withColumn(spec.name, windowCol)
    }

    SparkPlan(df, schema)
  }

  private[spark] def createDataFrame[T](values: Vector[T], schema: Schema[T]): DataFrame = {
    val structType = SchemaConverter.toStructType(schema)
    val rows = values.map(v => RowConverter.toRow(v, schema))
    val javaRows = java.util.Arrays.asList(rows*)
    spark.createDataFrame(javaRows, structType)
  }
}
