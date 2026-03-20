package net.ghoula.strongbow.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

import net.ghoula.strongbow.{Column => SBColumn, Dataset, Expr, Interpreter, MaterializedDataset, Schema}
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
    buildPlan(dataset).flatMap { plan =>
      val rows = plan.df.collect()
      RowConverter.toMaterialized(rows, plan.schema)
    }
  }

  private def buildPlan[T](dataset: Dataset[T]): Either[ExecutionError, SparkPlan[T]] = {
    dataset match {

      case root: Dataset.Root[T] =>
        val df = DataFrameBuilder.fromColumns(spark, root.columns, root.schema)
        Right(SparkPlan(df, root.schema))

      case filt: Dataset.Filter[T] =>
        buildPlan(filt.parent).flatMap { parent =>
          ExprToColumn.convert(filt.predicate) match {
            case Right((sparkCol, _)) =>
              Right(SparkPlan(parent.df.filter(sparkCol), parent.schema))
            case Left(_) =>
              Right(applyFilterViaMap(parent, filt.predicate))
          }
        }

      case m: Dataset.Map[a, T] =>
        buildPlan(m.parent).map { parent =>
          applyMapFunction[a, T](parent, m.func, m.schema)
        }

      case fm: Dataset.FlatMap[a, T] =>
        buildPlan(fm.parent).map { parent =>
          applyFlatMapFunction[a, T](parent, fm.func, fm.schema)
        }

      case sel: Dataset.Select[a, T] =>
        buildPlan(sel.parent).map { parent =>
          applyMapFunction[a, T](parent, sel.projection, sel.schema)
        }

      case selectExprs: Dataset.SelectExprs[_, T] =>
        buildPlan(selectExprs.parent).flatMap { parent =>
          convertExprs(selectExprs.exprs).map { columns =>
            SparkPlan(parent.df.select(columns*), selectExprs.schema)
          }
        }

      case dist: Dataset.Distinct[T] =>
        buildPlan(dist.parent).map { parent =>
          SparkPlan(parent.df.distinct(), parent.schema)
        }

      case lim: Dataset.Limit[T] =>
        buildPlan(lim.parent).map { parent =>
          SparkPlan(parent.df.limit(lim.n), parent.schema)
        }

      case un: Dataset.Union[T] =>
        for {
          left <- buildPlan(un.left)
          right <- buildPlan(un.right)
        } yield SparkPlan(left.df.union(right.df), left.schema)

      case inter: Dataset.Intersect[T] =>
        for {
          left <- buildPlan(inter.left)
          right <- buildPlan(inter.right)
        } yield SparkPlan(left.df.intersect(right.df), left.schema)

      case exc: Dataset.Except[T] =>
        for {
          left <- buildPlan(exc.left)
          right <- buildPlan(exc.right)
        } yield SparkPlan(left.df.except(right.df), left.schema)

      case srt: Dataset.Sort[T] =>
        buildPlan(srt.parent).map { parent =>
          applySortViaCollect(parent, srt.ordering)
        }

      case srtBy: Dataset.SortBy[T, _] =>
        buildPlan(srtBy.parent).map { parent =>
          applySortByViaCollect(parent, srtBy.key, srtBy.ordering)
        }

      case srtExpr: Dataset.SortByExpr[T, _] =>
        buildPlan(srtExpr.parent).map { parent =>
          ExprToColumn.convert(srtExpr.keyExpr) match {
            case Right((sparkCol, _)) =>
              SparkPlan(parent.df.sort(sparkCol), parent.schema)
            case Left(_) =>
              applySortByExprViaCollect(parent, srtExpr.keyExpr, srtExpr.keyType)
          }
        }

      case samp: Dataset.Sample[T] =>
        buildPlan(samp.parent).map { parent =>
          SparkPlan(
            parent.df.sample(samp.withReplacement, samp.fraction, samp.seed),
            parent.schema
          )
        }

      case zip: Dataset.ZipWithIndex[a] =>
        buildPlan(zip.parent).map { parent =>
          applyZipWithIndex[a](parent)
        }

      case zip: Dataset.ZipWithUniqueId[a] =>
        buildPlan(zip.parent).map { parent =>
          applyZipWithUniqueId[a](parent)
        }

      case persist: Dataset.Persist[T] =>
        buildPlan(persist.parent).map { parent =>
          SparkPlan(parent.df.persist(), parent.schema)
        }

      case cp: Dataset.Checkpoint[T] =>
        buildPlan(cp.parent).map { parent =>
          SparkPlan(parent.df.checkpoint(), parent.schema)
        }

      case reb: Dataset.Rebalance[T] =>
        buildPlan(reb.parent).map { parent =>
          val df = reb.numPartitions match {
            case Some(n) => parent.df.repartition(n)
            case None => parent.df.repartition()
          }
          SparkPlan(df, parent.schema)
        }

      case jn: Dataset.InnerJoin[a, b] =>
        for {
          left <- buildPlan(jn.left)
          right <- buildPlan(jn.right)
        } yield applyInnerJoin[a, b](left, right, jn.condition)

      case jn: Dataset.LeftJoin[a, b] =>
        for {
          left <- buildPlan(jn.left)
          right <- buildPlan(jn.right)
        } yield applyLeftJoin[a, b](left, right, jn.condition)

      case jn: Dataset.RightJoin[a, b] =>
        for {
          left <- buildPlan(jn.left)
          right <- buildPlan(jn.right)
        } yield applyRightJoin[a, b](left, right, jn.condition)

      case jn: Dataset.FullJoin[a, b] =>
        for {
          left <- buildPlan(jn.left)
          right <- buildPlan(jn.right)
        } yield applyFullJoin[a, b](left, right, jn.condition)

      case jn: Dataset.LeftAntiJoin[a, b] =>
        for {
          left <- buildPlan(jn.left)
          right <- buildPlan(jn.right)
        } yield applyLeftAntiJoin[a, b](left, right, jn.condition)

      case jn: Dataset.InnerJoinOn[a, b, _] =>
        applyInnerJoinOnExpr[a, b](jn.left, jn.right, jn.leftKey, jn.rightKey)

      case jn: Dataset.LeftJoinOn[a, b, _] =>
        applyLeftJoinOnExpr[a, b](jn.left, jn.right, jn.leftKey, jn.rightKey)

      case jn: Dataset.RightJoinOn[a, b, _] =>
        applyRightJoinOnExpr[a, b](jn.left, jn.right, jn.leftKey, jn.rightKey)

      case jn: Dataset.FullJoinOn[a, b, _] =>
        applyFullJoinOnExpr[a, b](jn.left, jn.right, jn.leftKey, jn.rightKey)

      case jn: Dataset.LeftAntiJoinOn[a, b, _] =>
        applyLeftOnlyJoinOnExpr[a, b](jn.left, jn.right, jn.leftKey, jn.rightKey, "left_anti")

      case gba: Dataset.GroupByAgg[_, T] =>
        buildPlan(gba.parent).flatMap { parent =>
          applyGroupByAgg(parent, gba.keySpecs, gba.aggSpecs, gba.schema)
        }

      case srtExprs: Dataset.SortByExprs[T] =>
        buildPlan(srtExprs.parent).flatMap { parent =>
          applySortByExprs(parent, srtExprs.sortKeys)
        }

      case jn: Dataset.LeftSemiJoinOn[a, b, _] =>
        applyLeftOnlyJoinOnExpr[a, b](jn.left, jn.right, jn.leftKey, jn.rightKey, "left_semi")

      case ww: Dataset.WithWindow[_, T] =>
        buildPlan(ww.parent).flatMap { parent =>
          applyWithWindow(parent, ww.windowExprs, ww.windowSpec, ww.schema)
        }

      case agg: Dataset.Aggregate[_, T] =>
        buildPlan(agg.parent).flatMap { parent =>
          applyGlobalAggregate(parent, agg.aggSpecs, agg.resultSchema)
        }
    }
  }

  private def collectValues[T](plan: SparkPlan[T]): Vector[T] = {
    plan.df.collect().iterator.map(r => RowConverter.fromRowUnsafe(r, plan.schema)).toVector
  }

  private def applyFilterViaMap[T](parent: SparkPlan[T], predicate: Expr[T, Boolean]): SparkPlan[T] = {
    val values = collectValues(parent)
    val filtered = values.filter { v =>
      import net.ghoula.strongbow.{ExprInterpreter, ColumnType}
      val encoded = parent.schema.encode(v)
      val cols = encoded.zipWithIndex.map { case (value, idx) =>
        SBColumn.fromValues(Vector(value), parent.schema.columnTypes(idx))
      }.collect { case Right(c) => c }.toVector
      ExprInterpreter.evalColumn(predicate, cols, ColumnType.BooleanType) match {
        case Right(SBColumn.BooleanColumn(data, nulls)) =>
          data.length > 0 && !nulls.contains(0) && data(0)
        case _ => false
      }
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

  private def applySortByExprViaCollect[T](
    parent: SparkPlan[T],
    keyExpr: Expr[T, ?],
    keyType: net.ghoula.strongbow.ColumnType
  ): SparkPlan[T] = {
    val values = collectValues(parent)
    import net.ghoula.strongbow.MaterializedDataset
    MaterializedDataset.fromVector(values)(using parent.schema) match {
      case Right(materialized) =>
        net.ghoula.strongbow.ExprInterpreter.evalColumn(keyExpr, materialized.columns, keyType) match {
          case Right(keyCol) =>
            val decoded = materialized.toVectorUnsafe
            val indices = sortIndicesByColumn(keyCol, materialized.rowCount)
            val sorted = indices.iterator.map(decoded).toVector
            SparkPlan(createDataFrame(sorted, parent.schema), parent.schema)
          case Left(_) =>
            parent
        }
      case Left(_) => parent
    }
  }

  private def sortIndicesByColumn(col: SBColumn[?], rowCount: Int): Array[Int] = {
    col match {
      case SBColumn.IntColumn(data, _) =>
        (0 until rowCount).sortWith((a, b) => data(a) < data(b)).toArray
      case SBColumn.LongColumn(data, _) =>
        (0 until rowCount).sortWith((a, b) => data(a) < data(b)).toArray
      case SBColumn.DoubleColumn(data, _) =>
        (0 until rowCount).sortWith((a, b) => java.lang.Double.compare(data(a), data(b)) < 0).toArray
      case SBColumn.StringColumn(data, _) =>
        (0 until rowCount).sortWith((a, b) => data(a).compareTo(data(b)) < 0).toArray
      case SBColumn.DateColumn(data, _) =>
        (0 until rowCount).sortWith((a, b) => data(a) < data(b)).toArray
      case SBColumn.BooleanColumn(data, _) =>
        (0 until rowCount).sortWith((a, b) => !data(a) && data(b)).toArray
      case SBColumn.AnyColumn(data, _) =>
        (0 until rowCount)
          .sortWith((a, b) => String.valueOf(data(a)).compareTo(String.valueOf(data(b))) < 0)
          .toArray
    }
  }

  private def applyInnerJoin[A, B](
    left: SparkPlan[A],
    right: SparkPlan[B],
    condition: (A, B) => Boolean
  ): SparkPlan[(A, B)] = {
    val leftValues = collectValues(left)
    val rightValues = collectValues(right)

    val resultRows = for {
      l <- leftValues
      r <- rightValues
      if condition(l, r)
    } yield (l, r)

    val tupleSchema = Schema.tuple2Schema[A, B](using left.schema, right.schema)
    val df = createDataFrame(resultRows, tupleSchema)
    SparkPlan(df, tupleSchema)
  }

  private def applyLeftJoin[A, B](
    left: SparkPlan[A],
    right: SparkPlan[B],
    condition: (A, B) => Boolean
  ): SparkPlan[(A, Option[B])] = {
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
    SparkPlan(df, resultSchema)
  }

  private def applyRightJoin[A, B](
    left: SparkPlan[A],
    right: SparkPlan[B],
    condition: (A, B) => Boolean
  ): SparkPlan[(Option[A], B)] = {
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
    SparkPlan(df, resultSchema)
  }

  private def applyFullJoin[A, B](
    left: SparkPlan[A],
    right: SparkPlan[B],
    condition: (A, B) => Boolean
  ): SparkPlan[(Option[A], Option[B])] = {
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
    SparkPlan(df, resultSchema)
  }

  private def applyLeftAntiJoin[A, B](
    left: SparkPlan[A],
    right: SparkPlan[B],
    condition: (A, B) => Boolean
  ): SparkPlan[A] = {
    val leftValues = collectValues(left)
    val rightValues = collectValues(right)

    val resultRows = leftValues.filter { l =>
      !rightValues.exists(r => condition(l, r))
    }

    val df = createDataFrame(resultRows, left.schema)
    SparkPlan(df, left.schema)
  }

  private def joinOnExprBase[A, B](
    leftDs: Dataset[A],
    rightDs: Dataset[B],
    leftKey: Expr[A, ?],
    rightKey: Expr[B, ?],
    joinType: String
  ): Either[ExecutionError, (SparkPlan[A], SparkPlan[B], DataFrame)] = {
    for {
      left <- buildPlan(leftDs)
      right <- buildPlan(rightDs)
    } yield {
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

      (left, right, selectedDf)
    }
  }

  private def applyInnerJoinOnExpr[A, B](
    leftDs: Dataset[A],
    rightDs: Dataset[B],
    leftKey: Expr[A, ?],
    rightKey: Expr[B, ?]
  ): Either[ExecutionError, SparkPlan[(A, B)]] = {
    joinOnExprBase(leftDs, rightDs, leftKey, rightKey, "inner").map { case (left, right, selectedDf) =>
      val resultSchema = Schema.tuple2Schema[A, B](using left.schema, right.schema)
      SparkPlan(selectedDf, resultSchema)
    }
  }

  private def applyLeftJoinOnExpr[A, B](
    leftDs: Dataset[A],
    rightDs: Dataset[B],
    leftKey: Expr[A, ?],
    rightKey: Expr[B, ?]
  ): Either[ExecutionError, SparkPlan[(A, Option[B])]] = {
    joinOnExprBase(leftDs, rightDs, leftKey, rightKey, "left").map { case (left, right, selectedDf) =>
      val rightOpt = Schema.optionSchema[B](using right.schema)
      val resultSchema = Schema.tuple2Schema[A, Option[B]](using left.schema, rightOpt)
      SparkPlan(selectedDf, resultSchema)
    }
  }

  private def applyRightJoinOnExpr[A, B](
    leftDs: Dataset[A],
    rightDs: Dataset[B],
    leftKey: Expr[A, ?],
    rightKey: Expr[B, ?]
  ): Either[ExecutionError, SparkPlan[(Option[A], B)]] = {
    joinOnExprBase(leftDs, rightDs, leftKey, rightKey, "right").map { case (left, right, selectedDf) =>
      val leftOpt = Schema.optionSchema[A](using left.schema)
      val resultSchema = Schema.tuple2Schema[Option[A], B](using leftOpt, right.schema)
      SparkPlan(selectedDf, resultSchema)
    }
  }

  private def applyFullJoinOnExpr[A, B](
    leftDs: Dataset[A],
    rightDs: Dataset[B],
    leftKey: Expr[A, ?],
    rightKey: Expr[B, ?]
  ): Either[ExecutionError, SparkPlan[(Option[A], Option[B])]] = {
    joinOnExprBase(leftDs, rightDs, leftKey, rightKey, "full").map { case (left, right, selectedDf) =>
      val leftOpt = Schema.optionSchema[A](using left.schema)
      val rightOpt = Schema.optionSchema[B](using right.schema)
      val resultSchema = Schema.tuple2Schema[Option[A], Option[B]](using leftOpt, rightOpt)
      SparkPlan(selectedDf, resultSchema)
    }
  }

  private def applyLeftOnlyJoinOnExpr[A, B](
    leftDs: Dataset[A],
    rightDs: Dataset[B],
    leftKey: Expr[A, ?],
    rightKey: Expr[B, ?],
    joinType: String
  ): Either[ExecutionError, SparkPlan[A]] = {
    joinOnExprBase(leftDs, rightDs, leftKey, rightKey, joinType).map { case (left, _, selectedDf) =>
      SparkPlan(selectedDf, left.schema)
    }
  }

  private def getColName[Row, A](expr: Expr[Row, A]): String = expr match {
    case cell: Expr.Cell[_, _] => cell.name
    case named: Expr.Named[_, _] => named.name
    case _ => "_expr"
  }

  private def applyZipWithIndex[A](parent: SparkPlan[A]): SparkPlan[(A, Long)] = {
    val values = collectValues(parent)
    val indexed = values.zipWithIndex.map { case (v, i) => (v, i.toLong) }

    val tupleSchema = Schema.tuple2Schema[A, Long](using parent.schema, Schema.longSchema)
    val df = createDataFrame(indexed, tupleSchema)
    SparkPlan(df, tupleSchema)
  }

  private def applyZipWithUniqueId[A](parent: SparkPlan[A]): SparkPlan[(A, Long)] = {
    import org.apache.spark.sql.functions.monotonically_increasing_id
    val tupleSchema = Schema.tuple2Schema[A, Long](using parent.schema, Schema.longSchema)
    val colNames = parent.df.columns
    val uidDf = parent.df.withColumn("_uid", monotonically_increasing_id())
    val values = uidDf
      .collect()
      .iterator
      .map { row =>
        val original = RowConverter.fromRowUnsafe(
          org.apache.spark.sql.Row.fromSeq(colNames.indices.map(row.get)),
          parent.schema
        )
        val uid = row.getLong(colNames.length)
        (original, uid)
      }
      .toVector
    val df = createDataFrame(values, tupleSchema)
    SparkPlan(df, tupleSchema)
  }

  private def convertExprs(
    exprs: Vector[(String, Expr[?, Any], net.ghoula.strongbow.ColumnType)]
  ): Either[ExecutionError, Vector[org.apache.spark.sql.Column]] = {
    exprs.foldLeft[Either[ExecutionError, Vector[org.apache.spark.sql.Column]]](Right(Vector.empty)) {
      case (acc, (name, expr, _)) =>
        acc.flatMap { cols =>
          ExprToColumn.convert(expr) match {
            case Right((sparkCol, _)) => Right(cols :+ sparkCol.as(name))
            case Left(err) => Left(ExecutionError.UnsupportedExpression(s"Expr conversion failed: $err"))
          }
        }
    }
  }

  private def convertExprToSparkCol[Row](
    expr: Expr[Row, ?],
    context: String
  ): Either[ExecutionError, org.apache.spark.sql.Column] = {
    ExprToColumn.convert(expr) match {
      case Right((sparkCol, _)) => Right(sparkCol)
      case Left(err) => Left(ExecutionError.UnsupportedExpression(s"$context: $err"))
    }
  }

  private def applyGroupByAgg[In, T](
    parent: SparkPlan[In],
    keySpecs: Vector[KeySpec[In]],
    aggSpecs: Vector[AggSpec[In]],
    schema: Schema[T]
  ): Either[ExecutionError, SparkPlan[T]] = {
    for {
      keyCols <- keySpecs.foldLeft[Either[ExecutionError, Vector[org.apache.spark.sql.Column]]](Right(Vector.empty)) {
        case (acc, spec) =>
          acc.flatMap { cols =>
            convertExprToSparkCol(spec.expr, "Key expr conversion failed").map(sc => cols :+ sc.as(spec.name))
          }
      }
      aggCols <- aggSpecs.foldLeft[Either[ExecutionError, Vector[org.apache.spark.sql.Column]]](Right(Vector.empty)) {
        case (acc, spec) =>
          acc.flatMap { cols =>
            convertExprToSparkCol(spec.expr, "Agg expr conversion failed").map(sc => cols :+ sc.as(spec.name))
          }
      }
    } yield {
      val grouped = parent.df.groupBy(keyCols*)
      val aggDf = grouped.agg(aggCols.head, aggCols.tail*)

      val outputColNames = schema.columnNames
      val currentColNames = aggDf.columns.toVector
      val renamedDf = currentColNames.zip(outputColNames).foldLeft(aggDf) { case (df, (current, target)) =>
        if (current != target) df.withColumnRenamed(current, target) else df
      }
      SparkPlan(renamedDf, schema)
    }
  }

  private def applyGlobalAggregate[In, T](
    parent: SparkPlan[In],
    aggSpecs: Vector[AggSpec[In]],
    schema: Schema[T]
  ): Either[ExecutionError, SparkPlan[T]] = {
    aggSpecs
      .foldLeft[Either[ExecutionError, Vector[org.apache.spark.sql.Column]]](Right(Vector.empty)) { case (acc, spec) =>
        acc.flatMap { cols =>
          convertExprToSparkCol(spec.expr, "Agg expr conversion failed").map(sc => cols :+ sc.as(spec.name))
        }
      }
      .map { aggCols =>
        val aggDf = parent.df.agg(aggCols.head, aggCols.tail*)

        val outputColNames = schema.columnNames
        val currentColNames = aggDf.columns.toVector
        val renamedDf = currentColNames.zip(outputColNames).foldLeft(aggDf) { case (df, (current, target)) =>
          if (current != target) df.withColumnRenamed(current, target) else df
        }
        SparkPlan(renamedDf, schema)
      }
  }

  private def applySortByExprs[T](
    parent: SparkPlan[T],
    sortKeys: Vector[SortSpec[T]]
  ): Either[ExecutionError, SparkPlan[T]] = {
    sortKeys
      .foldLeft[Either[ExecutionError, Vector[org.apache.spark.sql.Column]]](Right(Vector.empty)) { case (acc, spec) =>
        acc.flatMap { cols =>
          convertExprToSparkCol(spec.expr, "Sort expr conversion failed").map { sc =>
            cols :+ (if (spec.ascending) sc.asc else sc.desc)
          }
        }
      }
      .map { sparkSortCols =>
        SparkPlan(parent.df.sort(sparkSortCols*), parent.schema)
      }
  }

  private def applyWithWindow[In, T](
    parent: SparkPlan[In],
    windowExprs: Vector[WindowExprSpec[In]],
    windowSpec: net.ghoula.strongbow.WindowSpec[In],
    schema: Schema[T]
  ): Either[ExecutionError, SparkPlan[T]] = {
    import org.apache.spark.sql.expressions.Window

    for {
      partCols <- windowSpec.partitionBy
        .foldLeft[Either[ExecutionError, Vector[org.apache.spark.sql.Column]]](Right(Vector.empty)) {
          case (acc, spec) =>
            acc.flatMap { cols =>
              convertExprToSparkCol(spec.expr, "Window partition expr conversion failed").map(cols :+ _)
            }
        }
      orderSparkCols <- windowSpec.orderBy
        .foldLeft[Either[ExecutionError, Vector[org.apache.spark.sql.Column]]](Right(Vector.empty)) {
          case (acc, spec) =>
            acc.flatMap { cols =>
              convertExprToSparkCol(spec.expr, "Window order expr conversion failed").map { sc =>
                cols :+ (if (spec.ascending) sc.asc else sc.desc)
              }
            }
        }
      resultDf <- {
        val w = {
          val partitioned = if (partCols.nonEmpty) Window.partitionBy(partCols*) else Window.partitionBy()
          if (orderSparkCols.nonEmpty) partitioned.orderBy(orderSparkCols*) else partitioned
        }

        windowExprs.foldLeft[Either[ExecutionError, DataFrame]](Right(parent.df)) { case (accDf, spec) =>
          accDf.flatMap { df =>
            resolveWindowExpr(spec.expr, w).map(windowCol => df.withColumn(spec.name, windowCol))
          }
        }
      }
    } yield SparkPlan(resultDf, schema)
  }

  private def resolveWindowExpr[In](
    windowExpr: Expr[In, ?],
    w: org.apache.spark.sql.expressions.WindowSpec
  ): Either[ExecutionError, org.apache.spark.sql.Column] = {
    import org.apache.spark.sql.functions.*
    windowExpr match {
      case _: Expr.RowNumber[_] => Right(row_number().over(w))
      case _: Expr.Rank[_] => Right(rank().over(w))
      case _: Expr.DenseRank[_] => Right(dense_rank().over(w))
      case lagExpr: Expr.Lag[_, _] =>
        convertExprToSparkCol(lagExpr.expr, "Lag expr conversion failed").map { innerCol =>
          lagExpr.default match {
            case Some(d) => lag(innerCol, lagExpr.offset, d).over(w)
            case scala.None => lag(innerCol, lagExpr.offset).over(w)
          }
        }
      case leadExpr: Expr.Lead[_, _] =>
        convertExprToSparkCol(leadExpr.expr, "Lead expr conversion failed").map { innerCol =>
          leadExpr.default match {
            case Some(d) => lead(innerCol, leadExpr.offset, d).over(w)
            case scala.None => lead(innerCol, leadExpr.offset).over(w)
          }
        }
      case other =>
        Left(ExecutionError.UnsupportedExpression(s"Unsupported window expr: $other"))
    }
  }

  private[spark] def createDataFrame[T](values: Vector[T], schema: Schema[T]): DataFrame = {
    val structType = SchemaConverter.toStructType(schema)
    val rows = values.map(v => RowConverter.toRow(v, schema))
    val javaRows = java.util.Arrays.asList(rows*)
    spark.createDataFrame(javaRows, structType)
  }
}
