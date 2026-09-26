package net.ghoula.vaire.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

import net.ghoula.vaire.Schema
import net.ghoula.vaire.column.{Column, ColumnType}
import net.ghoula.vaire.dataset.{Dataset, InMemorySource, MaterializedDataset}
import net.ghoula.vaire.errors.ExecutionError
import net.ghoula.vaire.expr.Expr
import net.ghoula.vaire.internal.{JoinOps, KeyedJoin, TypeChecks}
import net.ghoula.vaire.interpreter.{CollectedDataset, ExprInterpreter, Interpreter}
import net.ghoula.vaire.params.{AggSpec, KeySpec, SortSpec, WindowExprSpec, WindowSpec}

/** Spark-based interpreter for Vairë Dataset plans.
  *
  * Executes the same Dataset[T] AST on Apache Spark. Operations expressible as Expr translate to
  * native Spark Column expressions for Catalyst optimization. Function-based operations (Map,
  * FlatMap, joins with opaque predicates) collect to the driver, apply the function, and recreate
  * the DataFrame.
  */
class SparkInterpreter(spark: SparkSession) extends Interpreter {

  private case class SparkPlan[T](df: DataFrame, schema: Schema[T])

  override def execute[T](dataset: Dataset[T]): Either[ExecutionError, MaterializedDataset[T]] = {
    buildPlan(dataset).flatMap { plan =>
      val rows = plan.df.collect()
      RowConverter.toMaterialized(rows, plan.schema)
    }
  }

  override def executeCollect[T](dataset: Dataset[T]): Either[ExecutionError, CollectedDataset[T]] =
    Left(
      ExecutionError.UnsupportedOperation(
        "executeCollect is an in-memory interpreter feature; per-row error policies are unrepresentable on Spark (see docs/error-policy-design.md §5.3)"
      )
    )

  /** Build a Spark DataFrame from a Dataset plan without collecting results.
    *
    * Returns the DataFrame for lazy Spark operations: `.count()`, `.write.parquet(path)`,
    * `.createTempView(name)`, or chaining with native Spark API.
    */
  def toDataFrame[T](dataset: Dataset[T]): Either[ExecutionError, DataFrame] =
    buildPlan(dataset).map(_.df)

  private def buildPlan[T](dataset: Dataset[T]): Either[ExecutionError, SparkPlan[T]] = {
    dataset match {

      case root: Dataset.Root[T] =>
        root.source match {
          case SparkSource(df) =>
            SchemaConverter.validateSchema(root.schema, df.schema) match {
              case Some(message) =>
                Left(ExecutionError.InvalidPlan(s"SparkSource schema mismatch: $message"))
              case None =>
                Right(SparkPlan(df, root.schema))
            }
          case InMemorySource(columns) =>
            val df = DataFrameBuilder.fromColumns(spark, columns, root.schema)
            Right(SparkPlan(df, root.schema))
          case other =>
            Left(
              ExecutionError.UnsupportedOperation(
                s"SparkInterpreter does not support ${other.getClass.getSimpleName}"
              )
            )
        }

      case filt: Dataset.Filter[T] =>
        buildPlan(filt.parent).flatMap { parent =>
          ExprToColumn.convert(filt.predicate) match {
            case Right((sparkCol, _)) =>
              Right(SparkPlan(parent.df.filter(sparkCol), parent.schema))
            case Left(_) =>
              applyFilterViaMap(parent, filt.predicate)
          }
        }

      case m: Dataset.Map[?, T] =>
        buildPlan(m.parent).flatMap(parent => collectAndTransform(parent, _.map(m.func), m.schema))

      case fm: Dataset.FlatMap[?, T] =>
        buildPlan(fm.parent).flatMap(parent => collectAndTransform(parent, _.flatMap(fm.func), fm.schema))

      case sel: Dataset.Select[?, T] =>
        buildPlan(sel.parent).flatMap(parent => collectAndTransform(parent, _.map(sel.projection), sel.schema))

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

      case un: Dataset.Union[T] => applySetOperation(un.left, un.right, _.union(_))
      case inter: Dataset.Intersect[T] => applySetOperation(inter.left, inter.right, _.intersect(_))
      case exc: Dataset.Except[T] => applySetOperation(exc.left, exc.right, _.except(_))

      case srt: Dataset.Sort[T] =>
        buildPlan(srt.parent).flatMap { parent =>
          applySortViaCollect(parent, srt.ordering)
        }

      case srtBy: Dataset.SortBy[T, _] =>
        buildPlan(srtBy.parent).flatMap { parent =>
          applySortByViaCollect(parent, srtBy.key, srtBy.ordering)
        }

      case srtExpr: Dataset.SortByExpr[T, _] =>
        buildPlan(srtExpr.parent).flatMap { parent =>
          checkResolvedType(parent, srtExpr.keyExpr, srtExpr.keyType, "sortByExpr").flatMap { _ =>
            ExprToColumn.convert(srtExpr.keyExpr) match {
              case Right((sparkCol, _)) =>
                Right(SparkPlan(parent.df.sort(sparkCol), parent.schema))
              case Left(_) =>
                applySortByExprViaCollect(parent, srtExpr.keyExpr, srtExpr.keyType)
            }
          }
        }

      case samp: Dataset.Sample[T] =>
        buildPlan(samp.parent).map { parent =>
          SparkPlan(
            parent.df.sample(samp.withReplacement, samp.fraction, samp.seed),
            parent.schema
          )
        }

      case zipIdx: Dataset.ZipWithIndex[?] =>
        buildPlan(zipIdx.parent).flatMap(applyZipWithIndex)

      case zipId: Dataset.ZipWithUniqueId[?] =>
        buildPlan(zipId.parent).flatMap(applyZipWithUniqueId)

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

      case jn: Dataset.InnerJoin[?, ?] => applyJoin(jn.left, jn.right, jn.condition, applyInnerJoin)
      case jn: Dataset.LeftJoin[?, ?] => applyJoin(jn.left, jn.right, jn.condition, applyLeftJoin)
      case jn: Dataset.RightJoin[?, ?] => applyJoin(jn.left, jn.right, jn.condition, applyRightJoin)
      case jn: Dataset.FullJoin[?, ?] => applyJoin(jn.left, jn.right, jn.condition, applyFullJoin)
      case jn: Dataset.LeftAntiJoin[?, ?] => applyJoin(jn.left, jn.right, jn.condition, applyLeftAntiJoin)

      case jn: Dataset.InnerJoinOn[a, b, _] =>
        joinOnExprHelper(
          jn.left,
          jn.right,
          jn.leftKey,
          jn.rightKey,
          jn.leftKeyType,
          jn.rightKeyType,
          "inner",
          (ls, rs) => Schema.tuple2Schema[a, b](using ls, rs)
        )

      case jn: Dataset.LeftJoinOn[a, b, _] =>
        joinOnExprHelper(
          jn.left,
          jn.right,
          jn.leftKey,
          jn.rightKey,
          jn.leftKeyType,
          jn.rightKeyType,
          "left",
          (ls, rs) => Schema.tuple2Schema[a, Option[b]](using ls, Schema.optionSchema[b](using rs))
        )

      case jn: Dataset.RightJoinOn[a, b, _] =>
        joinOnExprHelper(
          jn.left,
          jn.right,
          jn.leftKey,
          jn.rightKey,
          jn.leftKeyType,
          jn.rightKeyType,
          "right",
          (ls, rs) => Schema.tuple2Schema[Option[a], b](using Schema.optionSchema[a](using ls), rs)
        )

      case jn: Dataset.FullJoinOn[a, b, _] =>
        joinOnExprHelper(
          jn.left,
          jn.right,
          jn.leftKey,
          jn.rightKey,
          jn.leftKeyType,
          jn.rightKeyType,
          "full",
          (ls, rs) =>
            Schema.tuple2Schema[Option[a], Option[b]](using
              Schema.optionSchema[a](using ls),
              Schema.optionSchema[b](using rs)
            )
        )

      case jn: Dataset.LeftAntiJoinOn[a, b, _] =>
        applyLeftOnlyJoinOnExpr[a, b](
          jn.left,
          jn.right,
          jn.leftKey,
          jn.rightKey,
          jn.leftKeyType,
          jn.rightKeyType,
          "left_anti"
        )

      case gba: Dataset.GroupByAgg[_, T] =>
        buildPlan(gba.parent).flatMap { parent =>
          applyGroupByAgg(parent, gba.keySpecs, gba.aggSpecs, gba.schema)
        }

      case srtExprs: Dataset.SortByExprs[T] =>
        buildPlan(srtExprs.parent).flatMap { parent =>
          applySortByExprs(parent, srtExprs.sortKeys)
        }

      case jn: Dataset.LeftSemiJoinOn[a, b, _] =>
        applyLeftOnlyJoinOnExpr[a, b](
          jn.left,
          jn.right,
          jn.leftKey,
          jn.rightKey,
          jn.leftKeyType,
          jn.rightKeyType,
          "left_semi"
        )

      case ww: Dataset.WithWindow[_, T] =>
        buildPlan(ww.parent).flatMap { parent =>
          applyWithWindow(parent, ww.windowExprs, ww.windowSpec, ww.schema)
        }

      case agg: Dataset.Aggregate[_, T] =>
        buildPlan(agg.parent).flatMap { parent =>
          applyGlobalAggregate(parent, agg.aggSpecs, agg.resultSchema)
        }

      case nar: Dataset.Narrow[_, T] =>
        buildPlan(nar.parent).flatMap(parent => narrowSchema(parent, nar.schema))

      case _: Dataset.WithPolicy[T] =>
        Left(
          ExecutionError.UnsupportedOperation(
            "withErrorPolicy scopes are an in-memory interpreter feature; per-row error policies are unrepresentable on Spark (see docs/error-policy-design.md §5.3)"
          )
        )
    }
  }

  /** Reinterpret a schema over the same Spark columns, asserting that non-optional fields hold no
    * nulls. The check runs a Spark job that stops at the first null.
    */
  private def narrowSchema[T, U](
    parent: SparkPlan[T],
    schema: Schema[U]
  ): Either[ExecutionError, SparkPlan[U]] = {
    val layoutMatches =
      parent.schema.columnCount == schema.columnCount &&
        parent.schema.columnNames == schema.columnNames &&
        parent.schema.columnTypes.zip(schema.columnTypes).forall { case (from, to) =>
          TypeChecks.underlying(from) == TypeChecks.underlying(to)
        }
    if (!layoutMatches)
      Left(ExecutionError.InvalidPlan("narrow: the new schema layout differs from the parent's"))
    else {
      val nonOptional = schema.columnNames.zip(schema.columnTypes).collect {
        case (name, ct) if !TypeChecks.isOptional(ct) => name
      }
      val hasNulls = nonOptional
        .map(name => org.apache.spark.sql.functions.col(name).isNull)
        .reduceOption(_ || _)
        .exists(cond => parent.df.filter(cond).limit(1).count() > 0)
      if (hasNulls)
        Left(ExecutionError.InvalidPlan("narrow: a column declared non-optional contains nulls"))
      else Right(SparkPlan(parent.df, schema))
    }
  }

  private def applySetOperation[T](
    leftDs: Dataset[T],
    rightDs: Dataset[T],
    op: (DataFrame, DataFrame) => DataFrame
  ): Either[ExecutionError, SparkPlan[T]] =
    for {
      left <- buildPlan(leftDs)
      right <- buildPlan(rightDs)
    } yield SparkPlan(op(left.df, right.df), left.schema)

  private def applyJoin[A, B, R](
    leftDs: Dataset[A],
    rightDs: Dataset[B],
    condition: (A, B) => Boolean,
    joinFunc: (SparkPlan[A], SparkPlan[B], (A, B) => Boolean) => Either[ExecutionError, SparkPlan[R]]
  ): Either[ExecutionError, SparkPlan[R]] =
    for {
      left <- buildPlan(leftDs)
      right <- buildPlan(rightDs)
      result <- joinFunc(left, right, condition)
    } yield result

  private def wrapAsSparkPlan[T](values: Vector[T], schema: Schema[T]): SparkPlan[T] =
    SparkPlan(createDataFrame(values, schema), schema)

  private def collectValues[T](plan: SparkPlan[T]): Either[ExecutionError, Vector[T]] = {
    val rows = plan.df.collect()
    rows.iterator
      .map(r => RowConverter.fromRow(r, plan.schema))
      .foldRight[Either[ExecutionError, Vector[T]]](Right(Vector.empty)) {
        case (Right(value), Right(acc)) => Right(value +: acc)
        case (Left(err), _) => Left(ExecutionError.DecodeFailed(err))
        case (_, left) => left
      }
  }

  private def applyFilterViaMap[T](
    parent: SparkPlan[T],
    predicate: Expr[T, Boolean]
  ): Either[ExecutionError, SparkPlan[T]] = {
    val rows = parent.df.collect()
    RowConverter.toMaterialized(rows, parent.schema).flatMap { materialized =>
      ExprInterpreter.evalColumn(predicate, materialized.columns, ColumnType.BooleanType).flatMap {
        case Column.BooleanColumn(data, _) =>
          data.indices
            .filter(data(_))
            .foldRight[Either[ExecutionError, Vector[T]]](Right(Vector.empty)) { (i, acc) =>
              for {
                values <- acc
                value <- RowConverter.fromRow(rows(i), parent.schema) match {
                  case Right(v) => Right(v)
                  case Left(err) => Left(ExecutionError.DecodeFailed(err))
                }
              } yield value +: values
            }
            .map(filtered => SparkPlan(createDataFrame(filtered, parent.schema), parent.schema))
        case other =>
          Left(ExecutionError.TypeMismatch("BooleanColumn", other.columnType.toString, "filter fallback"))
      }
    }
  }

  private def collectAndTransform[A, B](
    parent: SparkPlan[A],
    transform: Vector[A] => Vector[B],
    schema: Schema[B]
  ): Either[ExecutionError, SparkPlan[B]] =
    collectValues(parent).map(values => wrapAsSparkPlan(transform(values), schema))

  private def applySortViaCollect[T](parent: SparkPlan[T], ord: Ordering[T]): Either[ExecutionError, SparkPlan[T]] =
    collectValues(parent).map(values => wrapAsSparkPlan(values.sorted(using ord), parent.schema))

  private def applySortByViaCollect[T, K](
    parent: SparkPlan[T],
    key: T => K,
    ord: Ordering[K]
  ): Either[ExecutionError, SparkPlan[T]] =
    collectValues(parent).map(values => wrapAsSparkPlan(values.sortBy(key)(using ord), parent.schema))

  private def applySortByExprViaCollect[T](
    parent: SparkPlan[T],
    keyExpr: Expr[T, ?],
    keyType: ColumnType
  ): Either[ExecutionError, SparkPlan[T]] =
    collectValues(parent).flatMap { values =>
      MaterializedDataset.fromVector(values)(using parent.schema).flatMap { materialized =>
        ExprInterpreter.evalColumn(keyExpr, materialized.columns, keyType).flatMap { keyCol =>
          TypeChecks.resolvedColumnType(keyCol, keyType, "sortByExpr").flatMap { _ =>
            materialized.toVectorOrError.map { decoded =>
              val indices = Column.sortIndicesByColumn(keyCol, materialized.rowCount)
              val sorted = indices.iterator.map(decoded).toVector
              SparkPlan(createDataFrame(sorted, parent.schema), parent.schema)
            }
          }
        }
      }
    }

  private def applyInnerJoin[A, B](
    left: SparkPlan[A],
    right: SparkPlan[B],
    condition: (A, B) => Boolean
  ): Either[ExecutionError, SparkPlan[(A, B)]] =
    for {
      leftValues <- collectValues(left)
      rightValues <- collectValues(right)
    } yield {
      val resultRows = for {
        l <- leftValues
        r <- rightValues
        if condition(l, r)
      } yield (l, r)
      wrapAsSparkPlan(resultRows, Schema.tuple2Schema[A, B](using left.schema, right.schema))
    }

  private def applyJoinViaFlatMap[A, B, R](
    left: SparkPlan[A],
    right: SparkPlan[B],
    condition: (A, B) => Boolean,
    schema: Schema[R]
  )(f: (A, Vector[B]) => Vector[R]): Either[ExecutionError, SparkPlan[R]] =
    for {
      leftValues <- collectValues(left)
      rightValues <- collectValues(right)
    } yield {
      val resultRows = leftValues.flatMap(l => f(l, rightValues.filter(r => condition(l, r))))
      wrapAsSparkPlan(resultRows, schema)
    }

  private def applyLeftJoin[A, B](
    left: SparkPlan[A],
    right: SparkPlan[B],
    condition: (A, B) => Boolean
  ): Either[ExecutionError, SparkPlan[(A, Option[B])]] =
    applyJoinViaFlatMap(
      left,
      right,
      condition,
      Schema.tuple2Schema[A, Option[B]](using left.schema, Schema.optionSchema[B](using right.schema))
    ) { (l, matches) =>
      if (matches.isEmpty) Vector((l, None)) else matches.map(r => (l, Some(r)))
    }

  private def applyRightJoin[A, B](
    left: SparkPlan[A],
    right: SparkPlan[B],
    condition: (A, B) => Boolean
  ): Either[ExecutionError, SparkPlan[(Option[A], B)]] =
    applyJoinViaFlatMap(
      right,
      left,
      (r, l) => condition(l, r),
      Schema.tuple2Schema[Option[A], B](using Schema.optionSchema[A](using left.schema), right.schema)
    ) { (r, matches) =>
      if (matches.isEmpty) Vector((None, r)) else matches.map(l => (Some(l), r))
    }

  private def applyFullJoin[A, B](
    left: SparkPlan[A],
    right: SparkPlan[B],
    condition: (A, B) => Boolean
  ): Either[ExecutionError, SparkPlan[(Option[A], Option[B])]] =
    for {
      leftValues <- collectValues(left)
      rightValues <- collectValues(right)
    } yield {
      val resultRows = JoinOps.fullJoin(leftValues, rightValues, condition)
      val resultSchema = Schema.tuple2Schema[Option[A], Option[B]](using
        Schema.optionSchema[A](using left.schema),
        Schema.optionSchema[B](using right.schema)
      )
      wrapAsSparkPlan(resultRows, resultSchema)
    }

  private def applyLeftAntiJoin[A, B](
    left: SparkPlan[A],
    right: SparkPlan[B],
    condition: (A, B) => Boolean
  ): Either[ExecutionError, SparkPlan[A]] =
    for {
      leftValues <- collectValues(left)
      rightValues <- collectValues(right)
    } yield {
      val resultRows = leftValues.filter(l => !rightValues.exists(r => condition(l, r)))
      wrapAsSparkPlan(resultRows, left.schema)
    }

  private def joinOnExprBase[A, B](
    leftDs: Dataset[A],
    rightDs: Dataset[B],
    leftKey: Expr[A, ?],
    rightKey: Expr[B, ?],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType,
    joinType: String
  ): Either[ExecutionError, (SparkPlan[A], SparkPlan[B], DataFrame)] =
    KeyedJoin.validateKeyTypes(leftKeyType, rightKeyType) match {
      case Some(error) => Left(error)
      case None =>
        for {
          left <- buildPlan(leftDs)
          right <- buildPlan(rightDs)
          _ <- checkResolvedType(left, leftKey, leftKeyType, "keyed join left")
          _ <- checkResolvedType(right, rightKey, rightKeyType, "keyed join right")
        } yield {
          val leftColName = getColName(leftKey)
          val rightColName = getColName(rightKey)

          val leftAlias = left.df.alias("_l")
          val rightAlias = right.df.alias("_r")

          import org.apache.spark.sql.functions.col
          val joinCondition = col(s"_l.$leftColName") === col(s"_r.$rightColName")
          val joinedDf = leftAlias.join(rightAlias, joinCondition, joinType)

          val leftColNames = left.schema.columnNames.map(c => col(s"_l.$c")).toArray
          val rightColNames = right.schema.columnNames.map(c => col(s"_r.$c")).toArray

          val selectedDf = joinType match {
            case "left_anti" | "left_semi" =>
              joinedDf.select(leftColNames*)
            case _ =>
              joinedDf.select(leftColNames ++ rightColNames*)
          }

          (left, right, selectedDf)
        }
    }

  private def joinOnExprHelper[A, B, R](
    leftDs: Dataset[A],
    rightDs: Dataset[B],
    leftKey: Expr[A, ?],
    rightKey: Expr[B, ?],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType,
    joinType: String,
    schemaFunc: (Schema[A], Schema[B]) => Schema[R]
  ): Either[ExecutionError, SparkPlan[R]] =
    joinOnExprBase(leftDs, rightDs, leftKey, rightKey, leftKeyType, rightKeyType, joinType).map {
      case (left, right, selectedDf) =>
        SparkPlan(selectedDf, schemaFunc(left.schema, right.schema))
    }

  private def applyLeftOnlyJoinOnExpr[A, B](
    leftDs: Dataset[A],
    rightDs: Dataset[B],
    leftKey: Expr[A, ?],
    rightKey: Expr[B, ?],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType,
    joinType: String
  ): Either[ExecutionError, SparkPlan[A]] =
    joinOnExprBase(leftDs, rightDs, leftKey, rightKey, leftKeyType, rightKeyType, joinType).map {
      case (left, _, selectedDf) =>
        SparkPlan(selectedDf, left.schema)
    }

  private def getColName[Row, A](expr: Expr[Row, A]): String = expr match {
    case cell: Expr.Cell[_, _] => cell.name
    case named: Expr.Named[_, _] => named.name
    case _ => "_expr"
  }

  /** Check a declared key/element type against the expression's column in the built plan.
    *
    * Only `Cell` and `Named` expressions resolve to a plan column; other expressions are converted
    * by `ExprToColumn`/`convertExprToSparkCol`, and Spark's own resolution applies. Non-column
    * expressions are left unchecked, matching the surface the Spark backend resolves directly.
    */
  private def checkResolvedType[T](
    plan: SparkPlan[T],
    expr: Expr[T, ?],
    declared: ColumnType,
    context: String
  ): Either[ExecutionError, Unit] =
    expr match {
      case cell: Expr.Cell[_, _] =>
        plan.schema.columnTypes.lift(cell.index.toInt) match {
          case Some(actual) => TypeChecks.resolvedType(actual, declared, context)
          case None => Right(())
        }
      case named: Expr.Named[_, _] => checkResolvedType(plan, named.expr, declared, context)
      case _ => Right(())
    }

  private def allChecks(checks: Vector[Either[ExecutionError, Unit]]): Either[ExecutionError, Unit] =
    checks.foldLeft[Either[ExecutionError, Unit]](Right(()))((acc, check) => acc.flatMap(_ => check))

  private def applyZipWithIndex[A](parent: SparkPlan[A]): Either[ExecutionError, SparkPlan[(A, Long)]] =
    collectValues(parent).map { values =>
      wrapAsSparkPlan(
        values.zipWithIndex.map { case (v, i) => (v, i.toLong) },
        Schema.tuple2Schema[A, Long](using parent.schema, Schema.longSchema)
      )
    }

  private def applyZipWithUniqueId[A](parent: SparkPlan[A]): Either[ExecutionError, SparkPlan[(A, Long)]] = {
    import org.apache.spark.sql.functions.monotonically_increasing_id
    val tupleSchema = Schema.tuple2Schema[A, Long](using parent.schema, Schema.longSchema)
    val colCount = parent.schema.columnNames.length
    val uidColName = s"_vaire_${java.util.UUID.randomUUID().nn.toString.replace("-", "")}"
    val uidDf = parent.df.withColumn(uidColName, monotonically_increasing_id())
    uidDf
      .collect()
      .iterator
      .map { row =>
        RowConverter.fromRow(org.apache.spark.sql.Row.fromSeq((0 until colCount).map(row.get)), parent.schema) match {
          case Right(original) => Right((original, row.getLong(colCount)))
          case Left(err) => Left(ExecutionError.DecodeFailed(err))
        }
      }
      .foldRight[Either[ExecutionError, Vector[(A, Long)]]](Right(Vector.empty)) {
        case (Right(value), Right(acc)) => Right(value +: acc)
        case (Left(err), _) => Left(err)
        case (_, left) => left
      }
      .map(values => SparkPlan(createDataFrame(values, tupleSchema), tupleSchema))
  }

  private def convertExprs(
    exprs: Vector[(String, Expr[?, Any], ColumnType)]
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

  private def convertToSparkCols[Row](
    specs: Vector[Expr[Row, ?]],
    context: String
  ): Either[ExecutionError, Vector[org.apache.spark.sql.Column]] = {
    specs.foldLeft[Either[ExecutionError, Vector[org.apache.spark.sql.Column]]](Right(Vector.empty)) {
      case (acc, expr) =>
        acc.flatMap { cols =>
          convertExprToSparkCol(expr, context).map(cols :+ _)
        }
    }
  }

  private def convertNamedExprsToSparkCols[Row](
    specs: Vector[(Expr[Row, ?], String)],
    context: String
  ): Either[ExecutionError, Vector[org.apache.spark.sql.Column]] = {
    specs.foldLeft[Either[ExecutionError, Vector[org.apache.spark.sql.Column]]](Right(Vector.empty)) {
      case (acc, (expr, name)) =>
        acc.flatMap { cols =>
          convertExprToSparkCol(expr, context).map(sc => cols :+ sc.as(name))
        }
    }
  }

  private def applyGroupByAgg[In, T](
    parent: SparkPlan[In],
    keySpecs: Vector[KeySpec[In]],
    aggSpecs: Vector[AggSpec[In]],
    schema: Schema[T]
  ): Either[ExecutionError, SparkPlan[T]] = {
    for {
      _ <- allChecks(keySpecs.map(spec => checkResolvedType(parent, spec.expr, spec.columnType, "groupByAgg key")))
      keyCols <- convertNamedExprsToSparkCols(keySpecs.map(s => (s.expr, s.name)), "Key expr conversion failed")
      aggCols <- convertNamedExprsToSparkCols(aggSpecs.map(s => (s.expr, s.name)), "Agg expr conversion failed")
    } yield {
      val aggDf = parent.df.groupBy(keyCols*).agg(aggCols.head, aggCols.tail*)
      SparkPlan(aggDf, schema)
    }
  }

  private def applyGlobalAggregate[In, T](
    parent: SparkPlan[In],
    aggSpecs: Vector[AggSpec[In]],
    schema: Schema[T]
  ): Either[ExecutionError, SparkPlan[T]] = {
    convertNamedExprsToSparkCols(aggSpecs.map(s => (s.expr, s.name)), "Agg expr conversion failed").map { aggCols =>
      val aggDf = parent.df.agg(aggCols.head, aggCols.tail*)
      SparkPlan(aggDf, schema)
    }
  }

  private def convertSortSpecsToSparkCols[Row](
    specs: Vector[SortSpec[Row]],
    context: String
  ): Either[ExecutionError, Vector[org.apache.spark.sql.Column]] = {
    specs.foldLeft[Either[ExecutionError, Vector[org.apache.spark.sql.Column]]](Right(Vector.empty)) {
      case (acc, spec) =>
        acc.flatMap { cols =>
          convertExprToSparkCol(spec.expr, context).map { sc =>
            cols :+ (if (spec.ascending) sc.asc else sc.desc)
          }
        }
    }
  }

  private def applySortByExprs[T](
    parent: SparkPlan[T],
    sortKeys: Vector[SortSpec[T]]
  ): Either[ExecutionError, SparkPlan[T]] =
    for {
      _ <- allChecks(sortKeys.map(spec => checkResolvedType(parent, spec.expr, spec.columnType, "sortByExprs")))
      sparkSortCols <- convertSortSpecsToSparkCols(sortKeys, "Sort expr conversion failed")
    } yield SparkPlan(parent.df.sort(sparkSortCols*), parent.schema)

  private def applyWithWindow[In, T](
    parent: SparkPlan[In],
    windowExprs: Vector[WindowExprSpec[In]],
    windowSpec: WindowSpec[In],
    schema: Schema[T]
  ): Either[ExecutionError, SparkPlan[T]] = {
    import org.apache.spark.sql.expressions.Window

    for {
      _ <- allChecks(
        windowSpec.partitionBy.map(spec => checkResolvedType(parent, spec.expr, spec.columnType, "window partitionBy"))
      )
      _ <- allChecks(
        windowSpec.orderBy.map(spec => checkResolvedType(parent, spec.expr, spec.columnType, "window orderBy"))
      )
      partCols <- convertToSparkCols(windowSpec.partitionBy.map(_.expr), "Window partition expr conversion failed")
      orderSparkCols <- convertSortSpecsToSparkCols(windowSpec.orderBy, "Window order expr conversion failed")
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
      case lagExpr: Expr.Lag[In, a] =>
        applyWindowFunc(
          lagExpr.expr,
          "Lag",
          innerCol =>
            lagExpr.default match {
              case Some(d) => lag(innerCol, lagExpr.offset, d).over(w)
              case scala.None => lag(innerCol, lagExpr.offset).over(w)
            }
        )
      case leadExpr: Expr.Lead[In, a] =>
        applyWindowFunc(
          leadExpr.expr,
          "Lead",
          innerCol =>
            leadExpr.default match {
              case Some(d) => lead(innerCol, leadExpr.offset, d).over(w)
              case scala.None => lead(innerCol, leadExpr.offset).over(w)
            }
        )
      case other =>
        Left(ExecutionError.UnsupportedExpression(s"Unsupported window expr: $other"))
    }
  }

  private def applyWindowFunc[In, A](
    expr: Expr[In, A],
    context: String,
    f: org.apache.spark.sql.Column => org.apache.spark.sql.Column
  ): Either[ExecutionError, org.apache.spark.sql.Column] =
    convertExprToSparkCol(expr, s"$context expr conversion failed").map(f)

  private[spark] def createDataFrame[T](values: Vector[T], schema: Schema[T]): DataFrame = {
    val structType = SchemaConverter.toStructType(schema)
    val rows = values.map(v => RowConverter.toRow(v, schema))
    val javaRows = java.util.Arrays.asList(rows*)
    spark.createDataFrame(javaRows, structType)
  }
}
