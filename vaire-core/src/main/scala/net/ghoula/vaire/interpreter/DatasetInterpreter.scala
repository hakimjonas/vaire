package net.ghoula.vaire.interpreter

import net.ghoula.vaire.Schema
import net.ghoula.vaire.column.{Column, ColumnType}
import net.ghoula.vaire.dataset.{Dataset, InMemorySource, MaterializedDataset}
import net.ghoula.vaire.errors.ExecutionError
import net.ghoula.vaire.expr.Expr
import net.ghoula.vaire.internal.JoinOps
import net.ghoula.vaire.params.{AggSpec, KeySpec, SortSpec, WindowExprSpec, WindowSpec}

/** Main interpreter for Dataset execution.
  *
  * Walks the Dataset AST and produces MaterializedDataset results. Delegates to ExprInterpreter for
  * expression evaluation and uses columnar operations for efficiency.
  */
object DatasetInterpreter extends Interpreter {

  /** Execute a Dataset plan to produce a MaterializedDataset.
    *
    * `errors` carries the active per-row error policy: the default `RowErrors.failFast` preserves
    * the fail-fast contract, while `executeCollect` drives the walk with a Collect collector so
    * per-row failures null-mark their row instead of aborting. A `withErrorPolicy` node reached
    * here fails: the plain execute path has no result shape for collected errors.
    */
  def execute[T](dataset: Dataset[T]): Either[ExecutionError, MaterializedDataset[T]] =
    executeScoped(dataset)(using RowErrors.failFast)

  /** The plan walk; `errors` carries the active per-row collector (fail-fast by default). */
  def executeScoped[T](
    dataset: Dataset[T]
  )(using errors: RowErrors = RowErrors.failFast): Either[ExecutionError, MaterializedDataset[T]] = {
    dataset match {
      case root: Dataset.Root[T] =>
        root.source match {
          case InMemorySource(columns) => Right(MaterializedDataset(columns, root.schema))
          case other =>
            Left(
              ExecutionError.UnsupportedOperation(
                s"DatasetInterpreter does not support ${other.getClass.getSimpleName}"
              )
            )
        }

      case _: Dataset.WithPolicy[T] =>
        if errors.isCollect then
          Left(
            ExecutionError.UnsupportedOperation(
              "Nested withErrorPolicy scopes are not supported: a policy scopes exactly one plan"
            )
          )
        else
          Left(
            ExecutionError.UnsupportedOperation(
              "Policy-scoped plans must run through executeCollect; the plain execute path carries no error result"
            )
          )

      case filt: Dataset.Filter[T] =>
        executeScoped(filt.parent).flatMap(parent => filter(parent, filt.predicate))

      case m: Dataset.Map[?, T] =>
        executeScoped(m.parent).flatMap(collectAndTransform(_, _.map(m.func), m.schema))

      case fm: Dataset.FlatMap[?, T] =>
        executeScoped(fm.parent).flatMap(collectAndTransform(_, _.flatMap(fm.func), fm.schema))

      case sel: Dataset.Select[?, T] =>
        executeScoped(sel.parent).flatMap(collectAndTransform(_, _.map(sel.projection), sel.schema))

      case selectExprs: Dataset.SelectExprs[_, T] =>
        executeScoped(selectExprs.parent).flatMap { parent =>
          selectExpressions(parent, selectExprs.exprs, selectExprs.schema)
        }

      case dist: Dataset.Distinct[T] =>
        executeScoped(dist.parent).flatMap(distinct)

      case lim: Dataset.Limit[T] =>
        executeScoped(lim.parent).flatMap(parent => limit(parent, lim.n))

      case un: Dataset.Union[T] =>
        executeJoin(un.left, un.right)(union)

      case inter: Dataset.Intersect[T] =>
        executeJoin(inter.left, inter.right)(setFilter(_, _, include = true))

      case exc: Dataset.Except[T] =>
        executeJoin(exc.left, exc.right)(setFilter(_, _, include = false))

      case jn: Dataset.InnerJoin[?, ?] =>
        executeJoin(jn.left, jn.right)(innerJoinDatasets(_, _, jn.condition))

      case jn: Dataset.LeftJoin[?, ?] =>
        executeJoin(jn.left, jn.right)(leftJoinDatasets(_, _, jn.condition))

      case jn: Dataset.RightJoin[?, ?] =>
        executeJoin(jn.left, jn.right)(rightJoinDatasets(_, _, jn.condition))

      case jn: Dataset.FullJoin[?, ?] =>
        executeJoin(jn.left, jn.right)(fullJoinDatasets(_, _, jn.condition))

      case jn: Dataset.LeftAntiJoin[?, ?] =>
        executeJoin(jn.left, jn.right)(leftAntiJoinDatasets(_, _, jn.condition))

      case jn: Dataset.InnerJoinOn[?, ?, ?] =>
        executeJoin(jn.left, jn.right)(innerJoinOnExpr(_, _, jn.leftKey, jn.rightKey, jn.leftKeyType, jn.rightKeyType))

      case jn: Dataset.LeftJoinOn[?, ?, ?] =>
        executeJoin(jn.left, jn.right)(leftJoinOnExpr(_, _, jn.leftKey, jn.rightKey, jn.leftKeyType, jn.rightKeyType))

      case jn: Dataset.RightJoinOn[?, ?, ?] =>
        executeJoin(jn.left, jn.right)(rightJoinOnExpr(_, _, jn.leftKey, jn.rightKey, jn.leftKeyType, jn.rightKeyType))

      case jn: Dataset.FullJoinOn[?, ?, ?] =>
        executeJoin(jn.left, jn.right)(fullJoinOnExpr(_, _, jn.leftKey, jn.rightKey, jn.leftKeyType, jn.rightKeyType))

      case jn: Dataset.LeftAntiJoinOn[?, ?, ?] =>
        executeJoin(jn.left, jn.right)(
          filterJoinOnExpr(_, _, jn.leftKey, jn.rightKey, jn.leftKeyType, jn.rightKeyType, include = false)
        )

      case srt: Dataset.Sort[T] =>
        executeScoped(srt.parent).flatMap(parent => sort(parent, srt.ordering))

      case srtBy: Dataset.SortBy[T, _] =>
        executeScoped(srtBy.parent).flatMap(parent => sortBy(parent, srtBy.key, srtBy.ordering))

      case srtExpr: Dataset.SortByExpr[T, _] =>
        executeScoped(srtExpr.parent).flatMap(parent => sortByExpr(parent, srtExpr.keyExpr, srtExpr.keyType))

      case samp: Dataset.Sample[T] =>
        executeScoped(samp.parent).map { parent =>
          sample(parent, samp.fraction, samp.seed, samp.withReplacement)
        }

      case zip: Dataset.ZipWithIndex[?] =>
        executeScoped(zip.parent).flatMap { parent =>
          zipWithIndex(parent)
        }

      case zip: Dataset.ZipWithUniqueId[?] =>
        executeScoped(zip.parent).flatMap { parent =>
          zipWithIndex(parent)
        }

      case persist: Dataset.Persist[T] =>
        executeScoped(persist.parent)

      case cp: Dataset.Checkpoint[T] =>
        executeScoped(cp.parent).flatMap { parent =>
          MaterializedDataset.fromVector(parent.toVectorUnsafe)(using parent.schema)
        }

      case reb: Dataset.Rebalance[T] =>
        executeScoped(reb.parent)

      case gba: Dataset.GroupByAgg[_, T] =>
        executeScoped(gba.parent).flatMap { parent =>
          groupByAgg(parent, gba.keySpecs, gba.aggSpecs, gba.schema)
        }

      case srtExprs: Dataset.SortByExprs[T] =>
        executeScoped(srtExprs.parent).flatMap { parent =>
          sortByExprs(parent, srtExprs.sortKeys)
        }

      case jn: Dataset.LeftSemiJoinOn[?, ?, ?] =>
        executeJoin(jn.left, jn.right)(
          filterJoinOnExpr(_, _, jn.leftKey, jn.rightKey, jn.leftKeyType, jn.rightKeyType, include = true)
        )

      case ww: Dataset.WithWindow[_, T] =>
        executeScoped(ww.parent).flatMap { parent =>
          withWindow(parent, ww.windowExprs, ww.windowSpec, ww.schema)
        }

      case agg: Dataset.Aggregate[_, T] =>
        executeScoped(agg.parent).flatMap { parent =>
          globalAggregate(parent, agg.aggSpecs, agg.resultSchema)
        }
    }
  }

  /** Execute a policy-scoped plan: the outermost node must be a `withErrorPolicy` scope. The
    * transformations inside the scope run under the scope's Collect policy — per-row failures
    * null-mark their row and are recorded (bounded by `maxErrors`, with a by-kind summary) instead
    * of aborting. Structural errors and aggregation-expression failures still abort the plan. On
    * the Spark backend this entry point is rejected: policies are an in-memory interpreter feature.
    */
  def executeCollect[T](dataset: Dataset[T]): Either[ExecutionError, CollectedDataset[T]] =
    dataset match {
      case wp: Dataset.WithPolicy[T] =>
        val collector = RowErrors(wp.policy)
        executeScoped(wp.parent)(using collector).map { values =>
          val (entries, truncated, byKind) = collector.result
          CollectedDataset(values, entries, truncated, byKind)
        }
      case _ =>
        Left(
          ExecutionError.UnsupportedOperation(
            "executeCollect requires a withErrorPolicy scope as the outermost plan node"
          )
        )
    }

  /** orNull without the binary dependency: Scala 3.9 made `Option.orNull` a real binary method
    * whose erased signature the 2.13 `scala-library` (loaded first under the Spark arrangement)
    * does not have. A match compiles to `Option`'s stable `isEmpty`/`get` API, identical across
    * 2.13 and 3.x.
    */
  private def orNullOf[A](o: Option[A]): A | Null =
    o match {
      case Some(a) => a
      case None    => null
    }

  private def executeJoin[A, B, R](
    leftDs: Dataset[A],
    rightDs: Dataset[B]
  )(
    f: (MaterializedDataset[A], MaterializedDataset[B]) => Either[ExecutionError, MaterializedDataset[R]]
  )(using errors: RowErrors): Either[ExecutionError, MaterializedDataset[R]] =
    for {
      left <- executeScoped(leftDs)
      right <- executeScoped(rightDs)
      result <- f(left, right)
    } yield result

  private def collectAndTransform[A, B](
    parent: MaterializedDataset[A],
    transform: Vector[A] => Vector[B],
    schema: Schema[B]
  ): Either[ExecutionError, MaterializedDataset[B]] =
    MaterializedDataset.fromVector(transform(parent.toVectorUnsafe))(using schema)

  private def filter[T](
    dataset: MaterializedDataset[T],
    predicate: Expr[T, Boolean]
  )(using errors: RowErrors): Either[ExecutionError, MaterializedDataset[T]] = {
    ExprInterpreter.evalColumn(predicate, dataset.columns, ColumnType.BooleanType).flatMap {
      case Column.BooleanColumn(data, _) =>
        val indices = data.indices.filter(data(_)).toArray
        val newColumns = dataset.columns.map(_.slice(indices))
        Right(MaterializedDataset(newColumns, dataset.schema))
      case other =>
        Left(ExecutionError.TypeMismatch("BooleanColumn", other.columnType.toString, "filter"))
    }
  }

  private def distinct[T](dataset: MaterializedDataset[T]): Either[ExecutionError, MaterializedDataset[T]] = {
    val seen = scala.collection.mutable.HashSet.empty[Vector[Any]]
    val rowIndices = (0 until dataset.rowCount).filter { rowIdx =>
      val row = dataset.columns.map(_.getValue(rowIdx))
      if (seen.contains(row)) {
        false
      } else {
        seen.add(row)
        true
      }
    }.toArray

    val newColumns = dataset.columns.map(_.slice(rowIndices))
    Right(MaterializedDataset(newColumns, dataset.schema))
  }

  private def limit[T](dataset: MaterializedDataset[T], n: Int): Either[ExecutionError, MaterializedDataset[T]] = {
    if (n >= dataset.rowCount) {
      Right(dataset)
    } else {
      val newColumns = dataset.columns.map(_.take(n))
      Right(MaterializedDataset(newColumns, dataset.schema))
    }
  }

  private def union[T](
    left: MaterializedDataset[T],
    right: MaterializedDataset[T]
  ): Either[ExecutionError, MaterializedDataset[T]] = {
    if (left.columnCount != right.columnCount) {
      Left(
        ExecutionError.PreconditionViolation(
          "Cannot union datasets with different column counts"
        )
      )
    } else {
      traverseEither(left.columns.zip(right.columns)) { (leftCol, rightCol) =>
        leftCol.concat(rightCol)
      }.map(cols => MaterializedDataset(cols, left.schema))
    }
  }

  private def reindexBy[T](dataset: MaterializedDataset[T], indices: Array[Int]): MaterializedDataset[T] =
    MaterializedDataset(dataset.columns.map(_.slice(indices)), dataset.schema)

  private def sortDatasetBy[T](dataset: MaterializedDataset[T])(
    lt: (Int, Int) => Boolean
  ): Either[ExecutionError, MaterializedDataset[T]] =
    Right(reindexBy(dataset, (0 until dataset.rowCount).sortWith(lt).toArray))

  private def sort[T](
    dataset: MaterializedDataset[T],
    ord: Ordering[T]
  ): Either[ExecutionError, MaterializedDataset[T]] = {
    val decoded = dataset.toVectorUnsafe
    sortDatasetBy(dataset)((a, b) => ord.lt(decoded(a), decoded(b)))
  }

  private def sortBy[T, K](
    dataset: MaterializedDataset[T],
    key: T => K,
    ord: Ordering[K]
  ): Either[ExecutionError, MaterializedDataset[T]] = {
    val keys = dataset.toVectorUnsafe.map(key)
    sortDatasetBy(dataset)((a, b) => ord.lt(keys(a), keys(b)))
  }

  private def sortByExpr[T, K](
    dataset: MaterializedDataset[T],
    keyExpr: Expr[T, K],
    keyType: ColumnType
  )(using errors: RowErrors): Either[ExecutionError, MaterializedDataset[T]] =
    ExprInterpreter.evalColumn(keyExpr, dataset.columns, keyType).map { keyCol =>
      reindexBy(dataset, Column.sortIndicesByColumn(keyCol, dataset.rowCount))
    }

  private def selectExpressions[In, Out](
    dataset: MaterializedDataset[In],
    exprs: Vector[(String, Expr[In, Any], ColumnType)],
    outputSchema: Schema[Out]
  )(using errors: RowErrors): Either[ExecutionError, MaterializedDataset[Out]] = {
    traverseEither(exprs) { (name, expr, columnType) =>
      ExprInterpreter.evalColumn(expr, dataset.columns, columnType).flatMap { col =>
        if (col.columnType == columnType) Right(col)
        else Left(ExecutionError.TypeMismatch(columnType.toString, col.columnType.toString, s"selectAs '$name'"))
      }
    }.map(cols => MaterializedDataset(cols, outputSchema))
  }

  private def sample[T](
    dataset: MaterializedDataset[T],
    fraction: Double,
    seed: Long,
    withReplacement: Boolean
  ): MaterializedDataset[T] = {
    val rng = new scala.util.Random(seed)
    val rowCount = dataset.rowCount

    val selectedIndices = if (withReplacement) {
      val n = (rowCount * fraction).toInt
      Vector.fill(n)(rng.nextInt(rowCount))
    } else {
      (0 until rowCount).filter(_ => rng.nextDouble() < fraction).toVector
    }

    val newColumns = dataset.columns.map(_.slice(selectedIndices.toArray))
    MaterializedDataset(newColumns, dataset.schema)
  }

  private def setFilter[T](
    left: MaterializedDataset[T],
    right: MaterializedDataset[T],
    include: Boolean
  ): Either[ExecutionError, MaterializedDataset[T]] = {
    val leftRows = left.toVectorUnsafe
    val rightSet = right.toVectorUnsafe.toSet
    val filtered =
      if (include) leftRows.filter(rightSet.contains).distinct
      else leftRows.filterNot(rightSet.contains).distinct

    MaterializedDataset.fromVector(filtered)(using left.schema)
  }

  private def zipWithIndex[T](
    dataset: MaterializedDataset[T]
  ): Either[ExecutionError, MaterializedDataset[(T, Long)]] = {
    val rows = dataset.toVectorUnsafe
    val indexed = rows.zipWithIndex.map { case (value, idx) => (value, idx.toLong) }

    given Schema[(T, Long)] = Schema.tuple2Schema[T, Long](using dataset.schema, Schema.longSchema)

    MaterializedDataset.fromVector(indexed)
  }

  private def innerJoinDatasets[A, B](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    condition: (A, B) => Boolean
  ): Either[ExecutionError, MaterializedDataset[(A, B)]] = {
    val leftRows = left.toVectorUnsafe
    val rightRows = right.toVectorUnsafe

    val resultRows = for {
      leftVal <- leftRows
      rightVal <- rightRows
      if condition(leftVal, rightVal)
    } yield (leftVal, rightVal)

    given Schema[(A, B)] = Schema.tuple2Schema[A, B](using left.schema, right.schema)
    MaterializedDataset.fromVector(resultRows)
  }

  private def leftJoinSchema[A, B](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B]
  ): Schema[(A, Option[B])] = {
    given optB: Schema[Option[B]] = Schema.optionSchema[B](using right.schema)
    Schema.tuple2Schema[A, Option[B]](using left.schema, optB)
  }

  private def rightJoinSchema[A, B](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B]
  ): Schema[(Option[A], B)] = {
    given optA: Schema[Option[A]] = Schema.optionSchema[A](using left.schema)
    Schema.tuple2Schema[Option[A], B](using optA, right.schema)
  }

  private def fullJoinSchema[A, B](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B]
  ): Schema[(Option[A], Option[B])] = {
    given leftOpt: Schema[Option[A]] = Schema.optionSchema[A](using left.schema)
    given rightOpt: Schema[Option[B]] = Schema.optionSchema[B](using right.schema)
    Schema.tuple2Schema[Option[A], Option[B]](using leftOpt, rightOpt)
  }

  private def outerJoinDatasets[A, B, R](
    primary: MaterializedDataset[A],
    secondary: MaterializedDataset[B],
    matches: (A, B) => Boolean,
    mkMatched: (A, B) => R,
    mkUnmatched: A => R,
    schema: Schema[R]
  ): Either[ExecutionError, MaterializedDataset[R]] = {
    val primaryRows = primary.toVectorUnsafe
    val secondaryRows = secondary.toVectorUnsafe

    val resultRows = primaryRows.flatMap { pVal =>
      val found = secondaryRows.filter(sVal => matches(pVal, sVal))
      if (found.isEmpty) Vector(mkUnmatched(pVal))
      else found.map(sVal => mkMatched(pVal, sVal))
    }

    MaterializedDataset.fromVector(resultRows)(using schema)
  }

  private def leftJoinDatasets[A, B](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    condition: (A, B) => Boolean
  ): Either[ExecutionError, MaterializedDataset[(A, Option[B])]] =
    outerJoinDatasets(left, right, condition, (a, b) => (a, Some(b)), a => (a, None), leftJoinSchema(left, right))

  private def rightJoinDatasets[A, B](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    condition: (A, B) => Boolean
  ): Either[ExecutionError, MaterializedDataset[(Option[A], B)]] =
    outerJoinDatasets(
      right,
      left,
      (b, a) => condition(a, b),
      (b, a) => (Some(a), b),
      b => (None, b),
      rightJoinSchema(left, right)
    )

  private def fullJoinDatasets[A, B](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    condition: (A, B) => Boolean
  ): Either[ExecutionError, MaterializedDataset[(Option[A], Option[B])]] =
    MaterializedDataset.fromVector(JoinOps.fullJoin(left.toVectorUnsafe, right.toVectorUnsafe, condition))(using
      fullJoinSchema(left, right)
    )

  private def leftAntiJoinDatasets[A, B](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    condition: (A, B) => Boolean
  ): Either[ExecutionError, MaterializedDataset[A]] = {
    val leftRows = left.toVectorUnsafe
    val rightRows = right.toVectorUnsafe

    val resultRows = leftRows.filter { leftVal =>
      !rightRows.exists(rightVal => condition(leftVal, rightVal))
    }

    MaterializedDataset.fromVector(resultRows)(using left.schema)
  }

  private def buildKeyIndex(keyCol: Column[?], rowCount: Int): scala.collection.mutable.HashMap[Any, Vector[Int]] = {
    val index = scala.collection.mutable.HashMap.empty[Any, Vector[Int]]
    (0 until rowCount).foreach { i =>
      val key = keyCol.getValue(i)
      index.updateWith(key) {
        case Some(existing) => Some(existing :+ i)
        case None => Some(Vector(i))
      }
    }
    index
  }

  private def assembleJoinColumns[A, B](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    leftIndices: Array[Int],
    rightIndices: Array[Int]
  ): (Vector[Column[?]], Schema[(A, B)]) = {
    val leftCols = left.columns.map(_.slice(leftIndices))
    val rightCols = right.columns.map(_.slice(rightIndices))
    val schema = Schema.tuple2Schema[A, B](using left.schema, right.schema)
    (leftCols ++ rightCols, schema)
  }

  private def innerJoinOnExpr[A, B, K](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType
  )(using errors: RowErrors): Either[ExecutionError, MaterializedDataset[(A, B)]] =
    evalKeys(left, right, leftKey, rightKey, leftKeyType, rightKeyType) { (leftKeyCol, rightKeyCol) =>
      val leftIndex = buildKeyIndex(leftKeyCol, left.rowCount)
      val leftIdxBuf = scala.collection.mutable.ArrayBuffer.empty[Int]
      val rightIdxBuf = scala.collection.mutable.ArrayBuffer.empty[Int]

      (0 until right.rowCount).foreach { ri =>
        val rKey = rightKeyCol.getValue(ri)
        leftIndex.get(rKey).foreach { leftRows =>
          leftRows.foreach { li =>
            leftIdxBuf += li
            rightIdxBuf += ri
          }
        }
      }

      val (cols, schema) = assembleJoinColumns(left, right, leftIdxBuf.toArray, rightIdxBuf.toArray)
      MaterializedDataset(cols, schema)
    }

  private def probeJoinOnKeys[P, S, R](
    probeRows: Vector[P],
    probeKeyCol: Column[?],
    lookupKeyCol: Column[?],
    lookupRows: Vector[S],
    lookupRowCount: Int,
    mkMatched: (P, S) => R,
    mkUnmatched: P => R,
    schema: Schema[R]
  ): MaterializedDataset[R] = {
    val lookupIndex = buildKeyIndex(lookupKeyCol, lookupRowCount)

    val resultRows: Vector[R] = probeRows.zipWithIndex.flatMap { case (pVal, pi) =>
      val pKey = probeKeyCol.getValue(pi)
      lookupIndex.get(pKey) match {
        case Some(sIdxs) => sIdxs.map(si => mkMatched(pVal, lookupRows(si)))
        case None => Vector(mkUnmatched(pVal))
      }
    }

    MaterializedDataset.fromVector(resultRows)(using schema) match {
      case Right(md) => md
      case Left(_) => MaterializedDataset(Vector.empty, schema)
    }
  }

  private def evalKeys[A, B, K, R](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType
  )(f: (Column[?], Column[?]) => R)(using errors: RowErrors): Either[ExecutionError, R] =
    for {
      leftKeyCol <- ExprInterpreter.evalColumn(leftKey, left.columns, leftKeyType)
      rightKeyCol <- ExprInterpreter.evalColumn(rightKey, right.columns, rightKeyType)
    } yield f(leftKeyCol, rightKeyCol)

  private def leftJoinOnExpr[A, B, K](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType
  )(using errors: RowErrors): Either[ExecutionError, MaterializedDataset[(A, Option[B])]] =
    evalKeys(left, right, leftKey, rightKey, leftKeyType, rightKeyType) { (lkc, rkc) =>
      probeJoinOnKeys(
        left.toVectorUnsafe,
        lkc,
        rkc,
        right.toVectorUnsafe,
        right.rowCount,
        (a, b) => (a, Some(b)),
        a => (a, None),
        leftJoinSchema(left, right)
      )
    }

  private def rightJoinOnExpr[A, B, K](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType
  )(using errors: RowErrors): Either[ExecutionError, MaterializedDataset[(Option[A], B)]] =
    evalKeys(left, right, leftKey, rightKey, leftKeyType, rightKeyType) { (lkc, rkc) =>
      probeJoinOnKeys(
        right.toVectorUnsafe,
        rkc,
        lkc,
        left.toVectorUnsafe,
        left.rowCount,
        (b, a) => (Some(a), b),
        b => (None, b),
        rightJoinSchema(left, right)
      )
    }

  private def fullJoinOnExpr[A, B, K](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType
  )(using errors: RowErrors): Either[ExecutionError, MaterializedDataset[(Option[A], Option[B])]] =
    evalKeys(left, right, leftKey, rightKey, leftKeyType, rightKeyType) { (leftKeyCol, rightKeyCol) =>
      val rightIndex = buildKeyIndex(rightKeyCol, right.rowCount)
      val leftRows = left.toVectorUnsafe
      val rightRows = right.toVectorUnsafe
      val matchedRight = scala.collection.mutable.HashSet.empty[Int]

      val leftSideResults: Vector[(Option[A], Option[B])] = leftRows.zipWithIndex.flatMap { case (leftVal, li) =>
        val lKey = leftKeyCol.getValue(li)
        rightIndex.get(lKey) match {
          case Some(rightIdxs) =>
            rightIdxs.map { ri =>
              matchedRight += ri
              (Option(leftVal), Option(rightRows(ri)))
            }
          case None => Vector((Option(leftVal), Option.empty[B]))
        }
      }

      val unmatchedRight: Vector[(Option[A], Option[B])] = rightRows.zipWithIndex.collect {
        case (rightVal, ri) if !matchedRight.contains(ri) =>
          (Option.empty[A], Option(rightVal))
      }

      val resultRows = leftSideResults ++ unmatchedRight

      given resultSchema: Schema[(Option[A], Option[B])] = fullJoinSchema(left, right)
      MaterializedDataset.fromVector(resultRows) match {
        case Right(md) => md
        case Left(_) => MaterializedDataset(Vector.empty, resultSchema)
      }
    }

  private def filterJoinOnExpr[A, B, K](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType,
    include: Boolean
  )(using errors: RowErrors): Either[ExecutionError, MaterializedDataset[A]] =
    evalKeys(left, right, leftKey, rightKey, leftKeyType, rightKeyType) { (leftKeyCol, rightKeyCol) =>
      val rightKeys = scala.collection.mutable.HashSet.empty[Any]
      (0 until right.rowCount).foreach { ri =>
        rightKeys += rightKeyCol.getValue(ri)
      }

      val indices = (0 until left.rowCount).filter { li =>
        rightKeys.contains(leftKeyCol.getValue(li)) == include
      }.toArray

      reindexBy(left, indices)
    }

  private def emptyResult[T](
    columnTypes: Vector[ColumnType],
    schema: Schema[T]
  ): Either[ExecutionError, MaterializedDataset[T]] =
    Right(MaterializedDataset(columnTypes.map(Column.empty), schema))

  private def partitionByKeys(
    keyCols: Vector[Column[?]],
    rowCount: Int
  ): Vector[(Vector[Any], scala.collection.mutable.ArrayBuffer[Int])] = {
    val groups = scala.collection.mutable.LinkedHashMap.empty[Vector[Any], scala.collection.mutable.ArrayBuffer[Int]]
    (0 until rowCount).foreach { i =>
      val key = keyCols.map(_.getValue(i))
      groups.getOrElseUpdate(key, scala.collection.mutable.ArrayBuffer.empty[Int]) += i
    }
    groups.toVector
  }

  private def groupByAgg[In, Out](
    dataset: MaterializedDataset[In],
    keySpecs: Vector[KeySpec[In]],
    aggSpecs: Vector[AggSpec[In]],
    outputSchema: Schema[Out]
  )(using errors: RowErrors): Either[ExecutionError, MaterializedDataset[Out]] = {
    val rowCount = dataset.rowCount
    if (rowCount == 0) {
      emptyResult(keySpecs.map(_.columnType) ++ aggSpecs.map(_.columnType), outputSchema)
    } else {

      val keyColsOrError =
        traverseEither(keySpecs)(spec => ExprInterpreter.evalColumn(spec.expr, dataset.columns, spec.columnType))

      keyColsOrError.flatMap { keyCols =>
        val groupEntries = partitionByKeys(keyCols, rowCount)

        val keyOutputCols = keySpecs.zipWithIndex.map { case (spec, ki) =>
          val values = groupEntries.map(_._1(ki))
          Column.fromValues(values, spec.columnType)
        }

        val aggOutputCols = aggSpecs.map { spec =>
          val aggValues = groupEntries.map { case (_, rowIndices) =>
            val groupIndices = rowIndices.toArray
            val groupColumns = dataset.columns.map(_.slice(groupIndices))
            ExprInterpreter.evalAggregation(spec.expr, groupColumns)
          }
          val firstError = aggValues.collectFirst { case Left(err) => err }
          firstError match {
            case Some(err) => Left(err)
            case None =>
              val values = aggValues.collect { case Right(v) => v }
              Column.fromValues(values, spec.columnType)
          }
        }

        sequenceEither(keyOutputCols ++ aggOutputCols)
          .map(cols => MaterializedDataset(cols, outputSchema))
      }
    }
  }

  private def multiColumnLt[T](keyCols: Vector[(Column[?], SortSpec[T])], a: Int, b: Int): Boolean = {
    @scala.annotation.tailrec
    def loop(idx: Int): Int =
      if (idx >= keyCols.length) 0
      else {
        val (col, spec) = keyCols(idx)
        val cmp = Column.compareAt(col, a, b)
        val oriented = if (spec.ascending) cmp else -cmp
        if (oriented != 0) oriented
        else loop(idx + 1)
      }
    loop(0) < 0
  }

  private def sortByExprs[T](
    dataset: MaterializedDataset[T],
    sortKeys: Vector[SortSpec[T]]
  )(using errors: RowErrors): Either[ExecutionError, MaterializedDataset[T]] = {
    val rowCount = dataset.rowCount
    if (rowCount <= 1) Right(dataset)
    else {
      traverseEither(sortKeys)(spec =>
        ExprInterpreter.evalColumn(spec.expr, dataset.columns, spec.columnType).map((_, spec))
      ).map { keyCols =>
        reindexBy(dataset, (0 until rowCount).sortWith(multiColumnLt(keyCols, _, _)).toArray)
      }
    }
  }

  private def withWindow[In, Out](
    dataset: MaterializedDataset[In],
    windowExprs: Vector[WindowExprSpec[In]],
    windowSpec: WindowSpec[In],
    outputSchema: Schema[Out]
  )(using errors: RowErrors): Either[ExecutionError, MaterializedDataset[Out]] = {
    val rowCount = dataset.rowCount
    if (rowCount == 0) {
      val parentCols = dataset.columns
      val windowCols = windowExprs.map(spec => Column.empty(spec.columnType))
      Right(MaterializedDataset(parentCols ++ windowCols, outputSchema))
    } else {

      val partColsOrError = traverseEither(windowSpec.partitionBy)(spec =>
        ExprInterpreter.evalColumn(spec.expr, dataset.columns, spec.columnType)
      )

      val orderColsOrError = traverseEither(windowSpec.orderBy)(spec =>
        ExprInterpreter.evalColumn(spec.expr, dataset.columns, spec.columnType).map((_, spec))
      )

      for {
        partCols <- partColsOrError
        orderCols <- orderColsOrError
        result <- {
          val sortedPartitions = partitionByKeys(partCols, rowCount).map { case (key, indices) =>
            (key, indices.sortWith(multiColumnLt(orderCols, _, _)))
          }

          val windowResultCols = windowExprs.map { spec =>
            val resultArray = new Array[Any | Null](rowCount)
            val exprError = sortedPartitions.foldLeft(Option.empty[ExecutionError]) { case (err, (_, sortedIndices)) =>
              if (err.isDefined) err
              else {
                val partSize = sortedIndices.length
                spec.expr match {
                  case _: Expr.RowNumber[_] =>
                    (0 until partSize).foreach(pos => resultArray(sortedIndices(pos)) = pos + 1)
                    None

                  case _: Expr.Rank[_] =>
                    (0 until partSize).foldLeft(1) { (lastRank, pos) =>
                      val rank =
                        if (pos == 0) 1
                        else if (sameOrderValues(orderCols, sortedIndices(pos - 1), sortedIndices(pos))) lastRank
                        else pos + 1
                      resultArray(sortedIndices(pos)) = rank
                      rank
                    }
                    None

                  case _: Expr.DenseRank[_] =>
                    (0 until partSize).foldLeft(0) { (currentRank, pos) =>
                      val newGroup = pos == 0 || !sameOrderValues(orderCols, sortedIndices(pos - 1), sortedIndices(pos))
                      val rank = if (newGroup) currentRank + 1 else currentRank
                      resultArray(sortedIndices(pos)) = rank
                      rank
                    }
                    None

                  case lag: Expr.Lag[_, _] =>
                      applyShiftWindow(
                        lag.expr,
                        lag.offset,
                        orNullOf(lag.default),
                        -1,
                      partSize,
                      sortedIndices,
                      resultArray,
                      dataset.columns
                    )

                  case lead: Expr.Lead[_, _] =>
                      applyShiftWindow(
                        lead.expr,
                        lead.offset,
                        orNullOf(lead.default),
                        1,
                      partSize,
                      sortedIndices,
                      resultArray,
                      dataset.columns
                    )

                  case other =>
                    Some(ExecutionError.InvalidValue(s"Unsupported window expression: $other"))
                }
              }
            }

            exprError match {
              case Some(err) => Left(err)
              case None => Column.fromValues(resultArray.toVector, spec.columnType)
            }
          }

          sequenceEither(windowResultCols)
            .map(windowCols => MaterializedDataset(dataset.columns ++ windowCols, outputSchema))
        }
      } yield result
    }
  }

  private def sameOrderValues[T](
    orderCols: Vector[(Column[?], SortSpec[T])],
    idxA: Int,
    idxB: Int
  ): Boolean =
    orderCols.forall { case (col, _) =>
      java.util.Objects.equals(col.getValue(idxA), col.getValue(idxB))
    }

  private def applyShiftWindow(
    expr: Expr[?, ?],
    offset: Int,
    defaultVal: Any | Null,
    sign: Int,
    partSize: Int,
    sortedIndices: scala.collection.mutable.ArrayBuffer[Int],
    resultArray: Array[Any | Null],
    columns: Vector[Column[?]]
  )(using errors: RowErrors): Option[ExecutionError] = {
    val innerType = ExprInterpreter.inferExprColumnType(expr, columns)
    ExprInterpreter.evalColumn(expr, columns, innerType) match {
      case Right(innerCol) =>
        (0 until partSize).foreach { pos =>
          val srcPos = pos + sign * offset
          resultArray(sortedIndices(pos)) =
            if (srcPos >= 0 && srcPos < partSize) innerCol.getValue(sortedIndices(srcPos))
            else defaultVal
        }
        None
      case Left(e) => Some(e)
    }
  }

  private def traverseEither[A, B](
    items: Vector[A]
  )(f: A => Either[ExecutionError, B]): Either[ExecutionError, Vector[B]] =
    items.foldLeft[Either[ExecutionError, Vector[B]]](Right(Vector.empty)) { (acc, item) =>
      acc.flatMap(results => f(item).map(results :+ _))
    }

  private def sequenceEither[A](
    items: Vector[Either[ExecutionError, A]]
  ): Either[ExecutionError, Vector[A]] =
    traverseEither(items)(identity)

  private def globalAggregate[In, Out](
    dataset: MaterializedDataset[In],
    aggSpecs: Vector[AggSpec[In]],
    outputSchema: Schema[Out]
  ): Either[ExecutionError, MaterializedDataset[Out]] = {
    if (dataset.rowCount == 0) {
      emptyResult(aggSpecs.map(_.columnType), outputSchema)
    } else {

      val aggOutputCols = aggSpecs.map { spec =>
        ExprInterpreter.evalAggregation(spec.expr, dataset.columns).flatMap { value =>
          Column.fromValues(Vector(value), spec.columnType)
        }
      }

      sequenceEither(aggOutputCols)
        .map(cols => MaterializedDataset(cols, outputSchema))
    }
  }
}
