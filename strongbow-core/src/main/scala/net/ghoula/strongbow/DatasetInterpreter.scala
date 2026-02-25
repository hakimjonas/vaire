package net.ghoula.strongbow

import net.ghoula.strongbow.errors.ExecutionError
import net.ghoula.strongbow.specs.{AggSpec, KeySpec, SortSpec, WindowExprSpec}
import net.ghoula.strongbow.types.RowIndex

/** Main interpreter for Dataset execution.
  *
  * Walks the Dataset AST and produces MaterializedDataset results. Delegates to ExprInterpreter for
  * expression evaluation and uses columnar operations for efficiency.
  */
object DatasetInterpreter extends Interpreter {

  /** Execute a Dataset plan to produce a MaterializedDataset. */
  def execute[T](dataset: Dataset[T]): Either[ExecutionError, MaterializedDataset[T]] = {
    (dataset: @unchecked) match {
      case root: Dataset.Root[T] =>
        Right(MaterializedDataset(root.columns, root.schema))

      case filt: Dataset.Filter[T] =>
        execute(filt.parent).map(parent => filter(parent, filt.predicate))

      case m: Dataset.Map[?, T] =>
        execute(m.parent).flatMap { parent =>
          val rows = parent.toVectorUnsafe
          val mapped = rows.map(m.func)
          MaterializedDataset.fromVector(mapped)(using m.schema)
        }

      case fm: Dataset.FlatMap[?, T] =>
        execute(fm.parent).flatMap { parent =>
          val rows = parent.toVectorUnsafe
          val mapped = rows.flatMap(fm.func)
          MaterializedDataset.fromVector(mapped)(using fm.schema)
        }

      case sel: Dataset.Select[?, T] =>
        execute(sel.parent).flatMap { parent =>
          val rows = parent.toVectorUnsafe
          val projected = rows.map(sel.projection)
          MaterializedDataset.fromVector(projected)(using sel.schema)
        }

      case selectExprs: Dataset.SelectExprs[_, T] =>
        execute(selectExprs.parent).flatMap { parent =>
          selectExpressions(parent, selectExprs.exprs, selectExprs.schema)
        }

      case dist: Dataset.Distinct[T] =>
        execute(dist.parent).flatMap(distinct)

      case lim: Dataset.Limit[T] =>
        execute(lim.parent).flatMap(parent => limit(parent, lim.n))

      case un: Dataset.Union[T] =>
        for {
          left <- execute(un.left)
          right <- execute(un.right)
          result <- union(left, right)
        } yield result

      case inter: Dataset.Intersect[T] =>
        for {
          left <- execute(inter.left)
          right <- execute(inter.right)
          result <- intersectDatasets(left, right)
        } yield result

      case exc: Dataset.Except[T] =>
        for {
          left <- execute(exc.left)
          right <- execute(exc.right)
          result <- exceptDatasets(left, right)
        } yield result

      case jn: Dataset.InnerJoin[?, ?] =>
        for {
          left <- execute(jn.left)
          right <- execute(jn.right)
          result <- innerJoinDatasets(left, right, jn.condition)
        } yield result.asInstanceOf[MaterializedDataset[T]] // scalafix:ok DisableSyntax.asInstanceOf

      case jn: Dataset.LeftJoin[?, ?] =>
        for {
          left <- execute(jn.left)
          right <- execute(jn.right)
          result <- leftJoinDatasets(left, right, jn.condition)
        } yield result.asInstanceOf[MaterializedDataset[T]] // scalafix:ok DisableSyntax.asInstanceOf

      case jn: Dataset.RightJoin[?, ?] =>
        for {
          left <- execute(jn.left)
          right <- execute(jn.right)
          result <- rightJoinDatasets(left, right, jn.condition)
        } yield result.asInstanceOf[MaterializedDataset[T]] // scalafix:ok DisableSyntax.asInstanceOf

      case jn: Dataset.FullJoin[?, ?] =>
        for {
          left <- execute(jn.left)
          right <- execute(jn.right)
          result <- fullJoinDatasets(left, right, jn.condition)
        } yield result.asInstanceOf[MaterializedDataset[T]] // scalafix:ok DisableSyntax.asInstanceOf

      case jn: Dataset.LeftAntiJoin[?, ?] =>
        for {
          left <- execute(jn.left)
          right <- execute(jn.right)
          result <- leftAntiJoinDatasets(left, right, jn.condition)
        } yield result.asInstanceOf[MaterializedDataset[T]] // scalafix:ok DisableSyntax.asInstanceOf

      case jn: Dataset.InnerJoinOn[?, ?, ?] =>
        for {
          left <- execute(jn.left)
          right <- execute(jn.right)
          result <- innerJoinOnExpr(left, right, jn.leftKey, jn.rightKey, jn.leftKeyType, jn.rightKeyType)
        } yield result.asInstanceOf[MaterializedDataset[T]] // scalafix:ok DisableSyntax.asInstanceOf

      case jn: Dataset.LeftJoinOn[?, ?, ?] =>
        for {
          left <- execute(jn.left)
          right <- execute(jn.right)
          result <- leftJoinOnExpr(left, right, jn.leftKey, jn.rightKey, jn.leftKeyType, jn.rightKeyType)
        } yield result.asInstanceOf[MaterializedDataset[T]] // scalafix:ok DisableSyntax.asInstanceOf

      case jn: Dataset.RightJoinOn[?, ?, ?] =>
        for {
          left <- execute(jn.left)
          right <- execute(jn.right)
          result <- rightJoinOnExpr(left, right, jn.leftKey, jn.rightKey, jn.leftKeyType, jn.rightKeyType)
        } yield result.asInstanceOf[MaterializedDataset[T]] // scalafix:ok DisableSyntax.asInstanceOf

      case jn: Dataset.FullJoinOn[?, ?, ?] =>
        for {
          left <- execute(jn.left)
          right <- execute(jn.right)
          result <- fullJoinOnExpr(left, right, jn.leftKey, jn.rightKey, jn.leftKeyType, jn.rightKeyType)
        } yield result.asInstanceOf[MaterializedDataset[T]] // scalafix:ok DisableSyntax.asInstanceOf

      case jn: Dataset.LeftAntiJoinOn[?, ?, ?] =>
        for {
          left <- execute(jn.left)
          right <- execute(jn.right)
          result <- leftAntiJoinOnExpr(left, right, jn.leftKey, jn.rightKey, jn.leftKeyType, jn.rightKeyType)
        } yield result.asInstanceOf[MaterializedDataset[T]] // scalafix:ok DisableSyntax.asInstanceOf

      case srt: Dataset.Sort[T] =>
        execute(srt.parent).flatMap(parent => sort(parent, srt.ordering))

      case srtBy: Dataset.SortBy[T, _] =>
        execute(srtBy.parent).flatMap(parent => sortBy(parent, srtBy.key, srtBy.ordering))

      case srtExpr: Dataset.SortByExpr[T, _] =>
        execute(srtExpr.parent).flatMap(parent =>
          sortByExpr(parent, srtExpr.keyExpr, srtExpr.keyType, srtExpr.ordering)
        )

      case samp: Dataset.Sample[T] =>
        execute(samp.parent).map { parent =>
          sample(parent, samp.fraction, samp.seed, samp.withReplacement)
        }

      case zip: Dataset.ZipWithIndex[_] =>
        execute(zip.parent).flatMap { parent =>
          zipWithIndex(parent).map(_.asInstanceOf[MaterializedDataset[T]]) // scalafix:ok DisableSyntax.asInstanceOf
        }

      case gba: Dataset.GroupByAgg[_, T] =>
        execute(gba.parent).flatMap { parent =>
          groupByAgg(parent, gba.keySpecs, gba.aggSpecs, gba.schema)
        }

      case srtExprs: Dataset.SortByExprs[T] =>
        execute(srtExprs.parent).flatMap { parent =>
          sortByExprs(parent, srtExprs.sortKeys)
        }

      case jn: Dataset.LeftSemiJoinOn[?, ?, ?] =>
        for {
          left <- execute(jn.left)
          right <- execute(jn.right)
          result <- leftSemiJoinOnExpr(left, right, jn.leftKey, jn.rightKey, jn.leftKeyType, jn.rightKeyType)
        } yield result.asInstanceOf[MaterializedDataset[T]] // scalafix:ok DisableSyntax.asInstanceOf

      case ww: Dataset.WithWindow[_, T] =>
        execute(ww.parent).flatMap { parent =>
          withWindow(parent, ww.windowExprs, ww.windowSpec, ww.schema)
        }

      case rbk: Dataset.ReduceByKey[k, v] =>
        execute(rbk.parent).flatMap { parent =>
          val rows = parent.toVectorUnsafe
          val reduced = reduceByKeyHelper(rows.asInstanceOf[Vector[(k, v)]], rbk.reduce) // scalafix:ok DisableSyntax.asInstanceOf
          given Schema[k] = rbk.schemaK
          given Schema[v] = rbk.schemaV
          MaterializedDataset.fromVector(reduced)(using
            Schema.tuple2Schema[k, v].asInstanceOf[Schema[T]] // scalafix:ok DisableSyntax.asInstanceOf
          )
        }

      case abk: Dataset.AggregateByKey[k, v, r] =>
        execute(abk.parent).flatMap { parent =>
          val rows = parent.toVectorUnsafe
          val result = aggregateByKeyHelper(
            rows.asInstanceOf[Vector[(k, v)]], // scalafix:ok DisableSyntax.asInstanceOf
            abk.extractors,
            abk.reducers,
            abk.assembler
          )
          given Schema[k] = abk.schemaK
          given Schema[r] = abk.schemaR
          MaterializedDataset.fromVector(result)(using
            Schema.tuple2Schema[k, r].asInstanceOf[Schema[T]] // scalafix:ok DisableSyntax.asInstanceOf
          )
        }

      case mwk: Dataset.MapWithKeyExpr[t, k] =>
        execute(mwk.parent).flatMap { parent =>
          ExprInterpreter.evalColumn(mwk.keyExpr, parent.columns, mwk.keyType).flatMap { keyCol =>
            val rows = parent.toVectorUnsafe
            val pairs = rows.indices.iterator
              .map(i =>
                (keyCol.getValue(i).asInstanceOf[k], rows(i)) // scalafix:ok DisableSyntax.asInstanceOf
              )
              .toVector
            given Schema[k] = mwk.schemaK
            given Schema[t] = mwk.schemaT
            MaterializedDataset.fromVector(pairs)(using
              Schema.tuple2Schema[k, t].asInstanceOf[Schema[T]] // scalafix:ok DisableSyntax.asInstanceOf
            )
          }
        }
    }
  }

  private def filter[T](
    dataset: MaterializedDataset[T],
    predicate: Expr[T, Boolean]
  ): MaterializedDataset[T] = {
    val rowIndices = (0 until dataset.rowCount).filter { rowIdx =>
      ExprInterpreter.evalBoolean(predicate, dataset.columns, RowIndex(rowIdx))
    }.toArray

    val newColumns = dataset.columns.map(_.slice(rowIndices))

    MaterializedDataset(newColumns, dataset.schema)
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
      left.columns
        .zip(right.columns)
        .foldLeft[Either[ExecutionError, Vector[Column]]](Right(Vector.empty)) { case (acc, (leftCol, rightCol)) =>
          acc.flatMap { cols =>
            leftCol.concat(rightCol).map(cols :+ _)
          }
        }
        .map(cols => MaterializedDataset(cols, left.schema))
    }
  }

  private def sort[T](
    dataset: MaterializedDataset[T],
    ord: Ordering[T]
  ): Either[ExecutionError, MaterializedDataset[T]] = {
    val rowCount = dataset.rowCount
    val decoded = dataset.toVectorUnsafe
    val indices = (0 until rowCount).sortWith((a, b) => ord.lt(decoded(a), decoded(b))).toArray
    val newColumns = dataset.columns.map(_.slice(indices))
    Right(MaterializedDataset(newColumns, dataset.schema))
  }

  private def sortBy[T, K](
    dataset: MaterializedDataset[T],
    key: T => K,
    ord: Ordering[K]
  ): Either[ExecutionError, MaterializedDataset[T]] = {
    val rowCount = dataset.rowCount
    val decoded = dataset.toVectorUnsafe
    val keys = decoded.map(key)
    val indices = (0 until rowCount).sortWith((a, b) => ord.lt(keys(a), keys(b))).toArray
    val newColumns = dataset.columns.map(_.slice(indices))
    Right(MaterializedDataset(newColumns, dataset.schema))
  }

  private def sortByExpr[T, K](
    dataset: MaterializedDataset[T],
    keyExpr: Expr[T, K],
    keyType: ColumnType,
    ord: Ordering[K]
  ): Either[ExecutionError, MaterializedDataset[T]] = {
    ExprInterpreter.evalColumn(keyExpr, dataset.columns, keyType).map { keyCol =>
      val rowCount = dataset.rowCount
      val indices = keyCol match {
        case Column.IntColumn(data, _) =>
          val intOrd = ord.asInstanceOf[Ordering[Int]] // scalafix:ok DisableSyntax.asInstanceOf
          (0 until rowCount).sortWith((a, b) => intOrd.lt(data(a), data(b))).toArray
        case Column.LongColumn(data, _) =>
          val longOrd = ord.asInstanceOf[Ordering[Long]] // scalafix:ok DisableSyntax.asInstanceOf
          (0 until rowCount).sortWith((a, b) => longOrd.lt(data(a), data(b))).toArray
        case Column.DoubleColumn(data, _) =>
          val dblOrd = ord.asInstanceOf[Ordering[Double]] // scalafix:ok DisableSyntax.asInstanceOf
          (0 until rowCount).sortWith((a, b) => dblOrd.lt(data(a), data(b))).toArray
        case Column.StringColumn(data, _) =>
          val strOrd = ord.asInstanceOf[Ordering[String]] // scalafix:ok DisableSyntax.asInstanceOf
          (0 until rowCount).sortWith((a, b) => strOrd.lt(data(a), data(b))).toArray
        case _ =>
          (0 until rowCount).sortWith { (a, b) =>
            ord.lt(
              keyCol.getValue(a).asInstanceOf[K],
              keyCol.getValue(b).asInstanceOf[K]
            ) // scalafix:ok DisableSyntax.asInstanceOf
          }.toArray
      }
      val newColumns = dataset.columns.map(_.slice(indices))
      MaterializedDataset(newColumns, dataset.schema)
    }
  }

  private def selectExpressions[In, Out](
    dataset: MaterializedDataset[In],
    exprs: Vector[(String, Expr[In, Any], ColumnType)],
    outputSchema: Schema[Out]
  ): Either[ExecutionError, MaterializedDataset[Out]] = {
    val newColumnsOrError = exprs.foldLeft[Either[ExecutionError, Vector[Column]]](Right(Vector.empty)) {
      case (acc, (_, expr, columnType)) =>
        acc.flatMap { cols =>
          ExprInterpreter.evalColumn(expr, dataset.columns, columnType).map(cols :+ _)
        }
    }

    newColumnsOrError.map(cols => MaterializedDataset(cols, outputSchema))
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

  private def intersectDatasets[T](
    left: MaterializedDataset[T],
    right: MaterializedDataset[T]
  ): Either[ExecutionError, MaterializedDataset[T]] = {
    val leftRows = left.toVectorUnsafe
    val rightSet = right.toVectorUnsafe.toSet
    val common = leftRows.filter(rightSet.contains).distinct

    MaterializedDataset.fromVector(common)(using left.schema)
  }

  private def exceptDatasets[T](
    left: MaterializedDataset[T],
    right: MaterializedDataset[T]
  ): Either[ExecutionError, MaterializedDataset[T]] = {
    val leftRows = left.toVectorUnsafe
    val rightSet = right.toVectorUnsafe.toSet
    val diff = leftRows.filterNot(rightSet.contains).distinct

    MaterializedDataset.fromVector(diff)(using left.schema)
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

  private def leftJoinDatasets[A, B](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    condition: (A, B) => Boolean
  ): Either[ExecutionError, MaterializedDataset[(A, Option[B])]] = {
    val leftRows = left.toVectorUnsafe
    val rightRows = right.toVectorUnsafe

    val resultRows = leftRows.flatMap { leftVal =>
      val matches = rightRows.filter(rightVal => condition(leftVal, rightVal))
      if (matches.isEmpty) {
        Vector((leftVal, None))
      } else {
        matches.map(rightVal => (leftVal, Some(rightVal)))
      }
    }

    given rightOptionSchema: Schema[Option[B]] = Schema.optionSchema[B](using right.schema)
    given resultSchema: Schema[(A, Option[B])] = Schema.tuple2Schema[A, Option[B]](using left.schema, rightOptionSchema)
    MaterializedDataset.fromVector(resultRows)
  }

  private def rightJoinDatasets[A, B](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    condition: (A, B) => Boolean
  ): Either[ExecutionError, MaterializedDataset[(Option[A], B)]] = {
    val leftRows = left.toVectorUnsafe
    val rightRows = right.toVectorUnsafe

    val resultRows = rightRows.flatMap { rightVal =>
      val matches = leftRows.filter(leftVal => condition(leftVal, rightVal))
      if (matches.isEmpty) {
        Vector((None, rightVal))
      } else {
        matches.map(leftVal => (Some(leftVal), rightVal))
      }
    }

    given leftOptionSchema: Schema[Option[A]] = Schema.optionSchema[A](using left.schema)
    given resultSchema: Schema[(Option[A], B)] = Schema.tuple2Schema[Option[A], B](using leftOptionSchema, right.schema)
    MaterializedDataset.fromVector(resultRows)
  }

  private def fullJoinDatasets[A, B](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    condition: (A, B) => Boolean
  ): Either[ExecutionError, MaterializedDataset[(Option[A], Option[B])]] = {
    val leftRows = left.toVectorUnsafe
    val rightRows = right.toVectorUnsafe

    val leftMatches = scala.collection.mutable.HashSet.empty[A]
    val rightMatches = scala.collection.mutable.HashSet.empty[B]

    val innerResults = for {
      leftVal <- leftRows
      rightVal <- rightRows
      if condition(leftVal, rightVal)
    } yield {
      leftMatches.add(leftVal)
      rightMatches.add(rightVal)
      (Some(leftVal), Some(rightVal))
    }

    val unmatchedLeft = leftRows.filterNot(leftMatches.contains).map(leftVal => (Some(leftVal), None))

    val unmatchedRight = rightRows.filterNot(rightMatches.contains).map(rightVal => (None, Some(rightVal)))

    val resultRows = innerResults ++ unmatchedLeft ++ unmatchedRight

    given leftOptionSchema: Schema[Option[A]] = Schema.optionSchema[A](using left.schema)
    given rightOptionSchema: Schema[Option[B]] = Schema.optionSchema[B](using right.schema)
    given resultSchema: Schema[(Option[A], Option[B])] =
      Schema.tuple2Schema[Option[A], Option[B]](using leftOptionSchema, rightOptionSchema)
    MaterializedDataset.fromVector(resultRows)
  }

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

  private def buildKeyIndex(keyCol: Column, rowCount: Int): scala.collection.mutable.HashMap[Any, Vector[Int]] = {
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
  ): (Vector[Column], Schema[(A, B)]) = {
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
  ): Either[ExecutionError, MaterializedDataset[(A, B)]] = {
    for {
      leftKeyCol <- ExprInterpreter.evalColumn(leftKey, left.columns, leftKeyType)
      rightKeyCol <- ExprInterpreter.evalColumn(rightKey, right.columns, rightKeyType)
    } yield {
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
  }

  private def leftJoinOnExpr[A, B, K](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType
  ): Either[ExecutionError, MaterializedDataset[(A, Option[B])]] = {
    for {
      leftKeyCol <- ExprInterpreter.evalColumn(leftKey, left.columns, leftKeyType)
      rightKeyCol <- ExprInterpreter.evalColumn(rightKey, right.columns, rightKeyType)
    } yield {
      val rightIndex = buildKeyIndex(rightKeyCol, right.rowCount)
      val leftRows = left.toVectorUnsafe
      val rightRows = right.toVectorUnsafe

      val resultRows: Vector[(A, Option[B])] = leftRows.zipWithIndex.flatMap { case (leftVal, li) =>
        val lKey = leftKeyCol.getValue(li)
        rightIndex.get(lKey) match {
          case Some(rightIdxs) => rightIdxs.map(ri => (leftVal, Option(rightRows(ri))))
          case None => Vector((leftVal, Option.empty[B]))
        }
      }

      given rightOptionSchema: Schema[Option[B]] = Schema.optionSchema[B](using right.schema)
      given resultSchema: Schema[(A, Option[B])] =
        Schema.tuple2Schema[A, Option[B]](using left.schema, rightOptionSchema)
      MaterializedDataset
        .fromVector(resultRows)
        .getOrElse(
          throw new RuntimeException("leftJoinOnExpr assembly failed") // scalafix:ok DisableSyntax.throw
        )
    }
  }

  private def rightJoinOnExpr[A, B, K](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType
  ): Either[ExecutionError, MaterializedDataset[(Option[A], B)]] = {
    for {
      leftKeyCol <- ExprInterpreter.evalColumn(leftKey, left.columns, leftKeyType)
      rightKeyCol <- ExprInterpreter.evalColumn(rightKey, right.columns, rightKeyType)
    } yield {
      val leftIndex = buildKeyIndex(leftKeyCol, left.rowCount)
      val leftRows = left.toVectorUnsafe
      val rightRows = right.toVectorUnsafe

      val resultRows: Vector[(Option[A], B)] = rightRows.zipWithIndex.flatMap { case (rightVal, ri) =>
        val rKey = rightKeyCol.getValue(ri)
        leftIndex.get(rKey) match {
          case Some(leftIdxs) => leftIdxs.map(li => (Option(leftRows(li)), rightVal))
          case None => Vector((Option.empty[A], rightVal))
        }
      }

      given leftOptionSchema: Schema[Option[A]] = Schema.optionSchema[A](using left.schema)
      given resultSchema: Schema[(Option[A], B)] =
        Schema.tuple2Schema[Option[A], B](using leftOptionSchema, right.schema)
      MaterializedDataset
        .fromVector(resultRows)
        .getOrElse(
          throw new RuntimeException("rightJoinOnExpr assembly failed") // scalafix:ok DisableSyntax.throw
        )
    }
  }

  private def fullJoinOnExpr[A, B, K](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType
  ): Either[ExecutionError, MaterializedDataset[(Option[A], Option[B])]] = {
    for {
      leftKeyCol <- ExprInterpreter.evalColumn(leftKey, left.columns, leftKeyType)
      rightKeyCol <- ExprInterpreter.evalColumn(rightKey, right.columns, rightKeyType)
    } yield {
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

      given leftOptionSchema: Schema[Option[A]] = Schema.optionSchema[A](using left.schema)
      given rightOptionSchema: Schema[Option[B]] = Schema.optionSchema[B](using right.schema)
      given resultSchema: Schema[(Option[A], Option[B])] =
        Schema.tuple2Schema[Option[A], Option[B]](using leftOptionSchema, rightOptionSchema)
      MaterializedDataset
        .fromVector(resultRows)
        .getOrElse(
          throw new RuntimeException("fullJoinOnExpr assembly failed") // scalafix:ok DisableSyntax.throw
        )
    }
  }

  private def leftAntiJoinOnExpr[A, B, K](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType
  ): Either[ExecutionError, MaterializedDataset[A]] = {
    for {
      leftKeyCol <- ExprInterpreter.evalColumn(leftKey, left.columns, leftKeyType)
      rightKeyCol <- ExprInterpreter.evalColumn(rightKey, right.columns, rightKeyType)
    } yield {
      val rightKeys = scala.collection.mutable.HashSet.empty[Any]
      (0 until right.rowCount).foreach { ri =>
        rightKeys += rightKeyCol.getValue(ri)
      }

      val indices = (0 until left.rowCount).filter { li =>
        !rightKeys.contains(leftKeyCol.getValue(li))
      }.toArray

      val newColumns = left.columns.map(_.slice(indices))
      MaterializedDataset(newColumns, left.schema)
    }
  }

  // Phase 1: GROUP BY + Aggregate
  private def groupByAgg[In, Out](
    dataset: MaterializedDataset[In],
    keySpecs: Vector[KeySpec[In]],
    aggSpecs: Vector[AggSpec[In]],
    outputSchema: Schema[Out]
  ): Either[ExecutionError, MaterializedDataset[Out]] = {
    val rowCount = dataset.rowCount
    if (rowCount == 0) {
      val emptyCols = (keySpecs.map(_.columnType) ++ aggSpecs.map(_.columnType)).map(Column.empty)
      return Right(MaterializedDataset(emptyCols, outputSchema)) // scalafix:ok DisableSyntax.return
    }

    // Evaluate key columns
    val keyColsOrError = keySpecs.foldLeft[Either[ExecutionError, Vector[Column]]](Right(Vector.empty)) {
      case (acc, spec) => acc.flatMap(cols => ExprInterpreter.evalColumn(spec.expr, dataset.columns, spec.columnType).map(cols :+ _))
    }

    keyColsOrError.flatMap { keyCols =>
      // Build groups: composite key -> row indices
      val groups = scala.collection.mutable.LinkedHashMap.empty[Vector[Any], scala.collection.mutable.ArrayBuffer[Int]]
      (0 until rowCount).foreach { i =>
        val key = keyCols.map(_.getValue(i))
        groups.getOrElseUpdate(key, scala.collection.mutable.ArrayBuffer.empty[Int]) += i
      }

      // For each group, evaluate aggregations
      val groupEntries = groups.toVector

      // Build key output columns
      val keyOutputCols = keySpecs.zipWithIndex.map { case (spec, ki) =>
        val values = groupEntries.map(_._1(ki))
        Column.fromValues(values, spec.columnType)
      }

      // Build agg output columns
      val aggOutputCols = aggSpecs.map { spec =>
        val aggValues = groupEntries.map { case (_, rowIndices) =>
          val groupIndices = rowIndices.toArray
          val groupColumns = dataset.columns.map(_.slice(groupIndices))
          ExprInterpreter.evalAggregation(spec.expr, groupColumns)
        }
        // Check for errors
        val firstError = aggValues.collectFirst { case Left(err) => err }
        firstError match {
          case Some(err) => Left(err)
          case None =>
            val values = aggValues.collect { case Right(v) => v }
            Column.fromValues(values, spec.columnType)
        }
      }

      // Combine all columns
      val allColsResult = (keyOutputCols ++ aggOutputCols).foldLeft[Either[ExecutionError, Vector[Column]]](Right(Vector.empty)) {
        case (acc, colOrErr) => acc.flatMap(cols => colOrErr.map(cols :+ _))
      }

      allColsResult.map(cols => MaterializedDataset(cols, outputSchema))
    }
  }

  // Phase 2: Multi-column ORDER BY
  private def sortByExprs[T](
    dataset: MaterializedDataset[T],
    sortKeys: Vector[SortSpec[T]]
  ): Either[ExecutionError, MaterializedDataset[T]] = {
    val rowCount = dataset.rowCount
    if (rowCount <= 1) return Right(dataset) // scalafix:ok DisableSyntax.return

    // Evaluate all sort key columns
    val keyColsOrError = sortKeys.foldLeft[Either[ExecutionError, Vector[(Column, SortSpec[T])]]](Right(Vector.empty)) {
      case (acc, spec) =>
        acc.flatMap(cols => ExprInterpreter.evalColumn(spec.expr, dataset.columns, spec.columnType).map(cols :+ (_, spec)))
    }

    keyColsOrError.map { keyCols =>
      val indices = (0 until rowCount).sortWith { (a, b) =>
        var i = 0 // scalafix:ok DisableSyntax.var
        var result = 0 // scalafix:ok DisableSyntax.var
        while (i < keyCols.length && result == 0) {
          val (col, spec) = keyCols(i)
          result = compareColumnValues(col, a, b, spec.ordering)
          if (!spec.ascending) result = -result
          i += 1
        }
        result < 0
      }.toArray

      val newColumns = dataset.columns.map(_.slice(indices))
      MaterializedDataset(newColumns, dataset.schema)
    }
  }

  /** Compare two column values using typed Ordering.
    *
    * Uses Column pattern matching with typed fast paths, replicating the existing
    * `sortByExpr` pattern. The asInstanceOf casts are sound because ColumnType and
    * Column are in sync, and the Ordering was summoned for the correct type at construction.
    */
  private def compareColumnValues[K](col: Column, a: Int, b: Int, ordering: Ordering[K]): Int =
    col match {
      case Column.IntColumn(data, nulls) =>
        val na = nulls.contains(a); val nb = nulls.contains(b)
        if (na && nb) 0 else if (na) -1 else if (nb) 1
        else ordering.asInstanceOf[Ordering[Int]].compare(data(a), data(b)) // scalafix:ok DisableSyntax.asInstanceOf
      case Column.LongColumn(data, nulls) =>
        val na = nulls.contains(a); val nb = nulls.contains(b)
        if (na && nb) 0 else if (na) -1 else if (nb) 1
        else ordering.asInstanceOf[Ordering[Long]].compare(data(a), data(b)) // scalafix:ok DisableSyntax.asInstanceOf
      case Column.DoubleColumn(data, nulls) =>
        val na = nulls.contains(a); val nb = nulls.contains(b)
        if (na && nb) 0 else if (na) -1 else if (nb) 1
        else ordering.asInstanceOf[Ordering[Double]].compare(data(a), data(b)) // scalafix:ok DisableSyntax.asInstanceOf
      case Column.StringColumn(data, nulls) =>
        val na = nulls.contains(a); val nb = nulls.contains(b)
        if (na && nb) 0 else if (na) -1 else if (nb) 1
        else ordering.asInstanceOf[Ordering[String]].compare(data(a), data(b)) // scalafix:ok DisableSyntax.asInstanceOf
      case Column.DateColumn(data, nulls) =>
        val na = nulls.contains(a); val nb = nulls.contains(b)
        if (na && nb) 0 else if (na) -1 else if (nb) 1
        else ordering.asInstanceOf[Ordering[Int]].compare(data(a), data(b)) // scalafix:ok DisableSyntax.asInstanceOf
      case _ =>
        val va = col.getValue(a)
        val vb = col.getValue(b)
        if (Option(va).isEmpty && Option(vb).isEmpty) 0
        else if (Option(va).isEmpty) -1
        else if (Option(vb).isEmpty) 1
        else ordering.compare(
          va.asInstanceOf[K], // scalafix:ok DisableSyntax.asInstanceOf
          vb.asInstanceOf[K]  // scalafix:ok DisableSyntax.asInstanceOf
        )
    }

  // Phase 4: Left semi-join
  private def leftSemiJoinOnExpr[A, B, K](
    left: MaterializedDataset[A],
    right: MaterializedDataset[B],
    leftKey: Expr[A, K],
    rightKey: Expr[B, K],
    leftKeyType: ColumnType,
    rightKeyType: ColumnType
  ): Either[ExecutionError, MaterializedDataset[A]] = {
    for {
      leftKeyCol <- ExprInterpreter.evalColumn(leftKey, left.columns, leftKeyType)
      rightKeyCol <- ExprInterpreter.evalColumn(rightKey, right.columns, rightKeyType)
    } yield {
      val rightKeys = scala.collection.mutable.HashSet.empty[Any]
      (0 until right.rowCount).foreach { ri =>
        rightKeys += rightKeyCol.getValue(ri)
      }

      val indices = (0 until left.rowCount).filter { li =>
        rightKeys.contains(leftKeyCol.getValue(li))
      }.toArray

      val newColumns = left.columns.map(_.slice(indices))
      MaterializedDataset(newColumns, left.schema)
    }
  }

  // Phase 5: Window functions
  private def withWindow[In, Out](
    dataset: MaterializedDataset[In],
    windowExprs: Vector[WindowExprSpec[In]],
    windowSpec: WindowSpec[In],
    outputSchema: Schema[Out]
  ): Either[ExecutionError, MaterializedDataset[Out]] = {
    val rowCount = dataset.rowCount
    if (rowCount == 0) {
      val parentCols = dataset.columns
      val windowCols = windowExprs.map(spec => Column.empty(spec.columnType))
      return Right(MaterializedDataset(parentCols ++ windowCols, outputSchema)) // scalafix:ok DisableSyntax.return
    }

    // Evaluate partition-by columns
    val partColsOrError = windowSpec.partitionBy.foldLeft[Either[ExecutionError, Vector[Column]]](Right(Vector.empty)) {
      case (acc, spec) => acc.flatMap(cols => ExprInterpreter.evalColumn(spec.expr, dataset.columns, spec.columnType).map(cols :+ _))
    }

    // Evaluate order-by columns (with typed SortSpec)
    val orderColsOrError = windowSpec.orderBy.foldLeft[Either[ExecutionError, Vector[(Column, SortSpec[In])]]](Right(Vector.empty)) {
      case (acc, spec) =>
        acc.flatMap(cols => ExprInterpreter.evalColumn(spec.expr, dataset.columns, spec.columnType).map(cols :+ (_, spec)))
    }

    for {
      partCols <- partColsOrError
      orderCols <- orderColsOrError
      result <- {
        // Build partitions: composite key -> row indices
        val partitions = scala.collection.mutable.LinkedHashMap.empty[Vector[Any], scala.collection.mutable.ArrayBuffer[Int]]
        (0 until rowCount).foreach { i =>
          val key = partCols.map(_.getValue(i))
          partitions.getOrElseUpdate(key, scala.collection.mutable.ArrayBuffer.empty[Int]) += i
        }

        // Sort rows within each partition using typed ordering
        val sortedPartitions = partitions.toVector.map { case (key, indices) =>
          val sorted = indices.sortWith { (a, b) =>
            var i = 0 // scalafix:ok DisableSyntax.var
            var result = 0 // scalafix:ok DisableSyntax.var
            while (i < orderCols.length && result == 0) {
              val (col, spec) = orderCols(i)
              result = compareColumnValues(col, a, b, spec.ordering)
              if (!spec.ascending) result = -result
              i += 1
            }
            result < 0
          }
          (key, sorted)
        }

        // Evaluate window functions: produce per-row values
        val windowResultCols = windowExprs.map { spec =>
          val resultArray = new Array[Any](rowCount)
          var exprError: Option[ExecutionError] = None // scalafix:ok DisableSyntax.var
          sortedPartitions.foreach { case (_, sortedIndices) =>
            val partSize = sortedIndices.length
            spec.expr match {
              case _: Expr.RowNumber[_] =>
                var pos = 0 // scalafix:ok DisableSyntax.var
                while (pos < partSize) {
                  resultArray(sortedIndices(pos)) = pos + 1
                  pos += 1
                }

              case _: Expr.Rank[_] =>
                var pos = 0 // scalafix:ok DisableSyntax.var
                while (pos < partSize) {
                  val rank = if (pos == 0) 1
                  else {
                    val prev = sortedIndices(pos - 1)
                    val curr = sortedIndices(pos)
                    val same = orderCols.forall { case (col, _) =>
                      java.util.Objects.equals(col.getValue(prev), col.getValue(curr))
                    }
                    if (same) resultArray(sortedIndices(pos - 1)).asInstanceOf[Int] // scalafix:ok DisableSyntax.asInstanceOf
                    else pos + 1
                  }
                  resultArray(sortedIndices(pos)) = rank
                  pos += 1
                }

              case _: Expr.DenseRank[_] =>
                var pos = 0 // scalafix:ok DisableSyntax.var
                var currentRank = 0 // scalafix:ok DisableSyntax.var
                while (pos < partSize) {
                  val newGroup = pos == 0 || {
                    val prev = sortedIndices(pos - 1)
                    val curr = sortedIndices(pos)
                    !orderCols.forall { case (col, _) =>
                      java.util.Objects.equals(col.getValue(prev), col.getValue(curr))
                    }
                  }
                  if (newGroup) currentRank += 1
                  resultArray(sortedIndices(pos)) = currentRank
                  pos += 1
                }

              case lag: Expr.Lag[_, _] =>
                val offset = lag.offset
                val defaultVal = lag.default.getOrElse(null) // scalafix:ok DisableSyntax.null
                val innerExpr = lag.expr
                var pos = 0 // scalafix:ok DisableSyntax.var
                while (pos < partSize) {
                  val srcPos = pos - offset
                  if (srcPos >= 0 && srcPos < partSize) {
                    val srcRow = sortedIndices(srcPos)
                    resultArray(sortedIndices(pos)) = ExprInterpreter.eval(innerExpr, dataset.columns, RowIndex(srcRow))
                      .getOrElse(defaultVal)
                  } else {
                    resultArray(sortedIndices(pos)) = defaultVal
                  }
                  pos += 1
                }

              case lead: Expr.Lead[_, _] =>
                val offset = lead.offset
                val defaultVal = lead.default.getOrElse(null) // scalafix:ok DisableSyntax.null
                val innerExpr = lead.expr
                var pos = 0 // scalafix:ok DisableSyntax.var
                while (pos < partSize) {
                  val srcPos = pos + offset
                  if (srcPos >= 0 && srcPos < partSize) {
                    val srcRow = sortedIndices(srcPos)
                    resultArray(sortedIndices(pos)) = ExprInterpreter.eval(innerExpr, dataset.columns, RowIndex(srcRow))
                      .getOrElse(defaultVal)
                  } else {
                    resultArray(sortedIndices(pos)) = defaultVal
                  }
                  pos += 1
                }

              case other =>
                exprError = Some(ExecutionError.InvalidValue(s"Unsupported window expression: $other"))
            }
          }

          exprError match {
            case Some(err) => Left(err)
            case None => Column.fromValues(resultArray.toVector, spec.columnType)
          }
        }

        // Collect all window columns, checking for errors
        windowResultCols.foldLeft[Either[ExecutionError, Vector[Column]]](Right(Vector.empty)) {
          case (acc, colOrErr) => acc.flatMap(cols => colOrErr.map(cols :+ _))
        }.map(windowCols => MaterializedDataset(dataset.columns ++ windowCols, outputSchema))
      }
    } yield result
  }

  private def reduceByKeyHelper[K, V](pairs: Vector[(K, V)], reduce: (V, V) => V): Vector[(K, V)] = {
    val builder = scala.collection.mutable.HashMap.empty[K, V]
    pairs.foreach { case (k, v) =>
      builder.get(k) match {
        case Some(existing) => builder(k) = reduce(existing, v)
        case None => builder(k) = v
      }
    }
    builder.toVector
  }

  private def aggregateByKeyHelper[K, V, R](
    pairs: Vector[(K, V)],
    extractors: Vector[V => Any],
    reducers: Vector[(Any, Any) => Any],
    assembler: Vector[Any] => R
  ): Vector[(K, R)] = {
    val n = extractors.length
    val builder = scala.collection.mutable.HashMap.empty[K, Array[Any]]
    pairs.foreach { case (k, v) =>
      val extracted = extractors.map(_(v))
      builder.get(k) match {
        case Some(existing) =>
          var i = 0 // scalafix:ok DisableSyntax.var
          while (i < n) {
            existing(i) = reducers(i)(existing(i), extracted(i))
            i += 1
          }
        case None =>
          builder(k) = extracted.toArray
      }
    }
    builder.toVector.map { case (k, aggs) => (k, assembler(aggs.toVector)) }
  }
}
