package net.ghoula.strongbow

import net.ghoula.strongbow.errors.ExecutionError
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

      case grp: Dataset.GroupedToPairs[k, v] =>
        val pairs: Vector[(k, v)] = GroupByInterpreter.execute(grp.grouped)

        // Use tuple2Schema given instance with captured evidence
        given Schema[k] = grp.schemaK
        given Schema[v] = grp.schemaV
        val tupleSchema: Schema[(k, v)] = summon[Schema[(k, v)]]

        MaterializedDataset.fromVector(pairs)(using
          tupleSchema.asInstanceOf[Schema[T]]
        ) // scalafix:ok DisableSyntax.asInstanceOf

      case keys: Dataset.GroupedKeys[k, v] =>
        val pairs: Vector[(k, v)] = GroupByInterpreter.execute(keys.grouped)
        val keyValues: Vector[k] = pairs.map(_._1)

        MaterializedDataset.fromVector(keyValues)(using
          keys.schemaK.asInstanceOf[Schema[T]]
        ) // scalafix:ok DisableSyntax.asInstanceOf

      case values: Dataset.GroupedValues[k, v] =>
        val pairs: Vector[(k, v)] = GroupByInterpreter.execute(values.grouped)
        val valueValues: Vector[v] = pairs.map(_._2)

        MaterializedDataset.fromVector(valueValues)(using
          values.schemaV.asInstanceOf[Schema[T]]
        ) // scalafix:ok DisableSyntax.asInstanceOf
    }
  }

  /** Filter rows based on predicate expression.
    *
    * Uses evalBoolean fast path — no Either allocation per row, short-circuit And/Or.
    */
  private def filter[T](
    dataset: MaterializedDataset[T],
    predicate: Expr[T, Boolean]
  ): MaterializedDataset[T] = {
    // Convert to Array for 2x better slice performance (benchmarked at 500K elements)
    val rowIndices = (0 until dataset.rowCount).filter { rowIdx =>
      ExprInterpreter.evalBoolean(predicate, dataset.columns, RowIndex(rowIdx))
    }.toArray

    // Use type-specialized slice instead of getValue + fromValues
    // This avoids boxing, intermediate Vector[Any], and multiple traversals
    val newColumns = dataset.columns.map(_.slice(rowIndices))

    MaterializedDataset(newColumns, dataset.schema)
  }

  /** Remove duplicate rows.
    *
    * Keeps the HashSet-based index collection (right algorithm), but uses Column.slice for
    * reconstruction instead of the boxing getValue/fromValues round-trip.
    */
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

  /** Limit result to first n rows.
    *
    * Uses Column.take for typed prefix slicing — no boxing round-trip.
    */
  private def limit[T](dataset: MaterializedDataset[T], n: Int): Either[ExecutionError, MaterializedDataset[T]] = {
    if (n >= dataset.rowCount) {
      Right(dataset)
    } else {
      val newColumns = dataset.columns.map(_.take(n))
      Right(MaterializedDataset(newColumns, dataset.schema))
    }
  }

  /** Union two datasets with the same schema.
    *
    * Uses Column.concat for typed array concatenation — no boxing round-trip.
    */
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

  /** Sort dataset using ordering.
    *
    * Index-based sort: decode N rows once, sort an index array, then Column.slice to reorder.
    * Eliminates the expensive MaterializedDataset.fromVector re-encoding (N encodes + N fromValues
    * boxing round-trips).
    */
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

  /** Sort dataset by key function.
    *
    * Index-based sort: decode N rows, apply key function once per row, sort index array by cached
    * keys, then Column.slice. Eliminates re-encoding.
    */
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

  /** Sort dataset by expression — zero decode.
    *
    * Evaluates the expression to a key column, then sorts index by reading typed array values
    * directly. For Cell expressions, this is a column passthrough (zero work). Eliminates the
    * toVectorUnsafe decode that lambda-based sortBy requires.
    */
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
          // Fallback: read via getValue
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

  /** Select columns by evaluating expressions.
    *
    * Uses vectorized evaluation — expressions operate on entire columns at once instead of
    * row-by-row. Cell references return columns directly (zero work), arithmetic and comparisons
    * use while-loops on typed arrays.
    */
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

  /** Sample rows using reservoir sampling. */
  private def sample[T](
    dataset: MaterializedDataset[T],
    fraction: Double,
    seed: Long,
    withReplacement: Boolean
  ): MaterializedDataset[T] = {
    val rng = new scala.util.Random(seed)
    val rowCount = dataset.rowCount

    val selectedIndices = if (withReplacement) {
      // Sample with replacement: allow duplicates
      val n = (rowCount * fraction).toInt
      Vector.fill(n)(rng.nextInt(rowCount))
    } else {
      // Sample without replacement: filter-based sampling
      (0 until rowCount).filter(_ => rng.nextDouble() < fraction).toVector
    }

    val newColumns = dataset.columns.map(_.slice(selectedIndices.toArray))
    MaterializedDataset(newColumns, dataset.schema)
  }

  /** Set intersection - rows in both datasets, deduplicated.
    *
    * Matches Spark's `Dataset.intersect()`: returns distinct rows present in both.
    */
  private def intersectDatasets[T](
    left: MaterializedDataset[T],
    right: MaterializedDataset[T]
  ): Either[ExecutionError, MaterializedDataset[T]] = {
    val leftRows = left.toVectorUnsafe
    val rightSet = right.toVectorUnsafe.toSet
    val common = leftRows.filter(rightSet.contains).distinct

    MaterializedDataset.fromVector(common)(using left.schema)
  }

  /** Set difference - rows in left but not right, deduplicated.
    *
    * Matches Spark's `Dataset.except()`: returns distinct rows in left not present in right.
    */
  private def exceptDatasets[T](
    left: MaterializedDataset[T],
    right: MaterializedDataset[T]
  ): Either[ExecutionError, MaterializedDataset[T]] = {
    val leftRows = left.toVectorUnsafe
    val rightSet = right.toVectorUnsafe.toSet
    val diff = leftRows.filterNot(rightSet.contains).distinct

    MaterializedDataset.fromVector(diff)(using left.schema)
  }

  /** Zip with sequential indices. */
  private def zipWithIndex[T](
    dataset: MaterializedDataset[T]
  ): Either[ExecutionError, MaterializedDataset[(T, Long)]] = {
    val rows = dataset.toVectorUnsafe
    val indexed = rows.zipWithIndex.map { case (value, idx) => (value, idx.toLong) }

    // Need tuple schema
    given Schema[(T, Long)] = Schema.tuple2Schema[T, Long](using dataset.schema, Schema.longSchema)

    MaterializedDataset.fromVector(indexed)
  }

  /** Inner join - only matching rows. */
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

  /** Left outer join - all left rows, with matching right rows or None. */
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

  /** Right outer join - all right rows, with matching left rows or None. */
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

  /** Full outer join - all rows from both sides, with None where no match. */
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

  /** Left anti join - left rows with no match in right. */
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

  // --- Expression-based joins using hash join ---

  /** Build a hash index: key value → list of row indices. */
  private def buildKeyIndex(keyCol: Column, rowCount: Int): scala.collection.mutable.HashMap[Any, Vector[Int]] = {
    val index = scala.collection.mutable.HashMap.empty[Any, Vector[Int]]
    var i = 0
    while (i < rowCount) {
      val key = keyCol.getValue(i)
      index.updateWith(key) {
        case Some(existing) => Some(existing :+ i)
        case None => Some(Vector(i))
      }
      i += 1
    }
    index
  }

  /** Assemble result columns from matched left/right index pairs. */
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

  /** Expression-based inner join using hash join.
    *
    * Evaluates key expressions to columns, builds hash index on left keys, probes with right keys.
    * O(N + M) instead of O(N * M) for the common equi-join case.
    */
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

      var ri = 0
      while (ri < right.rowCount) {
        val rKey = rightKeyCol.getValue(ri)
        leftIndex.get(rKey).foreach { leftRows =>
          leftRows.foreach { li =>
            leftIdxBuf += li
            rightIdxBuf += ri
          }
        }
        ri += 1
      }

      val (cols, schema) = assembleJoinColumns(left, right, leftIdxBuf.toArray, rightIdxBuf.toArray)
      MaterializedDataset(cols, schema)
    }
  }

  /** Expression-based left join using hash join. */
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

  /** Expression-based right join using hash join. */
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

  /** Expression-based full join using hash join. */
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

  /** Expression-based anti join using hash join. */
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
      // Build set of all right key values
      val rightKeys = scala.collection.mutable.HashSet.empty[Any]
      var ri = 0
      while (ri < right.rowCount) {
        rightKeys += rightKeyCol.getValue(ri)
        ri += 1
      }

      // Filter left rows whose key is not in right keys
      val indices = (0 until left.rowCount).filter { li =>
        !rightKeys.contains(leftKeyCol.getValue(li))
      }.toArray

      val newColumns = left.columns.map(_.slice(indices))
      MaterializedDataset(newColumns, left.schema)
    }
  }
}
