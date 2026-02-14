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

      case srt: Dataset.Sort[T] =>
        execute(srt.parent).flatMap(parent => sort(parent, srt.ordering))

      case srtBy: Dataset.SortBy[T, _] =>
        execute(srtBy.parent).flatMap(parent => sortBy(parent, srtBy.key, srtBy.ordering))

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

  /** Filter rows based on predicate expression. */
  private def filter[T](
    dataset: MaterializedDataset[T],
    predicate: Expr[T, Boolean]
  ): MaterializedDataset[T] = {
    // Convert to Array for 2x better slice performance (benchmarked at 500K elements)
    val rowIndices = (0 until dataset.rowCount).filter { rowIdx =>
      ExprInterpreter.eval(predicate, dataset.columns, RowIndex(rowIdx)) match {
        case Right(true) => true
        case _ => false
      }
    }.toArray

    // Use type-specialized slice instead of getValue + fromValues
    // This avoids boxing, intermediate Vector[Any], and multiple traversals
    val newColumns = dataset.columns.map(_.slice(rowIndices))

    MaterializedDataset(newColumns, dataset.schema)
  }

  /** Remove duplicate rows. */
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
    }

    dataset.columns
      .foldLeft[Either[ExecutionError, Vector[Column]]](Right(Vector.empty)) { (acc, col) =>
        acc.flatMap { cols =>
          Column
            .fromValues(
              rowIndices.map(idx => col.getValue(idx)).toVector,
              col.columnType
            )
            .map(cols :+ _)
        }
      }
      .map(cols => MaterializedDataset(cols, dataset.schema))
  }

  /** Limit result to first n rows. */
  private def limit[T](dataset: MaterializedDataset[T], n: Int): Either[ExecutionError, MaterializedDataset[T]] = {
    if (n >= dataset.rowCount) {
      Right(dataset)
    } else {
      dataset.columns
        .foldLeft[Either[ExecutionError, Vector[Column]]](Right(Vector.empty)) { (acc, col) =>
          acc.flatMap { cols =>
            Column
              .fromValues(
                (0 until n).map(idx => col.getValue(idx)).toVector,
                col.columnType
              )
              .map(cols :+ _)
          }
        }
        .map(cols => MaterializedDataset(cols, dataset.schema))
    }
  }

  /** Union two datasets with the same schema. */
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
            if (leftCol.columnType != rightCol.columnType) {
              Left(
                ExecutionError.PreconditionViolation(
                  s"Cannot union columns with different types: ${leftCol.columnType} vs ${rightCol.columnType}"
                )
              )
            } else {
              val leftValues = (0 until leftCol.length).map(leftCol.getValue)
              val rightValues = (0 until rightCol.length).map(rightCol.getValue)

              Column.fromValues((leftValues ++ rightValues).toVector, leftCol.columnType).map(cols :+ _)
            }
          }
        }
        .map(cols => MaterializedDataset(cols, left.schema))
    }
  }

  /** Sort dataset using ordering. */
  private def sort[T](
    dataset: MaterializedDataset[T],
    ord: Ordering[T]
  ): Either[ExecutionError, MaterializedDataset[T]] = {
    val rows = dataset.toVectorUnsafe
    val sortedRows = rows.sorted(using ord)

    MaterializedDataset.fromVector(sortedRows)(using dataset.schema)
  }

  /** Sort dataset by key function. */
  private def sortBy[T, K](
    dataset: MaterializedDataset[T],
    key: T => K,
    ord: Ordering[K]
  ): Either[ExecutionError, MaterializedDataset[T]] = {
    val rows = dataset.toVectorUnsafe
    val sortedRows = rows.sortBy(key)(using ord)

    MaterializedDataset.fromVector(sortedRows)(using dataset.schema)
  }

  /** Select columns by evaluating expressions. */
  private def selectExpressions[In, Out](
    dataset: MaterializedDataset[In],
    exprs: Vector[(String, Expr[In, Any], ColumnType)],
    outputSchema: Schema[Out]
  ): Either[ExecutionError, MaterializedDataset[Out]] = {
    val rowCount = dataset.rowCount

    // Evaluate each expression to create new columns
    val newColumnsOrError = exprs.foldLeft[Either[ExecutionError, Vector[Column]]](Right(Vector.empty)) {
      case (acc, (_, expr, columnType)) =>
        acc.flatMap { cols =>
          val valuesOrError = (0 until rowCount).foldLeft[Either[ExecutionError, Vector[Any]]](Right(Vector.empty)) {
            (accValues, rowIdx) =>
              accValues.flatMap { values =>
                ExprInterpreter.eval(expr, dataset.columns, RowIndex(rowIdx)) match {
                  case Right(value) => Right(values :+ value)
                  case Left(err) =>
                    Left(ExecutionError.InvalidValue(s"Expression evaluation failed: $err"))
                }
              }
          }

          valuesOrError.flatMap(values => Column.fromValues(values, columnType).map(cols :+ _))
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
}
