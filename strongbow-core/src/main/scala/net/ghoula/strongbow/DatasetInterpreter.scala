package net.ghoula.strongbow

import net.ghoula.strongbow.errors.{DecodeError, ExecutionError}
import net.ghoula.strongbow.types.RowIndex

/** Main interpreter for Dataset execution.
  *
  * Walks the Dataset AST and produces MaterializedDataset results. Delegates to ExprInterpreter for
  * expression evaluation and uses columnar operations for efficiency.
  */
object DatasetInterpreter {

  /** Execute a Dataset plan to produce a MaterializedDataset. */
  def execute[T](dataset: Dataset[T]): Either[ExecutionError, MaterializedDataset[T]] = {
    (dataset: @unchecked) match {
      case root: Dataset.Root[T] =>
        Right(MaterializedDataset(root.columns, root.schema))

      case filt: Dataset.Filter[T] =>
        execute(filt.parent).map(parent => filter(parent, filt.predicate))

      case _: Dataset.Map[?, T] =>
        Left(ExecutionError.UnsupportedOperation(
          "Map requires explicit schema - use select or provide Schema[U]"
        ))

      case _: Dataset.FlatMap[?, T] =>
        Left(ExecutionError.UnsupportedOperation(
          "FlatMap requires explicit schema - use select or provide Schema[U]"
        ))

      case _: Dataset.Select[?, T] =>
        Left(ExecutionError.UnsupportedOperation(
          "Select requires explicit schema - provide Schema[U]"
        ))

      case selectExprs: Dataset.SelectExprs[T] =>
        execute(selectExprs.parent).flatMap(parent => selectExpressions(parent, selectExprs.exprs))

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

      case srt: Dataset.Sort[T] =>
        execute(srt.parent).flatMap(parent => sort(parent, srt.ordering))

      case srtBy: Dataset.SortBy[T, _] =>
        execute(srtBy.parent).flatMap(parent => sortBy(parent, srtBy.key, srtBy.ordering))

      case grp: Dataset.GroupedToPairs[k, v] =>
        val pairs: Vector[(k, v)] = GroupByInterpreter.execute(grp.grouped)

        // Use tuple2Schema given instance with captured evidence
        given Schema[k] = grp.schemaK
        given Schema[v] = grp.schemaV
        val tupleSchema: Schema[(k, v)] = summon[Schema[(k, v)]]

        MaterializedDataset.fromVector(pairs)(using tupleSchema.asInstanceOf[Schema[T]]) // scalafix:ok DisableSyntax.asInstanceOf

      case keys: Dataset.GroupedKeys[k, v] =>
        val pairs: Vector[(k, v)] = GroupByInterpreter.execute(keys.grouped)
        val keyValues: Vector[k] = pairs.map(_._1)

        MaterializedDataset.fromVector(keyValues)(using keys.schemaK.asInstanceOf[Schema[T]]) // scalafix:ok DisableSyntax.asInstanceOf

      case values: Dataset.GroupedValues[k, v] =>
        val pairs: Vector[(k, v)] = GroupByInterpreter.execute(values.grouped)
        val valueValues: Vector[v] = pairs.map(_._2)

        MaterializedDataset.fromVector(valueValues)(using values.schemaV.asInstanceOf[Schema[T]]) // scalafix:ok DisableSyntax.asInstanceOf
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

    dataset.columns.foldLeft[Either[ExecutionError, Vector[Column]]](Right(Vector.empty)) { (acc, col) =>
      acc.flatMap { cols =>
        Column.fromValues(
          rowIndices.map(idx => col.getValue(idx)).toVector,
          col.columnType
        ).map(cols :+ _)
      }
    }.map(cols => MaterializedDataset(cols, dataset.schema))
  }

  /** Limit result to first n rows. */
  private def limit[T](dataset: MaterializedDataset[T], n: Int): Either[ExecutionError, MaterializedDataset[T]] = {
    if (n >= dataset.rowCount) {
      Right(dataset)
    } else {
      dataset.columns.foldLeft[Either[ExecutionError, Vector[Column]]](Right(Vector.empty)) { (acc, col) =>
        acc.flatMap { cols =>
          Column.fromValues(
            (0 until n).map(idx => col.getValue(idx)).toVector,
            col.columnType
          ).map(cols :+ _)
        }
      }.map(cols => MaterializedDataset(cols, dataset.schema))
    }
  }

  /** Union two datasets with the same schema. */
  private def union[T](
    left: MaterializedDataset[T],
    right: MaterializedDataset[T]
  ): Either[ExecutionError, MaterializedDataset[T]] = {
    if (left.columnCount != right.columnCount) {
      Left(ExecutionError.PreconditionViolation(
        "Cannot union datasets with different column counts"
      ))
    } else {
      left.columns.zip(right.columns).foldLeft[Either[ExecutionError, Vector[Column]]](Right(Vector.empty)) {
        case (acc, (leftCol, rightCol)) =>
          acc.flatMap { cols =>
            if (leftCol.columnType != rightCol.columnType) {
              Left(ExecutionError.PreconditionViolation(
                s"Cannot union columns with different types: ${leftCol.columnType} vs ${rightCol.columnType}"
              ))
            } else {
              val leftValues = (0 until leftCol.length).map(leftCol.getValue)
              val rightValues = (0 until rightCol.length).map(rightCol.getValue)

              Column.fromValues((leftValues ++ rightValues).toVector, leftCol.columnType).map(cols :+ _)
            }
          }
      }.map(cols => MaterializedDataset(cols, left.schema))
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
  private def selectExpressions[T](
    dataset: MaterializedDataset[T],
    exprs: Vector[(String, Expr[T, Any], ColumnType)]
  ): Either[ExecutionError, MaterializedDataset[T]] = {
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

    // Create schema with new column names and types
    val newSchema = new Schema[T] {
      def columnCount: Int = exprs.length
      def columnNames: Vector[String] = exprs.map(_._1)
      def columnTypes: Vector[ColumnType] = exprs.map(_._3)
      def encode(value: T): Vector[Any] = dataset.schema.encode(value)
      def decode(values: Vector[Any]): Either[DecodeError, T] = dataset.schema.decode(values)
    }

    newColumnsOrError.map(cols => MaterializedDataset(cols, newSchema))
  }
}
