package net.ghoula.strongbow

import net.ghoula.strongbow.types.RowIndex
import net.ghoula.strongbow.errors.DecodeError

/** Main interpreter for Dataset execution.
  *
  * Walks the Dataset AST and produces MaterializedDataset results. Delegates to ExprInterpreter
  * for expression evaluation and uses columnar operations for efficiency.
  */
object DatasetInterpreter {

  /** Execute a Dataset plan to produce a MaterializedDataset. */
  def execute[T](dataset: Dataset[T]): MaterializedDataset[T] = {
    (dataset: @unchecked) match {
      case root: Dataset.Root[T] =>
        MaterializedDataset(root.columns, root.schema)

      case filt: Dataset.Filter[T] =>
        val parentResult = execute(filt.parent)
        filter(parentResult, filt.predicate)

      case _: Dataset.Map[?, T] =>
        // We can't easily implement map without knowing the target schema
        // This requires schema inference or explicit schema passing
        throw new UnsupportedOperationException(
          "Map requires explicit schema - use select or provide Schema[U]"
        )

      case _: Dataset.FlatMap[?, T] =>
        throw new UnsupportedOperationException(
          "FlatMap requires explicit schema - use select or provide Schema[U]"
        )

      case _: Dataset.Select[?, T] =>
        throw new UnsupportedOperationException(
          "Select requires explicit schema - provide Schema[U]"
        )

      case selectExprs: Dataset.SelectExprs[T] =>
        val parentResult = execute(selectExprs.parent)
        selectExpressions(parentResult, selectExprs.exprs)

      case dist: Dataset.Distinct[T] =>
        val parentResult = execute(dist.parent)
        distinct(parentResult)

      case lim: Dataset.Limit[T] =>
        val parentResult = execute(lim.parent)
        limit(parentResult, lim.n)

      case un: Dataset.Union[T] =>
        val leftResult = execute(un.left)
        val rightResult = execute(un.right)
        union(leftResult, rightResult)

      case srt: Dataset.Sort[T] =>
        val parentResult = execute(srt.parent)
        sort(parentResult, srt.ordering)

      case srtBy: Dataset.SortBy[T, _] =>
        val parentResult = execute(srtBy.parent)
        sortBy(parentResult, srtBy.key, srtBy.ordering)

      case grp: Dataset.GroupedToPairs[k, v] =>
        // Execute Grouped to get (K, V) pairs
        val pairs: Vector[(k, v)] = GroupByInterpreter.execute(grp.grouped)

        // For now, create a simple tuple schema
        // TODO: This requires a proper Schema[(K, V)] implementation
        val tupleSchema = new Schema[(k, v)] {
          def columnCount: Int = 2
          def columnNames: Vector[String] = Vector("_1", "_2")
          def columnTypes: Vector[ColumnType] = Vector(ColumnType.AnyType, ColumnType.AnyType)
          def encode(value: (k, v)): Vector[Any] = Vector(value._1, value._2)
          def decode(values: Vector[Any]): Either[DecodeError, (k, v)] = {
            if (values.length != 2) {
              Left(DecodeError.WrongArity(2, values.length))
            } else {
              Right((values(0).asInstanceOf[k], values(1).asInstanceOf[v]))
            }
          }
        }

        MaterializedDataset.fromVector(pairs)(using tupleSchema.asInstanceOf[Schema[T]])

      case keys: Dataset.GroupedKeys[k, v] =>
        // Execute Grouped and extract keys
        val pairs: Vector[(k, v)] = GroupByInterpreter.execute(keys.grouped)
        val keyValues: Vector[k] = pairs.map(_._1)

        // Create a simple schema for keys
        val keySchema = new Schema[k] {
          def columnCount: Int = 1
          def columnNames: Vector[String] = Vector("key")
          def columnTypes: Vector[ColumnType] = Vector(ColumnType.AnyType)
          def encode(value: k): Vector[Any] = Vector(value)
          def decode(values: Vector[Any]): Either[DecodeError, k] = {
            if (values.length != 1) {
              Left(DecodeError.WrongArity(1, values.length))
            } else {
              Right(values(0).asInstanceOf[k])
            }
          }
        }

        MaterializedDataset.fromVector(keyValues)(using keySchema.asInstanceOf[Schema[T]])

      case values: Dataset.GroupedValues[k, v] =>
        // Execute Grouped and extract values
        val pairs: Vector[(k, v)] = GroupByInterpreter.execute(values.grouped)
        val valueValues: Vector[v] = pairs.map(_._2)

        // Create a simple schema for values
        val valueSchema = new Schema[v] {
          def columnCount: Int = 1
          def columnNames: Vector[String] = Vector("value")
          def columnTypes: Vector[ColumnType] = Vector(ColumnType.AnyType)
          def encode(value: v): Vector[Any] = Vector(value)
          def decode(values: Vector[Any]): Either[DecodeError, v] = {
            if (values.length != 1) {
              Left(DecodeError.WrongArity(1, values.length))
            } else {
              Right(values(0).asInstanceOf[v])
            }
          }
        }

        MaterializedDataset.fromVector(valueValues)(using valueSchema.asInstanceOf[Schema[T]])
    }
  }

  /** Filter rows based on predicate expression. */
  private def filter[T](
      dataset: MaterializedDataset[T],
      predicate: Expr[T, Boolean]
  ): MaterializedDataset[T] = {
    val rowIndices = (0 until dataset.rowCount).filter { rowIdx =>
      ExprInterpreter.eval(predicate, dataset.columns, RowIndex(rowIdx)) match {
        case Right(true) => true
        case _           => false
      }
    }

    // Use type-specialized slice instead of getValue + fromValues
    // This avoids boxing, intermediate Vector[Any], and multiple traversals
    val newColumns = dataset.columns.map(_.slice(rowIndices))

    MaterializedDataset(newColumns, dataset.schema)
  }

  /** Remove duplicate rows. */
  private def distinct[T](dataset: MaterializedDataset[T]): MaterializedDataset[T] = {
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

    val newColumns = dataset.columns.map { col =>
      Column.fromValues(
        rowIndices.map(idx => col.getValue(idx)).toVector,
        col.columnType
      )
    }

    MaterializedDataset(newColumns, dataset.schema)
  }

  /** Limit result to first n rows. */
  private def limit[T](dataset: MaterializedDataset[T], n: Int): MaterializedDataset[T] = {
    if (n >= dataset.rowCount) {
      dataset
    } else {
      val newColumns = dataset.columns.map { col =>
        Column.fromValues(
          (0 until n).map(idx => col.getValue(idx)).toVector,
          col.columnType
        )
      }

      MaterializedDataset(newColumns, dataset.schema)
    }
  }

  /** Union two datasets with the same schema. */
  private def union[T](
      left: MaterializedDataset[T],
      right: MaterializedDataset[T]
  ): MaterializedDataset[T] = {
    require(
      left.columnCount == right.columnCount,
      "Cannot union datasets with different column counts"
    )

    val newColumns = left.columns.zip(right.columns).map { case (leftCol, rightCol) =>
      require(
        leftCol.columnType == rightCol.columnType,
        s"Cannot union columns with different types: ${leftCol.columnType} vs ${rightCol.columnType}"
      )

      val leftValues = (0 until leftCol.length).map(leftCol.getValue)
      val rightValues = (0 until rightCol.length).map(rightCol.getValue)

      Column.fromValues((leftValues ++ rightValues).toVector, leftCol.columnType)
    }

    MaterializedDataset(newColumns, left.schema)
  }

  /** Sort dataset using ordering. */
  private def sort[T](
      dataset: MaterializedDataset[T],
      ord: Ordering[T]
  ): MaterializedDataset[T] = {
    val rows = dataset.toVectorUnsafe
    val sortedRows = rows.sorted(using ord)

    MaterializedDataset.fromVector(sortedRows)(using dataset.schema)
  }

  /** Sort dataset by key function. */
  private def sortBy[T, K](
      dataset: MaterializedDataset[T],
      key: T => K,
      ord: Ordering[K]
  ): MaterializedDataset[T] = {
    val rows = dataset.toVectorUnsafe
    val sortedRows = rows.sortBy(key)(using ord)

    MaterializedDataset.fromVector(sortedRows)(using dataset.schema)
  }

  /** Select columns by evaluating expressions. */
  private def selectExpressions[T](
      dataset: MaterializedDataset[T],
      exprs: Vector[(String, Expr[T, Any], ColumnType)]
  ): MaterializedDataset[T] = {
    val rowCount = dataset.rowCount

    // Evaluate each expression to create new columns
    val newColumns = exprs.map { case (_, expr, columnType) =>
      val values = (0 until rowCount).map { rowIdx =>
        ExprInterpreter.eval(expr, dataset.columns, RowIndex(rowIdx)) match {
          case Right(value) => value
          case Left(err)    => throw new RuntimeException(s"Expression evaluation failed: $err")
        }
      }.toVector

      Column.fromValues(values, columnType)
    }

    // Create schema with new column names and types
    val newSchema = new Schema[T] {
      def columnCount: Int = exprs.length
      def columnNames: Vector[String] = exprs.map(_._1)
      def columnTypes: Vector[ColumnType] = exprs.map(_._3)
      def encode(value: T): Vector[Any] = dataset.schema.encode(value)
      def decode(values: Vector[Any]): Either[DecodeError, T] = dataset.schema.decode(values)
    }

    MaterializedDataset(newColumns, newSchema)
  }
}
