package net.ghoula.strongbow

/** Single canonical import for the Strongbow public API.
  *
  * @example
  *   {{{
  * import net.ghoula.strongbow.prelude.*
  *
  * val plan: Dataset[Int] = Dataset.fromColumns(cols, schema) match {
  *   case Right(ds) => ds.filter(Expr.cell("value", ColumnIndex(0)) > Expr.const(18))
  *   case Left(errors) => ???
  * }
  *   }}}
  */
object prelude {
  export net.ghoula.strongbow.column.{Column, ColumnType}
  export net.ghoula.strongbow.dataset.{
    Dataset,
    DatasetActions,
    DatasetExplainer,
    DataSource,
    InMemorySource,
    MaterializedDataset
  }
  export net.ghoula.strongbow.dataset.DatasetActions.*
  export net.ghoula.strongbow.expr.Expr
  export net.ghoula.strongbow.interpreter.{DatasetInterpreter, ExprInterpreter, Interpreter}
  export net.ghoula.strongbow.Schema
  export net.ghoula.strongbow.types.{ColumnIndex, Date, Decimal, RowIndex}
  export net.ghoula.strongbow.errors.{SchemaError, DecodeError, ExecutionError, NonEmptyList}
  export net.ghoula.strongbow.params.{AggSpec, KeySpec, SortSpec, WindowExprSpec, WindowSpec}
  export net.ghoula.strongbow.internal.ExprCompiler
  export net.ghoula.strongbow.internal.{where, sortByColumn, withFields, project}
  export net.ghoula.strongbow.agg.AggBuilders as agg
  export net.ghoula.strongbow.agg.as
}
