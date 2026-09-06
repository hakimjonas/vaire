package net.ghoula.vaire

/** Single canonical import for the Vairë public API.
  *
  * @example
  *   {{{
  * import net.ghoula.vaire.prelude.*
  *
  * val plan: Dataset[Int] = Dataset.fromColumns(cols, schema) match {
  *   case Right(ds) => ds.filter(Expr.cell("value", ColumnIndex(0)) > Expr.const(18))
  *   case Left(errors) => ???
  * }
  *   }}}
  */
object prelude {
  export net.ghoula.vaire.column.{Column, ColumnType}
  export net.ghoula.vaire.dataset.{
    Dataset,
    DatasetActions,
    DatasetExplainer,
    DataSource,
    InMemorySource,
    MaterializedDataset
  }
  export net.ghoula.vaire.dataset.DatasetActions.*
  export net.ghoula.vaire.expr.Expr
  export net.ghoula.vaire.interpreter.{DatasetInterpreter, ExprInterpreter, Interpreter}
  export net.ghoula.vaire.Schema
  export net.ghoula.vaire.types.{ColumnIndex, Date, Decimal, RowIndex, Time}
  export net.ghoula.vaire.errors.{SchemaError, DecodeError, ExecutionError, NonEmptyList}
  export net.ghoula.vaire.params.{AggSpec, KeySpec, SortSpec, WindowExprSpec, WindowSpec}
  export net.ghoula.vaire.internal.ExprCompiler
  export net.ghoula.vaire.internal.{where, sortByColumn, withFields, project}
  export net.ghoula.vaire.agg.AggBuilders as agg
  export net.ghoula.vaire.agg.as
}
