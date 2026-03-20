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
  export net.ghoula.strongbow.{Dataset, Expr, Column, ColumnType, Schema}
  export net.ghoula.strongbow.MaterializedDataset
  export net.ghoula.strongbow.{Interpreter, DatasetInterpreter, ExprInterpreter}
  export net.ghoula.strongbow.types.{ColumnIndex, Date, RowIndex}
  export net.ghoula.strongbow.errors.{SchemaError, DecodeError, ExecutionError, NonEmptyList}
  export net.ghoula.strongbow.DatasetActions.*
  export net.ghoula.strongbow.specs.{SortSpec, KeySpec, AggSpec, WindowExprSpec}
  export net.ghoula.strongbow.DatasetExplainer
  export net.ghoula.strongbow.ExprMacro
  export net.ghoula.strongbow.agg.AggBuilders as agg
  export net.ghoula.strongbow.agg.as
}
