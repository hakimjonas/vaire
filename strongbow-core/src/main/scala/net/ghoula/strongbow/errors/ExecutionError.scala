package net.ghoula.strongbow.errors

/** Errors that occur during dataset plan execution.
  */
enum ExecutionError {
  case DivisionByZero(row: Int)
  case IndexOutOfBounds(index: Int, size: Int)
  case UnsupportedOperation(operation: String)
  case UnsupportedExpression(expression: String)
  case InvalidPlan(message: String)
  case ResourceExhausted(resource: String)
}
