package net.ghoula.strongbow.errors

/** Errors that occur during dataset plan execution.
  */
enum ExecutionError {

  /** A per-row division by zero, carrying the failing row index. */
  case DivisionByZero(row: Int)

  /** An element access outside the array's bounds. */
  case IndexOutOfBounds(index: Int, size: Int)

  /** An operation the executing backend does not support. */
  case UnsupportedOperation(operation: String)

  /** An expression the executing backend cannot compile. */
  case UnsupportedExpression(expression: String)

  /** A plan that cannot be executed as constructed. */
  case InvalidPlan(message: String)

  /** A resource limit was exceeded during execution. */
  case ResourceExhausted(resource: String)

  /** A column of the wrong type reached an operation, with context. */
  case TypeMismatch(expected: String, actual: String, context: String)

  /** A row-level data error with a log-safe message (no input data). */
  case InvalidValue(message: String)

  /** A structural pre-condition of the plan was violated. */
  case PreconditionViolation(message: String)
}
