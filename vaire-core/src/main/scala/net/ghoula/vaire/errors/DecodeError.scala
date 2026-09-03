package net.ghoula.vaire.errors

/** Errors that occur during decoding from columnar format to typed values.
  */
enum DecodeError {

  /** Expected a one-value encoding but found a different arity. */
  case WrongArity(expected: Int, actual: Int)

  /** A value could not be decoded to the target type. */
  case DecodeFailed(message: String)

  /** The stored value's runtime type does not match the target type. */
  case TypeMismatch(expected: String, actual: String)

  /** A SQL NULL was decoded into a non-nullable target. */
  case NullValue(columnIndex: Int)
}
