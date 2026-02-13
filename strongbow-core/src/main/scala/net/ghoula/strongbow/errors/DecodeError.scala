package net.ghoula.strongbow.errors

/** Errors that occur during decoding from columnar format to typed values.
  */
enum DecodeError {
  case WrongArity(expected: Int, actual: Int)
  case DecodeFailed(message: String)
  case TypeMismatch(expected: String, actual: String)
  case NullValue(columnIndex: Int)
}
