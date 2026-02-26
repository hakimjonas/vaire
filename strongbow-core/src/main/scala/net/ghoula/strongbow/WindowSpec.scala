package net.ghoula.strongbow

import net.ghoula.strongbow.specs.{KeySpec, SortSpec}

/** Specification for window function partitioning and ordering.
  *
  * Used by `Dataset.WithWindow` to define how rows are partitioned into groups and ordered within
  * each partition for window function evaluation.
  */
case class WindowSpec[T](
  partitionBy: Vector[KeySpec[T]],
  orderBy: Vector[SortSpec[T]]
) derives CanEqual
