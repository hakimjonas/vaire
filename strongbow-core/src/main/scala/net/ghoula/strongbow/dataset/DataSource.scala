package net.ghoula.strongbow.dataset

import net.ghoula.strongbow.column.Column

/** Capability trait representing a physical data source.
  *
  * Interpreters pattern match on concrete implementations to access data in their native format.
  * The trait is open — backend modules extend it without modifying the core.
  */
trait DataSource

/** In-memory columnar storage. The default source for Dataset.fromColumns. */
case class InMemorySource(columns: Vector[Column[?]]) extends DataSource
