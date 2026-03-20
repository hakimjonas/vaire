package net.ghoula.strongbow

/** Formats Dataset AST as human-readable string. */
object DatasetExplainer {

  def explain[T](dataset: Dataset[T], indent: Int = 0): String = {
    val prefix = "  " * indent
    val node = dataset match {
      case Dataset.Root(columns, schema) =>
        s"Root[${columns.length} columns, ${columns.headOption.map(_.length).getOrElse(0)} rows]"

      case Dataset.Filter(parent, predicate) =>
        s"Filter($predicate)\n${explain(parent, indent + 1)}"

      case Dataset.Map(parent, _, _) =>
        s"Map[func]\n${explain(parent, indent + 1)}"

      case Dataset.FlatMap(parent, _, _) =>
        s"FlatMap[func]\n${explain(parent, indent + 1)}"

      case Dataset.Select(parent, _, _) =>
        s"Select[projection]\n${explain(parent, indent + 1)}"

      case Dataset.SelectExprs(parent, exprs, _) =>
        s"SelectExprs[${exprs.map(_._1).mkString(", ")}]\n${explain(parent, indent + 1)}"

      case Dataset.Distinct(parent) =>
        s"Distinct\n${explain(parent, indent + 1)}"

      case Dataset.Limit(parent, n) =>
        s"Limit($n)\n${explain(parent, indent + 1)}"

      case Dataset.Union(left, right) =>
        s"Union\n${explain(left, indent + 1)}\n${explain(right, indent + 1)}"

      case Dataset.Intersect(left, right) =>
        s"Intersect\n${explain(left, indent + 1)}\n${explain(right, indent + 1)}"

      case Dataset.Except(left, right) =>
        s"Except\n${explain(left, indent + 1)}\n${explain(right, indent + 1)}"

      case Dataset.InnerJoin(left, right, _) =>
        s"InnerJoin\n${explain(left, indent + 1)}\n${explain(right, indent + 1)}"

      case Dataset.LeftJoin(left, right, _) =>
        s"LeftJoin\n${explain(left, indent + 1)}\n${explain(right, indent + 1)}"

      case Dataset.RightJoin(left, right, _) =>
        s"RightJoin\n${explain(left, indent + 1)}\n${explain(right, indent + 1)}"

      case Dataset.FullJoin(left, right, _) =>
        s"FullJoin\n${explain(left, indent + 1)}\n${explain(right, indent + 1)}"

      case Dataset.LeftAntiJoin(left, right, _) =>
        s"LeftAntiJoin\n${explain(left, indent + 1)}\n${explain(right, indent + 1)}"

      case Dataset.InnerJoinOn(left, right, _, _, _, _) =>
        s"InnerJoinOn[expr]\n${explain(left, indent + 1)}\n${explain(right, indent + 1)}"

      case Dataset.LeftJoinOn(left, right, _, _, _, _) =>
        s"LeftJoinOn[expr]\n${explain(left, indent + 1)}\n${explain(right, indent + 1)}"

      case Dataset.RightJoinOn(left, right, _, _, _, _) =>
        s"RightJoinOn[expr]\n${explain(left, indent + 1)}\n${explain(right, indent + 1)}"

      case Dataset.FullJoinOn(left, right, _, _, _, _) =>
        s"FullJoinOn[expr]\n${explain(left, indent + 1)}\n${explain(right, indent + 1)}"

      case Dataset.LeftAntiJoinOn(left, right, _, _, _, _) =>
        s"LeftAntiJoinOn[expr]\n${explain(left, indent + 1)}\n${explain(right, indent + 1)}"

      case Dataset.Sort(parent, _) =>
        s"Sort[ordering]\n${explain(parent, indent + 1)}"

      case Dataset.SortBy(parent, _, _) =>
        s"SortBy[key]\n${explain(parent, indent + 1)}"

      case Dataset.SortByExpr(parent, _, _, _) =>
        s"SortByExpr[expr]\n${explain(parent, indent + 1)}"

      case Dataset.Sample(parent, fraction, seed, withReplacement) =>
        s"Sample(fraction=$fraction, seed=$seed, withReplacement=$withReplacement)\n${explain(parent, indent + 1)}"

      case Dataset.ZipWithIndex(parent) =>
        s"ZipWithIndex\n${explain(parent, indent + 1)}"

      case Dataset.ZipWithUniqueId(parent) =>
        s"ZipWithUniqueId\n${explain(parent, indent + 1)}"

      case Dataset.Persist(parent) =>
        s"Persist\n${explain(parent, indent + 1)}"

      case Dataset.Checkpoint(parent) =>
        s"Checkpoint\n${explain(parent, indent + 1)}"

      case Dataset.Rebalance(parent, numPartitions) =>
        s"Rebalance(${numPartitions.getOrElse("auto")})\n${explain(parent, indent + 1)}"

      case Dataset.GroupByAgg(parent, keySpecs, aggSpecs, _) =>
        val keys = keySpecs.map(_.name).mkString(", ")
        val aggs = aggSpecs.map(_.name).mkString(", ")
        s"GroupByAgg[keys=($keys), aggs=($aggs)]\n${explain(parent, indent + 1)}"

      case Dataset.SortByExprs(parent, sortKeys) =>
        val dirs = sortKeys.map(spec => if (spec.ascending) "ASC" else "DESC").mkString(", ")
        s"SortByExprs[$dirs]\n${explain(parent, indent + 1)}"

      case Dataset.LeftSemiJoinOn(left, right, _, _, _, _) =>
        s"LeftSemiJoinOn[expr]\n${explain(left, indent + 1)}\n${explain(right, indent + 1)}"

      case Dataset.WithWindow(parent, windowExprs, _, _) =>
        val cols = windowExprs.map(_.name).mkString(", ")
        s"WithWindow[$cols]\n${explain(parent, indent + 1)}"

      case Dataset.Aggregate(parent, aggSpecs, _) =>
        val aggs = aggSpecs.map(_.name).mkString(", ")
        s"Aggregate[aggs=($aggs)]\n${explain(parent, indent + 1)}"
    }
    s"$prefix$node"
  }

}
