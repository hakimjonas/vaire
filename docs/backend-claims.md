# Backend capability claims

Status: current
Audience: user.

Vairë's two backends do not provide the same set of expressions and dataset transformations.
This table names the constructs one backend rejects and the other evaluates. The authoritative
list of rejection messages, with their class and the reason for each excluded message, is
[`backend-claims.json`](../backend-claims.json).

## Rejected constructs

| Construct | In-memory | Spark |
|---|---|---|
| `Between` on an untyped column | rejects | evaluates |
| Comparison on an untyped column | rejects | evaluates |
| `Explode` in a column expression | requires the Dataset-level form | evaluates |
| `ArraySort` on an untyped column | rejects | evaluates |
| `VariantExplode` in a column expression | requires the Dataset-level form | evaluates |
| `Posexplode`, `ExplodeOuter`, `Inline` | require the Dataset-level form | evaluate |
| Aggregations in columnar `evalColumn` | reject; aggregates run through `evalAggregation` | evaluate |
| Window functions in columnar `evalColumn` | reject; windows run through the window path | evaluate |
| Datasketches and `current_path` | rejects | evaluates |
| AES encrypt/decrypt | rejects | evaluates |
| `HistogramNumeric` | rejects | evaluates |
| `Grouping` | rejects; requires a grouping-sets context | evaluates |
| `GroupingId` | rejects; requires a grouping-sets context | evaluates |
| Eight `TryXpath*` variants | evaluates | rejects |
| `json_tuple` key mixing a quote with a dot or bracket | evaluates | rejects the conversion |
| `schema_of_json` on a non-constant input | evaluates | rejects |
| Some window forms in a Dataset window (for example `NTile`) | evaluates | rejects |
