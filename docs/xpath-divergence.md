# XPath divergence between the in-memory engine and Spark

This is the single statement of how Vairë's `xpath*` family differs between the in-memory
interpreter and the Spark backend. The test suites and the README point here; they do not
restate the cases.

The affected expressions are the eight strict cases `Expr.Xpath`, `XpathString`,
`XpathBoolean`, `XpathShort`, `XpathInt`, `XpathLong`, `XpathFloat` and `XpathDouble`.
The eight `TryXpath*` cases are a separate capability gap, covered in
[TryXpath* open question](#tryxpath-open-question).

## The divergence

The document used for every case below:

```xml
<ns:r xmlns:ns="urn:x"><ns:b>1</ns:b><b>2</b></ns:r>
```

| Path | Vairë in-memory | Spark 4.2 | XPath 1.0 result |
|---|---|---|---|
| `r/b` | `""` | `"1"` | `""` |
| `//ns:b` | `"1"` | `""` | `"1"` |
| `//*:b` | `"1"` | throws `Prefix must resolve to a namespace: ` | not an XPath 1.0 expression |

The first two rows are the substantive divergence. XPath 1.0 treats an unprefixed name
test as a test against the null namespace, so `r` does not match the element whose name is
`ns:r`; the prefixed test `ns:b` expands `ns` through the in-scope declaration
`xmlns:ns="urn:x"` and matches the first child. Vairë's in-memory evaluator (Sarati)
implements those rules. Spark returns the opposite values because it evaluates over a
document parsed without namespace processing.

The third row is not a case of Spark returning a wrong value. `*:b` is the `*:local` name
test, which XPath 2.0 introduced; the XPath 1.0 grammar admits `*`, `prefix:*` and a QName,
not `*:local`. Sarati accepts the form and matches any namespace, so Vairë returns `"1"`.
Spark's evaluator rejects the expression during compilation, treating `*` as an undeclared
prefix:

```
Prefix must resolve to a namespace:
```

That message is evidence that Spark's XPath layer has no namespace model, but the
conformance question in this row is about an extension, not about a value XPath 1.0
defines.

### Which side to trust

Trust Vairë's in-memory result for `r/b` and `//ns:b`. Spark is wrong on both. Any document
that uses XML namespaces, whether through a prefix or a default declaration, is exposed to
the same defect: Spark's engine resolves name tests against a DOM that carries no namespace
information, so a prefixed test such as `//ns:b` selects nothing, while an unprefixed test
can match a prefixed element depending on the axis. The results do not follow from the
document's namespace declarations. XML from SOAP, SAML, Office formats, Maven POMs, SVG and
many configuration formats is namespaced, so the affected input class is ordinary, not
exotic.

The third row is an extension on Vairë's side. It is not a reason to prefer Spark: on the
same document Spark already returns wrong values for the first two rows.

## Mechanism

Confirmed on 2026-10-21 against spark-catalyst 4.2.0.

`org.apache.spark.sql.catalyst.expressions.xml.UDFXPathUtil` constructs
`DocumentBuilderFactory.newInstance()` and never calls `setNamespaceAware(true)`. The JAXP
default is `false`. `initializeDocumentBuilderFactory()` only disables external general and
parameter entities; it does not enable namespace processing. Every `xpath*` function reaches
this class through the evaluators in `XmlExpressionEvalUtils`.

A document parsed by that factory has elements with `getNodeName` equal to the qualified
name (`ns:r`) and `getLocalName` and `getNamespaceURI` both null. The JDK XPath engine
derives a local name from the qualified name for some operations (`local-name(/*)` returns
`r`), but its name tests behave differently by axis: `r/b` returns `"1"`, `//b` returns
`"2"` and `/descendant::b` returns `"2"` on the same document. A standalone program using
the default factory reproduces all five values from the table:

```java
DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
// dbf.isNamespaceAware() == false
Document d = dbf.newDocumentBuilder().parse(new InputSource(new StringReader(doc)));
XPath xp = XPathFactory.newInstance().newXPath();
xp.evaluate("r/b", d, XPathConstants.STRING);        // "1"
xp.evaluate("//ns:b", d, XPathConstants.STRING);      // ""
xp.evaluate("//*:b", d, XPathConstants.STRING);       // throws
```

With `dbf.setNamespaceAware(true)`, the same document parses with `localName=r` and
`namespaceURI=urn:x`, `r/b` returns `""` and `//ns:b` returns `"1"`.

The same probe also settles two values that earlier prose described incorrectly. On this
document Spark returns `"2"` for both `//b` and `/descendant::b`, and the in-memory engine
returns `"2"` for both. Those two paths are parity cases, not divergences; they are pinned
in `XpathDivergenceSpec` as controls.

What remains unconfirmed is intent, not behaviour: no Spark ticket or comment states
whether namespace support is in scope. The factory default is inherited from Hive's
implementation, which the class documents as its basis.

## Decision (taken)

The in-memory engine stays XPath 1.0-correct for unprefixed and prefixed name tests and is
not changed to reproduce Spark's results. Two reasons:

1. There is no Spark intent to be compatible with. The namespace-unaware parse is an
   unchosen default in a utility class inherited from Hive, not a documented semantic.
2. The in-memory evaluator is also the implementation behind Spark's own fallback paths.
   `SparkInterpreter.applyFilterViaMap` (`SparkInterpreter.scala:348`) and
   `SparkInterpreter.applySortByExprViaCollect` (`SparkInterpreter.scala:392`) call
   `ExprInterpreter.evalColumn` after collecting to the driver. Bending the evaluator to
   match Spark would make those paths wrong as well.

The `*:b` extension stays as it is. It is not part of the XPath 1.0 conformance claim and
is not a candidate for removal in this increment.

## TryXpath* open question

The eight `TryXpath*` cases are rejected on Spark; the rejection claim, its mechanism and its
test are recorded in [`backend-claims.md`](backend-claims.md). Whether that rejection stays is
open.

Not decided, and not implemented in this increment.

**Keep `UnsupportedOperation`.** The try family exists for per-row error handling, which is
an in-memory execution mode. A Spark implementation would have to collect the input to the
driver and evaluate row by row, giving up Catalyst and the distributed execution model. The
current error is explicit and fails fast instead of silently moving a potentially large
dataset through one JVM.

**Add a driver-collect fallback.** Spark already has the precedent:
`applyFilterViaMap` (`SparkInterpreter.scala:342`) and `applySortByExprViaCollect`
(`SparkInterpreter.scala:385`) collect, evaluate in memory, and rebuild a `DataFrame`. A
`TryXpath*` fallback would follow the same shape and turn a hard capability gap into a
visible performance cost, with semantics identical to the in-memory engine. The cost is a
new execution path, a changed public failure mode, and the risk of exhausting driver memory
on large inputs.

## Upstream status

No evidence that Spark has examined this behaviour, as of 2026-10-21.

- An all-time JIRA summary search for `xpath` returns eight tickets: the original function
  implementation (2016), Scala/Python bindings (2023), collations (May 2024), return-type
  nullness (August 2024), codegen (October 2024) and docstrings (January 2025). None
  concerns evaluation semantics, the XPath version, namespace handling, or the parser.
- A full-text search for `saxon` returns no tickets.
- A full-text search for `namespace aware` returns four tickets, none XPath-related.
- The three most recent substantive tickets, SPARK-48160, SPARK-50081 and SPARK-50764, have
  one or two comments each. None mentions the parser, the DOM, or namespaces. Those tickets
  do show continued activity in the XPath code, so the committers who review changes in this
  area are identifiable.

### Record for filing

File this as a defect with a reproducer, not as a feature request. A request to "support
XML namespaces" invites a scope and policy discussion and will stall; a wrong `xpath_string`
result on a five-line document is a bug with a pinned expectation.

Minimal reproducer:

```sql
SELECT xpath_string('<ns:r xmlns:ns="urn:x"><ns:b>1</ns:b><b>2</b></ns:r>', '//ns:b');
-- Spark 4.2 returns ""
-- XPath 1.0 requires "1"
```

```sql
SELECT xpath_string('<ns:r xmlns:ns="urn:x"><ns:b>1</ns:b><b>2</b></ns:r>', 'r/b');
-- Spark 4.2 returns "1"
-- XPath 1.0 requires ""
```

Pinned expectations on each backend are in the table above and asserted by
`vaire-spark/src/test/scala/net/ghoula/vaire/spark/XpathDivergenceSpec.scala`.

The ticket should cite the mechanism as confirmed by source inspection and standalone
reproduction: `UDFXPathUtil` never enables namespace processing, and the resulting DOM has
null `localName` and `namespaceURI` for every element. The one point to state as
uncertain is intent: the class does not say whether the omission is deliberate, and no
Spark discussion records a decision.

### How the values were verified

Spark 4.2.0, `local[2]`, forked test JVM heap `-Xmx4G`, host DRAM 186 GiB total, on
2026-10-21. The values in the table were produced by a one-off probe on the Spark test
classpath and are now pinned as assertions in `XpathDivergenceSpec`. No production code
changed to produce them.

## Backend claims

Rejection claims outside the XPath divergence — aggregations, window functions, datasketches,
AES, generator expressions and the rest — are enumerated with their tests in
[`backend-claims.md`](backend-claims.md). That document owns the coverage table; this one owns
the XPath divergence and the upstream record.
