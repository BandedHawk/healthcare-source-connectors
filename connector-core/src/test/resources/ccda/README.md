# C-CDA test documents

All synthetic or de-identified; none contains PHI.

| File | Source |
|---|---|
| `google-synthea-sample1.xml`, `google-synthea-sample1.json` | Input and expected output of Google's Healthcare Data Harmonization XmlToJson tool (`wstl1/tools/XmlToJson/src/test/resources/synthea/`, sample 1), Apache License 2.0. A Synthea synthetic patient. Used to check the converter matches Google's JSON style. |
| `uscdi-v3-all-mappings.xml` | USCDI v3 sample covering every mapping, provided by the project owner. |
| `master-ccd.xml` | Master CCD sample, provided by the project owner. |
| `alice-newman-uscdi-v3.xml` | ONC test patient Alice Newman, USCDI v3, provided by the project owner. |
| `epic-inpatient-ccd-deidentified.xml` | De-identified Epic inpatient CCD, provided by the project owner. Digits were rewritten by the de-identification: its `xsi` namespace is `http://www.w3.org/9999901/XMLSchema-instance` and template IDs contain `99999`. The patient's `administrativeGenderCode` was recoded from `code="U"` (HL7 v2's Unknown, not in the HL7 v3 AdministrativeGender value set C-CDA uses) to `nullFlavor="UNK"`. The de-identification also renamed the narrative's `ID` attributes to `state` (292 of them), and `<reference value="#…">` points at those. |

The four documents other than Google's are also the sample documents of the US Core 6.1 C-CDA mapping (`mapping` repository, `us-core-6.1/src/test/resources/samples/`), as this converter's JSON output. The mapping keeps a copy of each XML beside its JSON, and its tests check that the JSON is exactly this converter's output for it (`SourceDocumentTest`). After changing one of these documents, copy it to the mapping as well; after changing the converter's output, the mapping's tests fail until its inputs are regenerated (`mvn test -Dinputs.regenerate=true` there) and its baselines accepted (`-Dbaseline.update=true`). The mapping's `fixtures/epic-gender-u.xml` is the Epic document with its original `code="U"`. The mapping repository's `ccda-quickrun` command-line tool also bundles `connector-core` for this converter, so it picks up a change to the conversion when it is rebuilt.

`CcdaConverterTest` also runs against all 13 of Google's published pairs when given `-Dccda.google.dir=<dir>` holding them as `sampleN.xml` / `sampleN.json`.
