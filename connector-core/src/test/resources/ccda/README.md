# C-CDA test documents

All synthetic or de-identified; none contains PHI.

| File | Source |
|---|---|
| `google-synthea-sample1.xml`, `google-synthea-sample1.json` | Input and expected output of Google's Healthcare Data Harmonization XmlToJson tool (`wstl1/tools/XmlToJson/src/test/resources/synthea/`, sample 1), Apache License 2.0. A Synthea synthetic patient. Used to check the converter matches Google's JSON style. |
| `uscdi-v3-all-mappings.xml` | USCDI v3 sample covering every mapping, provided by the project owner. |
| `master-ccd.xml` | Master CCD sample, provided by the project owner. |
| `alice-newman-uscdi-v3.xml` | ONC test patient Alice Newman, USCDI v3, provided by the project owner. |
| `epic-inpatient-ccd-deidentified.xml` | De-identified Epic inpatient CCD, provided by the project owner. Digits were rewritten by the de-identification: its `xsi` namespace is `http://www.w3.org/9999901/XMLSchema-instance` and template IDs contain `99999`. |

`CcdaConverterTest` also runs against all 13 of Google's published pairs when given `-Dccda.google.dir=<dir>` holding them as `sampleN.xml` / `sampleN.json`.
