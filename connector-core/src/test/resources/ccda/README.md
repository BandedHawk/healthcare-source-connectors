# C-CDA test documents

All synthetic or de-identified; none contains PHI.

| File | Source |
|---|---|
| `google-synthea-sample1.xml`, `google-synthea-sample1.json` | Input and expected output of Google's Healthcare Data Harmonization XmlToJson tool (`wstl1/tools/XmlToJson/src/test/resources/synthea/`, sample 1), Apache License 2.0. A Synthea synthetic patient. Used to check the converter matches Google's JSON style. |
| `uscdi-v3-all-mappings.xml` | USCDI v3 sample covering every mapping, provided by the project owner. |
| `master-ccd.xml` | Master CCD sample, provided by the project owner. |
| `alice-newman-uscdi-v3.xml` | ONC test patient Alice Newman, USCDI v3, provided by the project owner. |
| `epic-inpatient-ccd-deidentified.xml` | De-identified Epic inpatient CCD, provided by the project owner. Digits were rewritten by the de-identification: its `xsi` namespace is `http://www.w3.org/9999901/XMLSchema-instance` and template IDs contain `99999`. The patient's `administrativeGenderCode` was recoded from `code="U"` (HL7 v2's Unknown, not in the HL7 v3 AdministrativeGender value set C-CDA uses) to `nullFlavor="UNK"`. The de-identification also renamed the narrative's `ID` attributes to `state` (292 of them), and `<reference value="#…">` points at those. |

The four documents other than Google's are also the sample documents of the US Core 6.1 C-CDA mapping (`mapping` repository, `us-core-6.1/src/test/resources/samples/`), as this converter's JSON output. After changing one of them, or the converter's output, regenerate the mapping's samples (`CcdaConverter.convert(xml, SplitConfig.defaults("ccda"))`) and accept its new baselines (`mvn test -Dbaseline.update=true` there). The mapping's `fixtures/epic-gender-u.json` is the Epic document with its original `code="U"`.

`CcdaConverterTest` also runs against all 13 of Google's published pairs when given `-Dccda.google.dir=<dir>` holding them as `sampleN.xml` / `sampleN.json`.
