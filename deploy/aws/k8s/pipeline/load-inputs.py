"""Makes the AWS PoC load test's inputs (deploy/aws/k8s/pipeline/load-test.sh). Only test documents with no PHI:

- Clean documents: mapping's 30 test documents (healthcare-data-transformer transform-stage/target/test-documents,
  synthetic, de-identified or HL7 examples), each copied COPIES times. A copy is made unique by a comment before its
  end tag, <!-- sdh-poc-load <run> c<n> -->: record IDs are keyed hashes of content, so identical copies would be one
  record downstream. The comment changes no line, so mapping's expected issues (lines and columns) still apply, and
  the gold check (GoldCheck) strips it to name the document. Half of them fail the US Core set's data checks
  (mapping's baseline), so the transform stage's fail store is exercised at volume.
- Error inputs: the file connector's demo files (file-connector DemoFiles), built exactly as it builds them from
  connector-core's sample documents, ERROR_COPIES of each, each copy tagged in its titles as DemoFiles tags them:
    batch      a batch of 3 documents (master, USCDI, Epic): the split stage cuts it into 3 records;
    bad-doc    a batch whose middle document has an undeclared &nbsp;: the convert stage fails that record
               (CONVERT_XML_MALFORMED), the other two pass;
    truncated  a batch cut off part-way through its second document: the split stage fails the item
               (PARSE_UNBALANCED_ELEMENT) after storing the first;
    utf16      a UTF-16 document: the split stage fails it at once (PARSE_XML_UNSUPPORTED_ENCODING);
    resend     the same bytes as one of the clean copies, under another key: landed as an item of its own with an
               item.duplicate_content warning, and processed again.

Writes the files to OUT (named by number, never by document) and OUT/manifest.json (what was made, for the report).

Usage: python3 -I load-inputs.py OUT RUN COPIES ERROR_COPIES TEST_DOCUMENTS SAMPLES
"""
import json
import os
import sys

UTF8_DECLARATION = '<?xml version="1.0" encoding="UTF-8"?>\n'
BATCH_START = '<batch xmlns="urn:hl7-org:v3">\n'


def document(path):
    """As DemoFiles.document: the sample's text, without a byte-order mark or XML declaration."""
    with open(path, encoding="utf-8") as f:
        text = f.read()
    if text.startswith("﻿"):
        text = text[1:]
    if text.startswith("<?xml"):
        text = text[text.index("?>") + 2:].lstrip()
    return text


def tagged(doc, tag):
    """As DemoFiles.tagged: the first title gets the tag."""
    return doc.replace("</title>", " [demo " + tag + "]</title>", 1)


def single(doc):
    return (UTF8_DECLARATION + doc).encode("utf-8")


def batch(*docs):
    text = UTF8_DECLARATION + BATCH_START
    for doc in docs:
        text += doc.strip() + "\n"
    return (text + "</batch>\n").encode("utf-8")


def unclosed_batch(complete, cut_off):
    return (UTF8_DECLARATION + BATCH_START + complete.strip() + "\n" + cut_off).encode("utf-8")


def utf16(doc):
    return ("﻿" + '<?xml version="1.0" encoding="UTF-16"?>\n' + doc).encode("utf-16-le")


def main():
    out, run, copies, error_copies, test_documents, samples = sys.argv[1:7]
    copies, error_copies = int(copies), int(error_copies)
    os.makedirs(out, exist_ok=False)
    manifest = {"run": run, "copies": copies, "error_copies": error_copies, "files": 0, "bytes": 0,
                "clean": {}, "errors": {}}

    def write(name, data):
        with open(os.path.join(out, name), "wb") as f:
            f.write(data)
        manifest["files"] += 1
        manifest["bytes"] += len(data)

    names = sorted(n[:-4] for n in os.listdir(test_documents) if n.endswith(".xml"))
    first_copies = []
    for k, name in enumerate(names):
        with open(os.path.join(test_documents, name + ".xml"), "rb") as f:
            original = f.read()
        end = original.rindex(b"</ClinicalDocument>")
        for c in range(copies):
            marker = ("<!-- sdh-poc-load %s c%d -->" % (run, c)).encode("ascii")
            data = original[:end] + marker + original[end:]
            write("c%04d-%02d.xml" % (c, k), data)
            if c == 0:
                first_copies.append(data)
        manifest["clean"][name] = copies

    synthea = document(os.path.join(samples, "google-synthea-sample1.xml"))
    alice = document(os.path.join(samples, "alice-newman-uscdi-v3.xml"))
    master = document(os.path.join(samples, "master-ccd.xml"))
    uscdi = document(os.path.join(samples, "uscdi-v3-all-mappings.xml"))
    epic = document(os.path.join(samples, "epic-inpatient-ccd-deidentified.xml"))
    for e in range(error_copies):
        tag = "load %s e%d" % (run, e)
        write("e%03d-batch.xml" % e, batch(tagged(master, tag), tagged(uscdi, tag), tagged(epic, tag)))
        unconvertible = tagged(alice, tag).replace("</title>", "&nbsp;(amended)</title>", 1)
        write("e%03d-bad-doc.xml" % e, batch(tagged(synthea, tag), unconvertible, tagged(uscdi, tag + " b")))
        truncated = master[:master.index(">", len(master) // 2) + 1]
        write("e%03d-truncated.xml" % e, unclosed_batch(tagged(epic, tag), truncated))
        write("e%03d-utf16.xml" % e, utf16(tagged(alice, tag)))
        write("e%03d-resend.xml" % e, first_copies[e % len(first_copies)])
    manifest["errors"] = {kind: error_copies for kind in ("batch", "bad-doc", "truncated", "utf16", "resend")}
    resent = [names[e % len(names)] for e in range(error_copies)]
    manifest["resent"] = {n: resent.count(n) for n in sorted(set(resent))}
    with open(os.path.join(out, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=1, sort_keys=True)


if __name__ == "__main__":
    main()
