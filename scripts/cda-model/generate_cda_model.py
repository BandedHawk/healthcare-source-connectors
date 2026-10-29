#!/usr/bin/env python3
"""
Generates the CDA R2 (+ SDTC extensions) element model used by the C-CDA to JSON converter (design §6.4).

The converter follows the JSON style of Google's Healthcare Data Harmonization XmlToJson tool, which is driven by
the CDA schema: an element the schema allows to repeat is always a JSON array, even with one occurrence. This
script reads HL7's published schemas and writes, for every complex type, its child elements, their types and
whether they repeat, plus which types carry text.

Usage:
    scripts/cda-model/generate_cda_model.py [--xsd-dir DIR] [--out FILE]

Without --xsd-dir the schemas are downloaded from HL7's CDA-core-2.0 repository at the pinned commit below.
The output is committed at connector-core/src/main/resources/com/sdh/healthcare/connector/core/convert/
cda-r2-sdtc.model; re-run this script and commit the result when upgrading the schema.

The schemas are HL7's ("This product includes software developed by Health Level Seven"); only the derived model
is committed, not the schemas.
"""
import argparse
import os
import sys
import tempfile
import urllib.request
import xml.etree.ElementTree as ET

REPO = "HL7/CDA-core-2.0"
COMMIT = "e922fc35586fd2629f0c8a021080bca9ab424e18"
BASE = "schema/extensions/SDTC/"
FILES = [
    "infrastructure/cda/CDA_SDTC.xsd",
    "infrastructure/cda/POCD_MT000040_SDTC.xsd",
    "infrastructure/cda/SDTC.xsd",
    "processable/coreschemas/NarrativeBlock.xsd",
    "processable/coreschemas/datatypes-base_SDTC.xsd",
    "processable/coreschemas/datatypes.xsd",
    "processable/coreschemas/infrastructureRoot.xsd",
    "processable/coreschemas/voc.xsd",
]
XS = "{http://www.w3.org/2001/XMLSchema}"
V3 = "urn:hl7-org:v3"
SDTC = "urn:hl7-org:sdtc"
PREFIX = {V3: "", SDTC: "sdtc:"}

# Google's post-processor turns these into single values whatever the schema says (PostProcessorCdaRev2).
FORCED_SINGLE = {"reference", "thumbnail", "high", "low", "width", "center"}


def fetch(dest):
    for f in FILES:
        path = os.path.join(dest, f)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        url = f"https://raw.githubusercontent.com/{REPO}/{COMMIT}/{BASE}{f}"
        with urllib.request.urlopen(url) as r, open(path, "wb") as out:
            out.write(r.read())


def qname(value, nsmap, default_ns):
    """Resolves a QName attribute value ("hl7:CE", "CE") to (namespace, local)."""
    if ":" in value:
        p, local = value.split(":", 1)
        return nsmap[p], local
    return default_ns, value


def key(ns, local):
    return PREFIX[ns] + local


def ns_map(path):
    m = {}
    for event, (prefix, uri) in ET.iterparse(path, events=["start-ns"]):
        m[prefix] = uri
    return m


def occurs_many(el):
    mx = el.get("maxOccurs", "1")
    return mx == "unbounded" or int(mx) > 1


INTEGER = {"integer", "int", "long", "short", "byte", "nonNegativeInteger", "positiveInteger", "nonPositiveInteger",
           "negativeInteger", "unsignedLong", "unsignedInt", "unsignedShort", "unsignedByte"}


class Schema:
    def __init__(self):
        self.types = {}       # key -> (element, nsmap, target namespace)
        self.elements = {}    # key -> (element, nsmap, target namespace)
        self.attr_groups = {} # key -> (element, nsmap, target namespace)

    def load(self, path):
        nsmap = ns_map(path)
        root = ET.parse(path).getroot()
        # Files without a targetNamespace are included into the v3 schema ("chameleon" includes).
        tns = root.get("targetNamespace") or V3
        for el in root:
            if el.tag in (XS + "complexType", XS + "simpleType") and el.get("name"):
                self.types[key(tns, el.get("name"))] = (el, nsmap, tns)
            elif el.tag == XS + "element" and el.get("name"):
                self.elements[key(tns, el.get("name"))] = (el, nsmap, tns)
            elif el.tag == XS + "attributeGroup" and el.get("name"):
                self.attr_groups[key(tns, el.get("name"))] = (el, nsmap, tns)

    def type_key(self, value, nsmap, tns):
        ns, local = qname(value, nsmap, tns)
        if ns == "http://www.w3.org/2001/XMLSchema":
            return "xs:" + local
        return key(ns, local)

    def simple_kind(self, name):
        """JSON kind of a simple type, as Google's schema binding writes it: boolean, integer, or None (string)."""
        if name.startswith("xs:"):
            local = name[3:]
            return "boolean" if local == "boolean" else "integer" if local in INTEGER else None
        if name not in self.types:
            return None
        el, nsmap, tns = self.types[name]
        if el.tag != XS + "simpleType":
            return None
        for d in el:
            if d.tag == XS + "restriction" and d.get("base"):
                return self.simple_kind(self.type_key(d.get("base"), nsmap, tns))
        return None  # unions and lists stay strings

    def attributes(self, node, nsmap, tns, out):
        for a in node:
            if a.tag == XS + "attribute" and a.get("name") and a.get("type") and a.get("use") != "prohibited":
                out[a.get("name")] = self.simple_kind(self.type_key(a.get("type"), nsmap, tns))
            elif a.tag == XS + "attributeGroup" and a.get("ref"):
                ns, local = qname(a.get("ref"), nsmap, tns)
                g, gnsmap, gtns = self.attr_groups[key(ns, local)]
                self.attributes(g, gnsmap, gtns, out)

    def attrs_of(self, name):
        """Typed attributes of a complex type, inherited ones included."""
        if name.startswith("xs:") or name not in self.types:
            return {}
        el, nsmap, tns = self.types[name]
        if el.tag != XS + "complexType":
            return {}
        out = {}
        for part in el:
            if part.tag in (XS + "complexContent", XS + "simpleContent"):
                for d in part:
                    if d.tag in (XS + "extension", XS + "restriction"):
                        out.update(self.attrs_of(self.type_key(d.get("base"), nsmap, tns)))
                        self.attributes(d, nsmap, tns, out)
        self.attributes(el, nsmap, tns, out)
        return out

    def resolve(self, name, seen=None):
        """Returns (children: {key: [type, many]}, mixed, text) for a type, flattening derivation."""
        if name.startswith("xs:") or name not in self.types:
            return {}, False, True
        el, nsmap, tns = self.types[name]
        if el.tag == XS + "simpleType":
            return {}, False, True
        children = {}
        mixed = el.get("mixed") == "true"
        text = mixed
        for part in el:
            if part.tag == XS + "simpleContent":
                text = True
            elif part.tag == XS + "complexContent":
                mixed = mixed or part.get("mixed") == "true"
                for d in part:
                    if d.tag in (XS + "extension", XS + "restriction"):
                        base = self.type_key(d.get("base"), nsmap, tns)
                        if d.tag == XS + "extension":
                            bc, bm, bt = self.resolve(base)
                            for k, v in bc.items():
                                children[k] = list(v)
                            mixed = mixed or bm
                            text = text or bm
                        self.particles(d, nsmap, tns, False, children)
            elif part.tag in (XS + "sequence", XS + "choice", XS + "all"):
                self.particles(part, nsmap, tns, occurs_many(part), children)
        text = text or mixed
        return children, mixed, text

    def particles(self, node, nsmap, tns, many, out):
        for c in node:
            if c.tag == XS + "element":
                m = many or occurs_many(c)
                if c.get("ref"):
                    ns, local = qname(c.get("ref"), nsmap, tns)
                    k = key(ns, local)
                    decl, dnsmap, dtns = self.elements[k]
                    t = self.type_key(decl.get("type"), dnsmap, dtns)
                else:
                    k = key(tns, c.get("name"))
                    t = self.type_key(c.get("type"), nsmap, tns) if c.get("type") else "xs:anyType"
                if k in out:
                    out[k][1] = out[k][1] or m
                else:
                    out[k] = [t, m]
            elif c.tag in (XS + "sequence", XS + "choice", XS + "all"):
                self.particles(c, nsmap, tns, many or occurs_many(c), out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xsd-dir")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "../../connector-core/src/main/resources/"
                                                  "com/sdh/healthcare/connector/core/convert/cda-r2-sdtc.model"))
    args = ap.parse_args()
    d = args.xsd_dir or tempfile.mkdtemp(prefix="cda-xsd-")
    if not args.xsd_dir:
        fetch(d)
    s = Schema()
    for f in FILES:
        s.load(os.path.join(d, f))

    lines = [
        "# CDA R2 + SDTC element model for the C-CDA JSON converter. Generated by scripts/cda-model/generate_cda_model.py",
        f"# from {REPO} @ {COMMIT} (schemas: Health Level Seven). Do not edit by hand.",
        "# root <element> <type>",
        "# type <type> <mixed 0|1> <text 0|1>",
        "# child <type> <element> <child type> <repeats 0|1>",
        "# attr <type> <attribute> <boolean|integer>   (attributes not listed are strings)",
    ]
    root_el, rns, rtns = s.elements["ClinicalDocument"]
    lines.append(f"root\tClinicalDocument\t{s.type_key(root_el.get('type'), rns, rtns)}")
    for name in sorted(s.types):
        children, mixed, text = s.resolve(name)
        if s.types[name][0].tag == XS + "simpleType":
            continue
        lines.append(f"type\t{name}\t{int(mixed)}\t{int(text)}")
        for k in sorted(children):
            t, many = children[k]
            if k in FORCED_SINGLE:
                many = False
            lines.append(f"child\t{name}\t{k}\t{t}\t{int(many)}")
        for a, kind in sorted(s.attrs_of(name).items()):
            if kind:
                lines.append(f"attr\t{name}\t{a}\t{kind}")
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as out:
        out.write("\n".join(lines) + "\n")
    print(f"wrote {args.out}: {sum(1 for l in lines if l.startswith('type'))} types, "
          f"{sum(1 for l in lines if l.startswith('child'))} child elements", file=sys.stderr)


if __name__ == "__main__":
    main()
