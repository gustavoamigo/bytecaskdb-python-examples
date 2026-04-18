#!/usr/bin/env python3
"""Generate Markdown API docs from the bytecaskdb module."""

import io
import pydoc
import re
import sys
from pathlib import Path

# ── Pydoc section patterns ──────────────────────────────────────────────────
CLASS_HEADER = re.compile(r"^    class (\w+)\((.+)\)$")
SECTION_DIVIDER = re.compile(r"^     \|  -{10,}")
SECTION_TITLE = re.compile(
    r"^     \|  (Methods defined here|Static methods defined here|"
    r"Readonly properties defined here|Data descriptors defined here|"
    r"Data and other attributes defined here|"
    r"Methods inherited from .+|Static methods inherited from .+|"
    r"Data descriptors inherited from .+|Method resolution order):$"
)
METHOD_DEF = re.compile(r"^     \|  (\w+)\(")
SIGNATURE_LINE = re.compile(r"^     \|      (\w+\(.*\).*)")
PROPERTY_LINE = re.compile(r"^     \|  (\w+)$")
DOC_LINE = re.compile(r"^     \|      (.+)")
EMPTY_PIPE = re.compile(r"^     \|[\s]*$")

# Classes whose inherited boilerplate we skip entirely.
SKIP_INHERITED_SECTIONS = True

# Dunder methods we don't surface.
HIDDEN_METHODS = {
    "__init__", "__new__", "__getattribute__", "__reduce__",
    "__repr__", "__str__", "__setstate__", "__weakref__",
    "__dict__", "__cause__", "__context__", "__suppress_context__",
    "__traceback__", "__enter__", "__exit__", "__iter__", "__next__",
    "args", "add_note", "with_traceback",
}

# Iterator classes with no unique user-facing API.
ITERATOR_CLASSES = {
    "EntryIterator", "KeyIterator", "ReverseEntryIterator", "ReverseKeyIterator",
}


def parse_pydoc(text: str):
    """Return a list of class descriptors parsed from pydoc output."""
    lines = text.splitlines()
    classes: list[dict] = []
    module_version = ""
    i = 0

    while i < len(lines):
        # Module version
        if lines[i].startswith("VERSION"):
            i += 1
            while i < len(lines) and lines[i].strip():
                module_version = lines[i].strip()
                i += 1
            continue

        m = CLASS_HEADER.match(lines[i])
        if not m:
            i += 1
            continue

        cls_name = m.group(1)
        cls_bases = m.group(2)
        i += 1

        # Collect the class-level docstring.
        # Pydoc format after the class header:
        #   |  ClassName(*args, **kwargs)   ← constructor repr (skip)
        #   |
        #   |  Docstring line 1.            ← collect these
        #   |  Docstring line 2.
        #   |
        #   |  Methods defined here:        ← stop
        cls_doc_lines: list[str] = []
        # Skip optional constructor repr line like "Options(*args, **kwargs)"
        if i < len(lines):
            raw = lines[i]
            if raw.startswith("     |") and len(raw) > 7:
                content = raw[7:].strip()
                # Constructor repr matches "ClassName(" — skip it
                if content.startswith(cls_name + "("):
                    i += 1

        # Now collect actual docstring lines
        in_docstring = False
        while i < len(lines):
            raw = lines[i]
            # Stop at section boundaries
            if SECTION_DIVIDER.match(raw) or SECTION_TITLE.match(raw):
                break
            if not raw.startswith("     |"):
                break
            content = raw[7:].rstrip() if len(raw) > 7 else ""
            # Skip boilerplate
            if content.startswith("__init__") or content == "Initialize self.  See help(type(self)) for accurate signature.":
                i += 1
                continue
            # Blank pipe line
            if not content:
                if in_docstring:
                    # Blank line between docstring paragraphs — keep going
                    # but stop if next line is a section title
                    if i + 1 < len(lines) and SECTION_TITLE.match(lines[i + 1]):
                        i += 1
                        break
                    cls_doc_lines.append("")
                i += 1
                continue
            # Stop if we hit a method definition
            if METHOD_DEF.match(raw):
                break
            in_docstring = True
            cls_doc_lines.append(content)
            i += 1

        # Clean up: strip trailing blanks, join into paragraphs
        while cls_doc_lines and not cls_doc_lines[-1]:
            cls_doc_lines.pop()
        # Remove leading whitespace common to all non-blank lines
        non_blank = [l for l in cls_doc_lines if l.strip()]
        if non_blank:
            common = min(len(l) - len(l.lstrip()) for l in non_blank)
            cls_doc_lines = [l[common:] if l.strip() else "" for l in cls_doc_lines]
        cls_doc = "\n".join(cls_doc_lines).strip()

        # Parse sections
        methods: list[dict] = []
        properties: list[dict] = []
        static_attrs: list[dict] = []
        is_inherited = False

        while i < len(lines):
            line = lines[i]

            # End of class block
            if line and not line.startswith("     |") and not line.startswith("    "):
                break
            if CLASS_HEADER.match(line):
                break

            # Section divider
            if SECTION_DIVIDER.match(line):
                i += 1
                continue

            # Section title
            sm = SECTION_TITLE.match(line)
            if sm:
                title = sm.group(1)
                is_inherited = "inherited" in title or title == "Method resolution order"
                i += 1
                continue

            # Skip inherited sections
            if is_inherited and SKIP_INHERITED_SECTIONS:
                i += 1
                continue

            # Method / static attr with signature
            mm = METHOD_DEF.match(line)
            if mm:
                name = mm.group(1)
                i += 1
                # Collect signature and doc lines
                sig_lines: list[str] = []
                doc_lines: list[str] = []
                while i < len(lines):
                    if SECTION_DIVIDER.match(lines[i]) or SECTION_TITLE.match(lines[i]):
                        break
                    if CLASS_HEADER.match(lines[i]):
                        break
                    if METHOD_DEF.match(lines[i]):
                        break
                    if PROPERTY_LINE.match(lines[i]) and not DOC_LINE.match(lines[i]):
                        # Could be next property
                        next_text = lines[i][7:].strip() if len(lines[i]) > 7 else ""
                        if next_text and not next_text.startswith(" ") and "(" not in next_text:
                            break
                    sl = SIGNATURE_LINE.match(lines[i])
                    dl = DOC_LINE.match(lines[i])
                    if sl and not sig_lines:
                        sig_lines.append(sl.group(1))
                    elif dl:
                        doc_lines.append(dl.group(1))
                    elif EMPTY_PIPE.match(lines[i]):
                        i += 1
                        continue
                    else:
                        break
                    i += 1

                if name in HIDDEN_METHODS or name == cls_name:
                    continue

                signature = sig_lines[0] if sig_lines else f"{name}(...)"
                doc = "\n".join(doc_lines).strip()
                methods.append({"name": name, "signature": signature, "doc": doc})
                continue

            # Property / data descriptor / static attr like "open = <...>"
            if len(line) > 7 and line.startswith("     |  "):
                prop_text = line[8:].rstrip()
                if not prop_text or prop_text.startswith(" "):
                    i += 1
                    continue
                # "name = <nanobind...>" style static attribute
                static_match = re.match(r"^(\w+)\s*=\s*<.+>$", prop_text)
                if static_match:
                    attr_name = static_match.group(1)
                    i += 1
                    sig_lines = []
                    doc_lines = []
                    while i < len(lines):
                        if SECTION_DIVIDER.match(lines[i]) or SECTION_TITLE.match(lines[i]):
                            break
                        if CLASS_HEADER.match(lines[i]):
                            break
                        sl = SIGNATURE_LINE.match(lines[i])
                        dl = DOC_LINE.match(lines[i])
                        if sl and not sig_lines:
                            sig_lines.append(sl.group(1))
                        elif dl:
                            doc_lines.append(dl.group(1))
                        elif EMPTY_PIPE.match(lines[i]):
                            i += 1
                            continue
                        else:
                            break
                        i += 1
                    if attr_name not in HIDDEN_METHODS:
                        signature = sig_lines[0] if sig_lines else f"{attr_name}(...)"
                        doc = "\n".join(doc_lines).strip()
                        methods.append({"name": attr_name, "signature": signature, "doc": doc, "static": True})
                    continue
                # Regular property
                if "(" not in prop_text and prop_text not in HIDDEN_METHODS:
                    prop_name = prop_text
                    i += 1
                    doc_lines = []
                    while i < len(lines):
                        dl = DOC_LINE.match(lines[i])
                        if dl:
                            doc_lines.append(dl.group(1))
                            i += 1
                        elif EMPTY_PIPE.match(lines[i]):
                            i += 1
                        else:
                            break
                    if prop_name not in HIDDEN_METHODS:
                        properties.append({"name": prop_name, "doc": "\n".join(doc_lines).strip()})
                    continue

            i += 1

        # Check for static "open = ..." style attrs
        # (already captured as methods via METHOD_DEF if they have signatures)

        classes.append({
            "name": cls_name,
            "bases": cls_bases,
            "doc": cls_doc,
            "methods": methods,
            "properties": properties,
            "static_attrs": static_attrs,
        })

    return classes, module_version


def clean_signature(sig: str) -> str:
    """Replace internal module paths for readability."""
    sig = sig.replace("bytecaskdb._bytecaskdb.", "")
    return sig


def render_markdown(classes: list[dict], version: str) -> str:
    """Render parsed classes to Markdown."""
    out: list[str] = []
    out.append("# ByteCaskDB Python API Reference\n")
    if version:
        out.append(f"> Version **{version}**\n")
    out.append("*Auto-generated from `python3 -c \"import bytecaskdb; help(bytecaskdb)\"`*\n")
    out.append("---\n")

    # Table of contents
    main_classes = [c for c in classes if c["name"] not in ITERATOR_CLASSES
                    and c["name"] != "OSError"]
    out.append("## Contents\n")
    for cls in main_classes:
        anchor = cls["name"].lower()
        out.append(f"- [{cls['name']}](#{anchor})")
    out.append("")

    # Iterators note
    out.append("> **Iterator types** (`EntryIterator`, `KeyIterator`, "
               "`ReverseEntryIterator`, `ReverseKeyIterator`) implement the "
               "standard Python iterator protocol (`__iter__` / `__next__`) "
               "and are returned by the `iter_from`, `keys_from`, `riter_from`, "
               "and `rkeys_from` methods.\n")
    out.append("---\n")

    for cls in main_classes:
        out.append(f"## {cls['name']}\n")
        if cls["doc"]:
            out.append(f"{cls['doc']}\n")

        # Properties
        if cls["properties"]:
            out.append("### Properties\n")
            out.append("| Property | Description |")
            out.append("|----------|-------------|")
            for p in cls["properties"]:
                doc = p["doc"].replace("\n", " ")
                out.append(f"| `{p['name']}` | {doc} |")
            out.append("")

        # Methods
        if cls["methods"]:
            out.append("### Methods\n")
            for m in cls["methods"]:
                sig = clean_signature(m["signature"])
                label = "*static*  " if m.get("static") else ""
                out.append(f"#### {label}`{sig}`\n")
                if m["doc"]:
                    out.append(f"{m['doc']}\n")

        out.append("---\n")

    return "\n".join(out)


def main():
    output_dir = Path("docs")
    output_path = output_dir / "api.md"

    import bytecaskdb
    text = pydoc.plain(pydoc.render_doc(bytecaskdb, title="%s"))

    classes, version = parse_pydoc(text)
    md = render_markdown(classes, version)

    output_dir.mkdir(exist_ok=True)
    output_path.write_text(md)
    print(f"Wrote {output_path}  ({len(classes)} classes parsed)")


if __name__ == "__main__":
    main()
