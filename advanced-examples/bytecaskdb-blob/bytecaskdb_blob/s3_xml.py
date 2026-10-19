"""S3-compatible XML serialization and deserialization."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timezone


def _xml_bytes(root: ET.Element) -> bytes:
    return ET.tostring(root, xml_declaration=True, encoding="UTF-8")


def _iso_time(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z"
    )


def error_xml(code: str, message: str) -> bytes:
    root = ET.Element("Error")
    ET.SubElement(root, "Code").text = code
    ET.SubElement(root, "Message").text = message
    return _xml_bytes(root)


def list_buckets_xml(buckets: list[dict]) -> bytes:
    root = ET.Element("ListAllMyBucketsResult")
    owner = ET.SubElement(root, "Owner")
    ET.SubElement(owner, "ID").text = "bytecaskdb"
    ET.SubElement(owner, "DisplayName").text = "bytecaskdb"
    buckets_el = ET.SubElement(root, "Buckets")
    for b in buckets:
        bkt = ET.SubElement(buckets_el, "Bucket")
        ET.SubElement(bkt, "Name").text = b["name"]
        ET.SubElement(bkt, "CreationDate").text = _iso_time(b["created_at"])
    return _xml_bytes(root)


def location_constraint_xml() -> bytes:
    root = ET.Element("LocationConstraint")
    root.text = ""
    return _xml_bytes(root)


def list_objects_xml(
    bucket: str,
    prefix: str,
    delimiter: str,
    common_prefixes: list[str],
    contents: list[dict],
) -> bytes:
    root = ET.Element("ListBucketResult")
    ET.SubElement(root, "Name").text = bucket
    ET.SubElement(root, "Prefix").text = prefix
    ET.SubElement(root, "Delimiter").text = delimiter
    ET.SubElement(root, "MaxKeys").text = "1000"
    ET.SubElement(root, "IsTruncated").text = "false"
    ET.SubElement(root, "KeyCount").text = str(len(contents) + len(common_prefixes))

    for obj in contents:
        c = ET.SubElement(root, "Contents")
        ET.SubElement(c, "Key").text = obj["key"]
        ET.SubElement(c, "Size").text = str(obj["size"])
        ET.SubElement(c, "ETag").text = f'"{obj["etag"]}"'
        ET.SubElement(c, "LastModified").text = _iso_time(
            obj.get("completed_at", obj.get("created_at", 0))
        )
        ET.SubElement(c, "StorageClass").text = "STANDARD"

    for pfx in common_prefixes:
        cp = ET.SubElement(root, "CommonPrefixes")
        ET.SubElement(cp, "Prefix").text = pfx

    return _xml_bytes(root)


def delete_result_xml(deleted: list[str], errors: list[tuple[str, str]]) -> bytes:
    root = ET.Element("DeleteResult")
    for key in deleted:
        d = ET.SubElement(root, "Deleted")
        ET.SubElement(d, "Key").text = key
    for key, msg in errors:
        e = ET.SubElement(root, "Error")
        ET.SubElement(e, "Key").text = key
        ET.SubElement(e, "Code").text = "InternalError"
        ET.SubElement(e, "Message").text = msg
    return _xml_bytes(root)


def initiate_multipart_xml(bucket: str, key: str, upload_id: str) -> bytes:
    root = ET.Element("InitiateMultipartUploadResult")
    ET.SubElement(root, "Bucket").text = bucket
    ET.SubElement(root, "Key").text = key
    ET.SubElement(root, "UploadId").text = upload_id
    return _xml_bytes(root)


def complete_multipart_xml(bucket: str, key: str, etag: str) -> bytes:
    root = ET.Element("CompleteMultipartUploadResult")
    ET.SubElement(root, "Bucket").text = bucket
    ET.SubElement(root, "Key").text = key
    ET.SubElement(root, "ETag").text = f'"{etag}"'
    return _xml_bytes(root)


def parse_delete_objects(body: bytes) -> list[str]:
    """Parse a Delete Objects XML request body, returning the list of keys.

    Raises ValueError on malformed XML.
    """
    try:
        xml_root = ET.fromstring(body)
    except ET.ParseError as e:
        raise ValueError(str(e)) from e

    keys: list[str] = []
    for obj in xml_root.findall("Object") or xml_root.findall("{*}Object"):
        key_el = obj.find("Key") or obj.find("{*}Key")
        if key_el is None:
            continue
        keys.append(key_el.text or "")
    return keys
