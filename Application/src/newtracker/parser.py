from __future__ import annotations

import csv
import hashlib
import re
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


PART_BLOCK_RE = re.compile(
    r"\( PARTNAME (?P<part>.*?)\s*\)\s*"
    r"\( PART REV (?P<rev>.*?)\s*\)\s*"
    r"\( ORDER NUMBER (?P<order>.*?)\s*\)\s*"
    r"\( QUANTITY NESTED\s+(?P<qty>\d+)\)",
    re.MULTILINE,
)

NPT_RE = re.compile(
    r"\(NPT:(?P<seq>[^S]+)S:\s*(?P<q>\d+)Q:\s*(?P<r>\d+)R:\s*(?P<o>\d+)O:\s*(?P<x>[\d.]+)X:\s*(?P<y>[\d.]+) Y:\s*(?P<part>[^\s]+)\s+(?P<rev>.*?)\)",
)

EMK_NPT_RE = re.compile(
    r"\(NPT:(?P<seq>\d+)Q:\s*(?P<q>\d+)R:\s*(?P<r>\d+)O:(?P<o>.*?)X:\s*(?P<x>[\d.]+)\s+Y:\s*(?P<y>[\d.]+)\s+N:(?P<part>\S+)\s+(?P<rev>[^)]*?)\s*\)",
)

LASER_NPT_RE = re.compile(
    r"\(NPT:(?P<seq>\d+)S:\s*(?P<s>\d+)Q:\s*(?P<q>\d+)R:\s*(?P<r>\d+)O:\s*(?P<o>\d+)X:\s*(?P<x>[\d.]+)\s+Y:\s*(?P<y>[\d.]+)\s*N:(?P<part>\S+)\s+(?P<rev>[^)]*?)\s*\)",
)

PROGRAM_NUMBER_RE = re.compile(r"^O(?P<number>\d+)", re.MULTILINE)
MACHINE_TYPE_RE = re.compile(r"^\((?P<machine>[A-Z0-9 ]+)\)$", re.MULTILINE)
DATE_RE = re.compile(r"\(\s*DATE\s*:\s*(?P<value>[^)]+)\)")
TIME_RE = re.compile(r"\(\s*TIME\s*:\s*(?P<value>[^)]+)\)")
SHEET_RE = re.compile(r"\(\s*SHEET PRORAM NAME\s+(?P<program>.*?)\s+MATERIAL\s+(?P<material>.*?)\s*\)")
PROCESS_RE = re.compile(r"\(\s*PROCESS THIS PRORAM\s+(?P<count>\d+)\s+TIME")
SIZE_RE = re.compile(r"\(\s*LENTH:(?P<length>[\d.]+)\s+WIDTH:(?P<width>[\d.]+)\)")
ORDER_PARSE_RE = re.compile(r"^(?P<code>[A-Z]+)\s+(?P<build>.+)$")
EMK_PROGRAM_RE = re.compile(r"\(PR/(?P<value>[^)]+)\)")
EMK_MACHINE_RE = re.compile(r"\(MC/(?P<value>[^)]+)\)")
EMK_CREATE_RE = re.compile(r"\(CR/Y(?P<year>\d{4})M(?P<month>\d{2})D(?P<day>\d{2})\)")
PART_NUMBER_LIKE_RE = re.compile(r"^[0-9A-Z]+(?:-[0-9A-Z]+)+$")


@dataclass
class ParsedNestPart:
    part_number: str
    part_revision: str
    quantity_nested: int
    order_number_raw: str
    npt_sequence: str | None = None
    npt_quantity: int | None = None
    npt_rotation: str | None = None
    npt_operation: str | None = None
    npt_x: float | None = None
    npt_y: float | None = None


@dataclass
class ParsedDatFile:
    barcode_filename: str
    program_file_name: str
    program_number: str | None
    machine_type: str | None
    sheet_program_name: str | None
    material_code: str | None
    sheet_length: float | None
    sheet_width: float | None
    program_date: str | None
    program_time: str | None
    process_count: int | None
    order_number_raw: str | None
    order_process_code: str | None
    build_date_code: str | None
    source_file_path: str
    parts: list[ParsedNestPart]


@dataclass
class ParsedNestComparisonRow:
    com_number: str
    part_number: str
    rev_level: str
    build_date: str
    quantity_per: int | None
    nested_on: str
    length: float | None
    width: float | None
    thickness: str
    item_class: str
    department_number: str
    part_parent: str
    ops_files: str
    pair_part_number: str
    p4_edits: str
    collection_cart: str
    routing: str
    model_number: str
    shear: str
    punch: str
    form: str
    requires_forming: int
    weight: float | None
    coded_part_msg: str
    parent_model_number: str
    skid_number: str
    page_number: str
    split_value: str
    source_file_path: str


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _clean(value: str | None) -> str:
    return (value or "").strip()


def _to_int(value: str | None) -> int | None:
    value = _clean(value)
    if not value:
        return None
    return int(float(value))


def _to_float(value: str | None) -> float | None:
    value = _clean(value)
    if not value:
        return None
    return float(value)


def _format_emk_date(year: str, month: str, day: str) -> str:
    month_value = int(month)
    day_value = int(day)
    return f"{month_value}/{day_value}/{year}"


def _looks_like_part_number(value: str | None) -> bool:
    return bool(PART_NUMBER_LIKE_RE.match(_clean(value)))


def parse_dat_file(path: Path) -> ParsedDatFile:
    content = path.read_text(encoding="utf-8", errors="ignore")

    program_number_match = PROGRAM_NUMBER_RE.search(content)
    machine_match = MACHINE_TYPE_RE.search(content)
    date_match = DATE_RE.search(content)
    time_match = TIME_RE.search(content)
    sheet_match = SHEET_RE.search(content)
    process_match = PROCESS_RE.search(content)
    size_match = SIZE_RE.search(content)
    emk_program_match = EMK_PROGRAM_RE.search(content)
    emk_machine_match = EMK_MACHINE_RE.search(content)
    emk_create_match = EMK_CREATE_RE.search(content)

    npt_lookup: dict[tuple[str, str], dict[str, str]] = {}
    for match in NPT_RE.finditer(content):
        part_number = _clean(match.group("part"))
        revision = _clean(match.group("rev"))
        npt_lookup[(part_number, revision)] = match.groupdict()

    emk_npt_rows: list[dict[str, str]] = []
    for match in EMK_NPT_RE.finditer(content):
        row = match.groupdict()
        row["part"] = _clean(row.get("part"))
        row["rev"] = _clean(row.get("rev"))
        emk_npt_rows.append(row)

    laser_npt_rows: list[dict[str, str]] = []
    for match in LASER_NPT_RE.finditer(content):
        row = match.groupdict()
        row["part"] = _clean(row.get("part"))
        row["rev"] = _clean(row.get("rev"))
        laser_npt_rows.append(row)

    parts: list[ParsedNestPart] = []
    for match in PART_BLOCK_RE.finditer(content):
        part_number = _clean(match.group("part"))
        revision = _clean(match.group("rev"))
        order_raw = _clean(match.group("order"))
        qty = int(match.group("qty")) if _clean(match.group("qty")) else 0
        if not part_number:
            continue
        npt = npt_lookup.get((part_number, revision), {})
        parts.append(
            ParsedNestPart(
                part_number=part_number,
                part_revision=revision,
                quantity_nested=qty,
                order_number_raw=order_raw,
                npt_sequence=_clean(npt.get("seq")),
                npt_quantity=_to_int(npt.get("q")),
                npt_rotation=_clean(npt.get("r")),
                npt_operation=_clean(npt.get("o")),
                npt_x=_to_float(npt.get("x")),
                npt_y=_to_float(npt.get("y")),
            )
        )

    if not parts and (emk_npt_rows or laser_npt_rows):
        source_rows = emk_npt_rows if emk_npt_rows else laser_npt_rows
        grouped_parts: OrderedDict[tuple[str, str], list[dict[str, str]]] = OrderedDict()
        for row in source_rows:
            key = (row["part"], row["rev"])
            grouped_parts.setdefault(key, []).append(row)

        for (part_number, revision), rows in grouped_parts.items():
            first = rows[0]
            parsed_quantities = []
            for row in rows:
                value = _to_int(row.get("s"))
                if value is None:
                    value = _to_int(row.get("q"))
                if value is not None:
                    parsed_quantities.append(value)
            quantity_nested = sum(parsed_quantities) if parsed_quantities else len(rows)
            parts.append(
                ParsedNestPart(
                    part_number=part_number,
                    part_revision=revision,
                    quantity_nested=quantity_nested,
                    order_number_raw="",
                    npt_sequence=_clean(first.get("seq")),
                    npt_quantity=_to_int(first.get("s")) if _to_int(first.get("s")) is not None else _to_int(first.get("q")),
                    npt_rotation=_clean(first.get("r")),
                    npt_operation=_clean(first.get("o")),
                    npt_x=_to_float(first.get("x")),
                    npt_y=_to_float(first.get("y")),
                )
            )

    if not parts:
        emk_program_name = _clean(emk_program_match.group("value") if emk_program_match else None)
        stem_name = _clean(path.stem)
        single_part_name = None
        if _looks_like_part_number(emk_program_name):
            single_part_name = emk_program_name
        elif _looks_like_part_number(stem_name):
            single_part_name = stem_name

        if single_part_name:
            parts.append(
                ParsedNestPart(
                    part_number=single_part_name,
                    part_revision="",
                    quantity_nested=1,
                    order_number_raw="",
                )
            )

    order_number_raw = None
    for part in parts:
        if _clean(part.order_number_raw):
            order_number_raw = _clean(part.order_number_raw)
            break
    order_process_code = None
    build_date_code = None
    if order_number_raw:
        order_match = ORDER_PARSE_RE.match(order_number_raw)
        if order_match:
            order_process_code = _clean(order_match.group("code"))
            build_date_code = _clean(order_match.group("build"))

    program_file_name = path.name
    if emk_program_match:
        program_file_name = f"{_clean(emk_program_match.group('value'))}{path.suffix}"

    machine_type = _clean(machine_match.group("machine") if machine_match else None)
    if not machine_type and emk_machine_match:
        machine_type = _clean(emk_machine_match.group("value"))

    program_date = _clean(date_match.group("value") if date_match else None)
    if not program_date and emk_create_match:
        program_date = _format_emk_date(
            emk_create_match.group("year"),
            emk_create_match.group("month"),
            emk_create_match.group("day"),
        )

    return ParsedDatFile(
        barcode_filename=path.name,
        program_file_name=program_file_name,
        program_number=_clean(program_number_match.group("number") if program_number_match else None),
        machine_type=machine_type,
        sheet_program_name=_clean(sheet_match.group("program") if sheet_match else None),
        material_code=_clean(sheet_match.group("material") if sheet_match else None),
        sheet_length=_to_float(size_match.group("length") if size_match else None),
        sheet_width=_to_float(size_match.group("width") if size_match else None),
        program_date=program_date,
        program_time=_clean(time_match.group("value") if time_match else None),
        process_count=_to_int(process_match.group("count") if process_match else None),
        order_number_raw=order_number_raw,
        order_process_code=order_process_code,
        build_date_code=build_date_code,
        source_file_path=str(path),
        parts=parts,
    )


def parse_nest_comparison_csv(path: Path) -> Iterable[ParsedNestComparisonRow]:
    with path.open("r", encoding="utf-8-sig", errors="ignore", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            form_raw = _clean(row.get("Form"))
            form_value = _to_float(form_raw)
            requires_forming = 1 if form_value is not None and form_value > 0 else 0
            yield ParsedNestComparisonRow(
                com_number=_clean(row.get("ComNumber")),
                part_number=_clean(row.get("PartNumber")),
                rev_level=_clean(row.get("Rev Level")),
                build_date=_clean(row.get("Build Date")),
                quantity_per=_to_int(row.get("QuantityPer")),
                nested_on=_clean(row.get("NestedOn")),
                length=_to_float(row.get("Length")),
                width=_to_float(row.get("Width")),
                thickness=_clean(row.get("Thickness")),
                item_class=_clean(row.get("ItemClass")),
                department_number=_clean(row.get("DepartmentNumber")),
                part_parent=_clean(row.get("PartParent")),
                ops_files=_clean(row.get("OPS Files")),
                pair_part_number=_clean(row.get("Pair Part Number")),
                p4_edits=_clean(row.get("P4 Edits")),
                collection_cart=_clean(row.get("Collection Cart")),
                routing=_clean(row.get("Routing")),
                model_number=_clean(row.get("Model #")),
                shear=_clean(row.get("Shear")),
                punch=_clean(row.get("Punch")),
                form=form_raw,
                requires_forming=requires_forming,
                weight=_to_float(row.get("Weight")),
                coded_part_msg=_clean(row.get("Coded Part Msg")),
                parent_model_number=_clean(row.get("Parent Model Number")),
                skid_number=_clean(row.get("Skid Number")),
                page_number=_clean(row.get("Page Number")),
                split_value=_clean(row.get("Split Value")),
                source_file_path=str(path),
            )
