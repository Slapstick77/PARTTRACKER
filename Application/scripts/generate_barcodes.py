from __future__ import annotations

import argparse
import math
import re
import sys
from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from newtracker.db import DATA_DIR, DatabaseConfigurationError, get_connection

DEFAULT_OUTPUT_DIR = DATA_DIR / "barcode_sheets"

PAGE_WIDTH = 2550
PAGE_HEIGHT = 3300
MARGIN = 80
LABEL_COLS = 2
LABEL_WIDTH = 1140
LABEL_HEIGHT = 360
LABEL_GAP_X = 50
LABEL_GAP_Y = 40
BARCODE_HEIGHT = 210

TEST_USERS = [
    ("STH", "Test User - STH"),
    ("JAL", "Test User - JAL"),
]

MACHINES = [
    ("ATC", "Machine - ATC"),
    ("CINCINNATI", "Machine - CINCINNATI"),
    ("EMK", "Machine - EMK"),
    ("RAS", "Machine - RAS"),
    ("LASER", "Machine - LASER"),
]

LOCATIONS = [
    ("DRP-2", "Location - DRP-2"),
    ("DRP-3", "Location - DRP-3"),
    ("WR-4", "Location - WR-4"),
    ("PALLET", "Location - PALLET"),
    ("T-15", "Location - T-15"),
]

DEFAULT_DATS = [
    "SCDDAGFY.DAT",
    "LBDLDKMY.DAT",
    "LBDLDKMZ.DAT",
    "SCDDDRHQ.DAT",
    "SCDDDQJL.DAT",
    "SCDDDMNL.DAT",
    "SCDDDRJW.DAT",
    "87722-03-1298.DAT",
]


@dataclass
class BarcodeEntry:
    value: str
    title: str
    subtitle: str = ""


@dataclass
class DatPartEntry:
    dat_name: str
    part_number: str
    part_revision: str
    quantity_nested: int
    requires_forming: int
    com_number: str | None
    form_value: str | None


def _safe_stem(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return safe or "barcode"


def _load_font(size: int) -> ImageFont.ImageFont | ImageFont.FreeTypeFont:
    for name in ["arial.ttf", "DejaVuSans.ttf"]:
        try:
            return ImageFont.truetype(name, size=size)
        except OSError:
            continue
    return ImageFont.load_default()


FONT_TITLE = _load_font(42)
FONT_SUBTITLE = _load_font(26)
FONT_HEADER = _load_font(64)
FONT_SECTION = _load_font(42)
FONT_PAGE = _load_font(28)


def render_barcode_image(value: str) -> Image.Image:
    from barcode import Code128
    from barcode.writer import ImageWriter

    barcode = Code128(value, writer=ImageWriter())
    rendered = barcode.render(
        writer_options={
            "module_width": 0.68,
            "module_height": 58,
            "font_size": 0,
            "quiet_zone": 10,
            "dpi": 600,
            "write_text": False,
        }
    )
    return rendered.convert("RGB")


def fit_barcode_to_width(barcode: Image.Image, *, target_width: int, min_height: int) -> Image.Image:
    source_width, source_height = barcode.size
    if source_width <= 0 or source_height <= 0:
        return barcode

    width_scale = target_width / source_width
    scaled_height = int(source_height * width_scale)

    target_height = max(min_height, scaled_height)
    target_size = (target_width, max(1, target_height))

    return barcode.resize(target_size, Image.Resampling.LANCZOS)


def make_label(entry: BarcodeEntry) -> Image.Image:
    card = Image.new("RGB", (LABEL_WIDTH, LABEL_HEIGHT), "white")
    draw = ImageDraw.Draw(card)
    draw.rounded_rectangle((2, 2, LABEL_WIDTH - 3, LABEL_HEIGHT - 3), radius=20, outline="black", width=3)

    draw.text((28, 16), entry.title, fill="black", font=FONT_TITLE)
    if entry.subtitle:
        draw.text((28, 66), entry.subtitle, fill="black", font=FONT_SUBTITLE)

    barcode = render_barcode_image(entry.value)
    barcode = fit_barcode_to_width(barcode, target_width=LABEL_WIDTH - 50, min_height=BARCODE_HEIGHT)
    x = 25
    y = 104
    card.paste(barcode, (x, y))

    value_font = _load_font(28)
    bbox = draw.textbbox((0, 0), entry.value, font=value_font)
    text_width = bbox[2] - bbox[0]
    value_y = min(LABEL_HEIGHT - 42, y + barcode.height + 10)
    draw.text(((LABEL_WIDTH - text_width) // 2, value_y), entry.value, fill="black", font=value_font)
    return card


def _save_page(page: Image.Image, output_path: Path) -> Path:
    final_path = output_path.with_suffix(".pdf")
    page.convert("RGB").save(final_path, format="PDF", resolution=300.0)
    return final_path


def _paginate(entries: list[BarcodeEntry], output_path: Path, page_title: str) -> list[Path]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pages: list[Path] = []
    per_page = 16

    for page_index in range(math.ceil(len(entries) / per_page) or 1):
        chunk = entries[page_index * per_page : (page_index + 1) * per_page]
        page = Image.new("RGB", (PAGE_WIDTH, PAGE_HEIGHT), "white")
        draw = ImageDraw.Draw(page)
        draw.text((MARGIN, MARGIN - 10), page_title, fill="black", font=FONT_HEADER)

        start_y = MARGIN + 90
        for idx, entry in enumerate(chunk):
            row = idx // LABEL_COLS
            col = idx % LABEL_COLS
            label = make_label(entry)
            x = MARGIN + col * (LABEL_WIDTH + LABEL_GAP_X)
            y = start_y + row * (LABEL_HEIGHT + LABEL_GAP_Y)
            page.paste(label, (x, y))

        final_path = output_path if page_index == 0 else output_path.with_name(
            f"{output_path.stem}_page_{page_index + 1}{output_path.suffix}"
        )
        pages.append(_save_page(page, final_path))

    return pages


def _draw_section(
    page: Image.Image,
    *,
    draw: ImageDraw.ImageDraw,
    title: str,
    entries: list[BarcodeEntry],
    start_y: int,
) -> int:
    draw.text((MARGIN, start_y), title, fill="black", font=FONT_SECTION)
    y = start_y + 64

    if not entries:
        draw.text((MARGIN + 20, y), "No parts in this section", fill="black", font=FONT_SUBTITLE)
        return y + 70

    for idx, entry in enumerate(entries):
        row = idx // LABEL_COLS
        col = idx % LABEL_COLS
        label = make_label(entry)
        x = MARGIN + col * (LABEL_WIDTH + LABEL_GAP_X)
        label_y = y + row * (LABEL_HEIGHT + LABEL_GAP_Y)
        page.paste(label, (x, label_y))

    rows_used = math.ceil(len(entries) / LABEL_COLS)
    return y + rows_used * LABEL_HEIGHT + max(0, rows_used - 1) * LABEL_GAP_Y


def _draw_page_number(page: Image.Image, page_number: int, total_pages: int) -> None:
    draw = ImageDraw.Draw(page)
    text = f"{page_number}/{total_pages}"
    bbox = draw.textbbox((0, 0), text, font=FONT_PAGE)
    text_width = bbox[2] - bbox[0]
    x = PAGE_WIDTH - MARGIN - text_width
    y = PAGE_HEIGHT - MARGIN + 8
    draw.text((x, y), text, fill="black", font=FONT_PAGE)


def _render_dat_pages(dat_name: str, unformed_entries: list[BarcodeEntry], formed_entries: list[BarcodeEntry]) -> list[Image.Image]:
    pages: list[Image.Image] = []

    def new_page(is_first: bool) -> tuple[Image.Image, ImageDraw.ImageDraw, int]:
        page = Image.new("RGB", (PAGE_WIDTH, PAGE_HEIGHT), "white")
        draw = ImageDraw.Draw(page)
        draw.text((MARGIN, MARGIN - 10), dat_name, fill="black", font=FONT_HEADER)

        if is_first:
            header_label = make_label(BarcodeEntry(value=dat_name, title=dat_name, subtitle="DAT Scan"))
            header_x = (PAGE_WIDTH - LABEL_WIDTH) // 2
            header_y = MARGIN + 90
            page.paste(header_label, (header_x, header_y))
            start_y = header_y + LABEL_HEIGHT + 50
        else:
            draw.text((MARGIN, MARGIN + 90), "Continued", fill="black", font=FONT_SUBTITLE)
            start_y = MARGIN + 140

        return page, draw, start_y

    def draw_section_paginated(
        section_title: str,
        entries: list[BarcodeEntry],
        *,
        page: Image.Image,
        draw: ImageDraw.ImageDraw,
        y: int,
        is_first_page: bool,
    ) -> tuple[Image.Image, ImageDraw.ImageDraw, int, bool]:
        if not entries:
            draw.text((MARGIN, y), section_title, fill="black", font=FONT_SECTION)
            y += 64
            draw.text((MARGIN + 20, y), "No parts in this section", fill="black", font=FONT_SUBTITLE)
            return page, draw, y + 70, is_first_page

        index = 0
        while index < len(entries):
            draw.text((MARGIN, y), section_title, fill="black", font=FONT_SECTION)
            y += 64

            usable_height = (PAGE_HEIGHT - MARGIN) - y
            rows_available = max(0, (usable_height + LABEL_GAP_Y) // (LABEL_HEIGHT + LABEL_GAP_Y))
            per_page_capacity = rows_available * LABEL_COLS

            if per_page_capacity <= 0:
                pages.append(page)
                page, draw, y = new_page(False)
                is_first_page = False
                continue

            chunk = entries[index : index + per_page_capacity]
            for idx, entry in enumerate(chunk):
                row = idx // LABEL_COLS
                col = idx % LABEL_COLS
                label = make_label(entry)
                x = MARGIN + col * (LABEL_WIDTH + LABEL_GAP_X)
                label_y = y + row * (LABEL_HEIGHT + LABEL_GAP_Y)
                page.paste(label, (x, label_y))

            rows_used = math.ceil(len(chunk) / LABEL_COLS)
            y += rows_used * LABEL_HEIGHT + max(0, rows_used - 1) * LABEL_GAP_Y
            index += len(chunk)

            if index < len(entries):
                pages.append(page)
                page, draw, y = new_page(False)
                is_first_page = False

        return page, draw, y + 30, is_first_page

    page, draw, y = new_page(True)
    is_first_page = True

    page, draw, y, is_first_page = draw_section_paginated(
        "NOT FORMED", unformed_entries, page=page, draw=draw, y=y, is_first_page=is_first_page
    )

    if y > PAGE_HEIGHT - (LABEL_HEIGHT + 120):
        pages.append(page)
        page, draw, y = new_page(False)
        is_first_page = False

    page, draw, y, is_first_page = draw_section_paginated(
        "FORMED", formed_entries, page=page, draw=draw, y=y, is_first_page=is_first_page
    )

    pages.append(page)
    return pages


def build_master_entries() -> dict[str, list[BarcodeEntry]]:
    return {
        "test_users": [BarcodeEntry(value=v, title=v, subtitle=d) for v, d in TEST_USERS],
        "machines": [BarcodeEntry(value=v, title=v, subtitle=d) for v, d in MACHINES],
        "locations": [BarcodeEntry(value=v, title=v, subtitle=d) for v, d in LOCATIONS],
    }


def fetch_dat_parts(dat_name: str) -> list[DatPartEntry]:
    try:
        with get_connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    barcode_filename,
                    part_number,
                    part_revision,
                    quantity_nested,
                    requires_forming,
                    com_number,
                    form_value
                FROM resolved_nest_parts
                WHERE barcode_filename = ?
                ORDER BY requires_forming DESC, part_number
                """,
                (dat_name,),
            ).fetchall()
    except DatabaseConfigurationError as exc:
        raise ValueError(str(exc)) from exc

    return [
        DatPartEntry(
            dat_name=row["barcode_filename"],
            part_number=row["part_number"],
            part_revision=row["part_revision"] or "-",
            quantity_nested=int(row["quantity_nested"] or 0),
            requires_forming=int(row["requires_forming"] or 0),
            com_number=row["com_number"],
            form_value=row["form_value"],
        )
        for row in rows
    ]


def _expand_part_entries(parts: list[DatPartEntry], *, requires_forming: int) -> list[BarcodeEntry]:
    expanded: list[BarcodeEntry] = []
    for part in parts:
        if int(part.requires_forming) != int(requires_forming):
            continue
        count = max(1, int(part.quantity_nested))
        subtitle = f"Rev {part.part_revision} | Qty {part.quantity_nested}"
        if part.com_number:
            subtitle += f" | COM {part.com_number}"
        for copy_idx in range(count):
            copy_suffix = f" ({copy_idx + 1}/{count})" if count > 1 else ""
            expanded.append(
                BarcodeEntry(
                    value=part.part_number,
                    title=part.part_number,
                    subtitle=subtitle + copy_suffix,
                )
            )
    return expanded


def generate_dat_sheet(dat_name: str, output_dir: Path) -> list[Path]:
    parts = fetch_dat_parts(dat_name)
    if not parts:
        raise ValueError(f"No resolved parts found in SQL for {dat_name}")

    output_dir.mkdir(parents=True, exist_ok=True)
    formed_entries = _expand_part_entries(parts, requires_forming=1)
    unformed_entries = _expand_part_entries(parts, requires_forming=0)

    rendered_pages = _render_dat_pages(dat_name, unformed_entries, formed_entries)
    total_pages = len(rendered_pages)

    outputs: list[Path] = []
    for page_index, page in enumerate(rendered_pages, start=1):
        _draw_page_number(page, page_index, total_pages)
        output_path = output_dir / f"{_safe_stem(dat_name)}.pdf"
        final_path = output_path if page_index == 1 else output_path.with_name(
            f"{output_path.stem}_page_{page_index}{output_path.suffix}"
        )
        outputs.append(_save_page(page, final_path))

    return outputs


def generate_master_sheets(output_dir: Path) -> list[Path]:
    pages: list[Path] = []
    entries = build_master_entries()
    pages.extend(_paginate(entries["test_users"], output_dir / "test_users.pdf", "Test Users"))
    pages.extend(_paginate(entries["machines"], output_dir / "machines.pdf", "Machines"))
    pages.extend(_paginate(entries["locations"], output_dir / "locations.pdf", "Locations"))
    return pages


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate printable Code 128 barcode sheets for test users, machines, locations, and DAT scan pages."
    )
    parser.add_argument(
        "dat_names",
        nargs="*",
        help="DAT filenames to generate part barcode sheets for. Defaults to the validated test DATs.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
    help=f"Directory for generated sheet PDF files. Default: {DEFAULT_OUTPUT_DIR}",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    dat_names = args.dat_names or DEFAULT_DATS
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    generated: list[Path] = []
    generated.extend(generate_master_sheets(output_dir))
    for dat_name in dat_names:
        generated.extend(generate_dat_sheet(dat_name, output_dir))

    print(f"Generated {len(generated)} barcode sheet file(s) into {output_dir}")
    for path in generated:
        print(path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))