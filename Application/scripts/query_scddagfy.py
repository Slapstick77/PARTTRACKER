from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(r"c:\Project p\NEWTRACKER\Application\data\newtracker.db")

query = """
SELECT
    np.part_number,
    np.part_revision,
    pn.build_date_code,
    pa.com_number,
    pa.form,
    pa.requires_forming
FROM program_nests pn
JOIN nest_parts np ON np.nest_id = pn.id
LEFT JOIN part_attributes pa
    ON pa.part_number = np.part_number
    AND IFNULL(pa.rev_level, '') = IFNULL(np.part_revision, '')
    AND IFNULL(pa.build_date, '') = IFNULL(pn.build_date_code, '')
WHERE pn.barcode_filename = ?
ORDER BY np.part_number
"""

with sqlite3.connect(DB_PATH) as connection:
    connection.row_factory = sqlite3.Row
    rows = connection.execute(query, ("SCDDAGFY.DAT",)).fetchall()

for row in rows:
    print(
        f"{row['part_number']}|{row['part_revision']}|{row['build_date_code']}|"
        f"{row['com_number']}|{row['form']}|{row['requires_forming']}"
    )
