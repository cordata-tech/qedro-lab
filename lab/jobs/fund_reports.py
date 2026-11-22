"""Publish one document per fund to S3, with each share class's figures.

The service's output, standing in for a key information document. A one-page PDF
written by hand, so the tools image needs no PDF library; run under art30-emit, which
declares one output per fund, so a fund's document can be traced back on its own.
"""

from __future__ import annotations

import os
import sys

import psycopg
import s3fs

sys.path.insert(0, "/lab")
import funds


def pdf(lines: list[str]) -> bytes:
    """A minimal one-page PDF with *lines* in Helvetica."""
    text = "BT /F1 11 Tf 56 780 Td 15 TL " + " ".join(
        "(" + line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)") + ") '"
        for line in lines
    ) + " ET"  # fmt: skip
    objects = [
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Contents 4 0 R "
        "/Resources << /Font << /F1 5 0 R >> >> >>",
        f"<< /Length {len(text)} >>\nstream\n{text}\nendstream",
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out, offsets = b"%PDF-1.4\n", []
    for i, body in enumerate(objects, 1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n{body}\nendobj\n".encode("latin-1")
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
    out += "".join(f"{o:010d} 00000 n \n" for o in offsets).encode()
    trailer = f"<< /Size {len(objects) + 1} /Root 1 0 R >>"
    out += f"trailer\n{trailer}\nstartxref\n{xref}\n%%EOF\n".encode()
    return out


def main() -> None:
    fs = s3fs.S3FileSystem(endpoint_url=os.environ["AWS_ENDPOINT_URL"])
    with psycopg.connect(funds.DSN) as conn:
        rows = conn.execute(
            "select fund, name, share_class, price_date, nav, ongoing_charges_pct "
            "from funds.ongoing_charges order by share_class"
        ).fetchall()
    for fund, name in funds.FUNDS.items():
        mine = [r for r in rows if r[0] == fund]
        lines = [name, f"Fund {fund}", ""]
        lines += [f"{r[2]}: NAV {r[4]:.4f} on {r[3]}, ongoing charges {r[5]}% a year" for r in mine]
        if len(mine) < len(funds.CLASSES):
            lines += ["", "A share class has no price for this date; its figures are not shown."]
        with fs.open(f"s3://funds/reports/kid/{fund}.pdf", "wb") as f:
            f.write(pdf(lines))
    print(f"published {len(funds.FUNDS)} fund documents")


if __name__ == "__main__":
    main()
