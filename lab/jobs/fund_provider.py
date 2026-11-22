"""Deliver the day's price file to S3, as the data provider does.

Not part of the service's pipeline, so it emits no lineage: the provider's delivery
is where the service's lineage starts, at the file it reads.
"""

from __future__ import annotations

import os
import sys
from datetime import date

import s3fs

sys.path.insert(0, "/lab")
import funds


def main(ds: str) -> None:
    day = (date.fromisoformat(ds) - funds.START).days + 1
    missing = funds.missing_on(day)
    fs = s3fs.S3FileSystem(endpoint_url=os.environ["AWS_ENDPOINT_URL"])
    with fs.open(funds.PROVIDER_FILE, "w") as f:
        f.write(funds.prices(day, missing))
    print(f"delivered day {day}'s prices" + (f", without {sorted(missing)}" if missing else ""))


if __name__ == "__main__":
    main(sys.argv[1])
