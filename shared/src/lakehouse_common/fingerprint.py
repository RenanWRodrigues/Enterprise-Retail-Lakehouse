"""Row fingerprint shared by Retail and IoT Silver (decision 088).

Promoted from ``retail_lakehouse.silver.standardize.fingerprint`` and the identical
``iot_lakehouse.silver.standardize._content_hash`` **without changing a single output byte**:
both values are persisted and are idempotency keys (Retail ``_row_hash`` in the quarantine
MERGE; IoT ``_content_hash`` in the Silver MERGE and resend detection). A new algorithm would
make every stored row look new on the next run (duplicates in the quarantine, resends taken as
conflicts), so any change needs a versioned column and a backfill, never an in-place swap.

Contract of ``fingerprint(c1, ..., cn)``:

- **fields and order**: exactly the given columns, in the given order (the caller chooses and
  documents them; reordering changes the hash);
- **normalization**: each value is ``CAST(value AS STRING)`` and nothing else: no trimming, no
  case folding, no Unicode normalization. The caller normalizes first if it wants to (IoT hashes
  its trimmed text, Retail the Bronze text as delivered);
- **NULL**: replaced by ``NULL_MARKER`` (U+0000) in its own position, so ``NULL`` differs from
  ``""`` and ``(a, NULL, b)`` from ``(a, b, NULL)`` (``xxhash64`` and plain ``concat_ws`` skip
  NULLs: the Retail bug fixed in 5461e80);
- **serialization**: the texts joined by ``FIELD_SEPARATOR`` (U+001F, ASCII unit separator);
- **types**: atomic Spark types (string, numeric, boolean, date, timestamp, decimal). The type is
  not hashed, only its text: ``1`` (int) and ``"1"`` match; ``1.0`` and ``1.00`` (decimal scales)
  differ; dates are ``yyyy-MM-dd``; timestamps are rendered in the **session time zone** (UTC in
  every pipeline and test session), so a session in another zone yields other hashes. Arrays,
  maps and structs are not supported (their text form does not mark NULL elements);
- **algorithm**: SHA-256 of the UTF-8 text, lowercase hex (64 characters), never NULL.

Collisions: SHA-256 itself is not a practical concern. The serialization is not injective for
values that contain U+001F or that equal U+0000: ``("a\\u001fb", "c")`` and ``("a", "b\\u001fc")``
hash alike, and a value ``"\\u0000"`` hashes like NULL. Neither character occurs in the sources
(text files and JSON from the generators and the delivered CSV contracts); escaping them would
change persisted hashes, so it is documented instead (tests pin both cases).
"""

from typing import Final

from pyspark.sql import Column
from pyspark.sql import functions as F

FIELD_SEPARATOR: Final = "\u001f"
NULL_MARKER: Final = "\u0000"
HASH_BITS: Final = 256


def fingerprint(*columns: Column | str) -> Column:
    """SHA-256 (hex) of ``columns`` in order, each cast to text, NULL marked in its position.

    Accepts column names or expressions. Raises ``ValueError`` without columns (the hash of
    nothing would be one constant shared by every row).
    """
    if not columns:
        raise ValueError("fingerprint needs at least one column")
    parts = [
        F.coalesce((F.col(c) if isinstance(c, str) else c).cast("string"), F.lit(NULL_MARKER))
        for c in columns
    ]
    return F.sha2(F.concat_ws(FIELD_SEPARATOR, *parts), HASH_BITS)
