# Suggested single X post (English)

**Status:** draft; replace `[REPO]` with the real public GitHub URL after publication. X typically counts the URL as 23 characters; the final text below is estimated at 270 characters with that substitution.

> @graphprotocol @streamingfastio: Incremental Substreams checkpoints—save changed keys/tombstones, periodically compact, fallback to full. Go file test: 12.2× faster writes at 1% churn (3 runs); NOT server speedup. 2,800 modeled cases + negatives: [REPO]

The 12.2× is the **median write-only local file result over 3 repeats in a standalone 96 MiB Go prototype**. It is **not** a real server measurement, not an end-to-end result, and not a guarantee. The model is synthetic and uncalibrated. Include the repository with the negative cases (scan overhead, high mutation rate and restore cost). Do not imply an upstream PR exists.
