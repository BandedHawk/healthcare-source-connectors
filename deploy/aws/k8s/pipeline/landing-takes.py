#!/usr/bin/env python3
# Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
#
# Proprietary and confidential. Use is subject to the terms in the LICENSE file
# in the root of this project.
"""What landing took from the intake, against what it landed (load-test.sh).

Reads the landing pods' kept logs (pod-logs/, plain or gzipped) and prints, as
report lines: how many objects landing took from the intake, by how it knew
them landed (item.finished_at_source: "landed", or "in_ledger" when listed
again); how many it left, changed or gone (item.left_at_source); and the
sightings taken but never landed, and landed but never taken. Both should be
0: a sighting taken and never landed is an object deleted unlanded (runs 55
and 56 lost objects). Sightings are hashes of a reference, size and version,
never a name.

Usage: landing-takes.py <log>...
"""
import collections
import gzip
import re
import sys


def main(paths):
    landed, taken = set(), []
    reasons, left = collections.Counter(), collections.Counter()
    for path in paths:
        opener = gzip.open if path.endswith(".gz") else open
        with opener(path, "rt", errors="replace") as log:
            for line in log:
                if " item.landed " in line:
                    m = re.search(r" sighting=([0-9a-f]+)", line)
                    if m:
                        landed.add(m.group(1))
                elif " item.finished_at_source " in line:
                    m = re.search(r" reason=(\S+).* sighting=([0-9a-f]+)",
                                  line)
                    if m:
                        reasons[m.group(1)] += 1
                        taken.append(m.group(2))
                elif " item.left_at_source " in line:
                    m = re.search(r" reason=(\S+)", line)
                    left[m.group(1) if m else "?"] += 1

    def listed(counts):
        return ", ".join("%s %d" % kv for kv in sorted(counts.items())) \
            or "none"

    done = set(taken)
    print(">>   landing took from the intake: %d (%s); left there, changed "
          "or gone: %d (%s)" % (len(taken), listed(reasons),
                                sum(left.values()), listed(left)))
    print(">>   sightings taken but never landed: %d (expected 0); landed "
          "but never taken: %d (expected 0)" % (len(done - landed),
                                                len(landed - done)))


if __name__ == "__main__":
    main(sys.argv[1:])
