# Watch runtime

Future watch runtime for bounded replay, subscriptions, progress tracking, and resync decisions.

It is intentionally soft state: a process restart must result in safe client resynchronization rather than data loss or an incorrect freshness claim.

