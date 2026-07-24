# Pending sessions

The first send may precede real session creation. The temporary id must be
atomically promoted and the payload dispatched at most once.
