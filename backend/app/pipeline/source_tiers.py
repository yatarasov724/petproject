"""
Source authority tier classification.

Tier-1 sources are primary/authoritative publishers — news agencies and
trusted market channels. Their publications are never suppressed by the
source authority guard. Tier-2 sources (everyone else) can be suppressed
if a tier-1 source published the same story within SOURCE_AUTH_MINUTES.
"""

TIER_1_SOURCES: frozenset[str] = frozenset({
    "TASS",
    "Interfax",
    "Prime",
    "RIA",
    "TG:moexnews",
    "TG:cbrstocks",
})


def get_tier(source_name: str) -> int:
    """Return 1 for authoritative sources, 2 for everyone else."""
    return 1 if source_name in TIER_1_SOURCES else 2
