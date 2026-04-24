"""Brand / model catalog used to seed the discovery queue.

Keep it broad on purpose — the queue layer dedups by URL so redundant entries
are cheap, and expanding coverage is the whole point of v2.
"""
from __future__ import annotations

ARABAM_BRANDS: list[str] = [
    "alfa-romeo", "audi", "bmw", "chery", "chevrolet", "chrysler", "citroen",
    "cupra", "dacia", "daihatsu", "dfsk", "dodge", "dongfeng", "ds-automobiles",
    "fiat", "ford", "geely", "honda", "hongqi", "hyundai", "infiniti", "isuzu",
    "iveco", "jaguar", "jeep", "kia", "lada", "lancia", "land-rover", "leapmotor",
    "lexus", "lincoln", "mahindra", "maserati", "mazda", "mercedes-benz",
    "mg", "mini", "mitsubishi", "nissan", "opel", "peugeot", "porsche", "proton",
    "renault", "rover", "saab", "seat", "skoda", "smart", "ssangyong", "subaru",
    "suzuki", "tata", "tesla", "tofas", "toyota", "volkswagen", "volvo",
]

# Optional model refinements — when None, adapter lists brand-wide, which pulls
# all models in pagination. Use this for brands with very deep catalogs.
ARABAM_BRAND_MODELS: dict[str, list[str]] = {
    "renault": ["clio", "megane", "symbol", "captur", "kadjar", "fluence", "taliant", "talisman"],
    "fiat": ["egea", "linea", "doblo", "500", "panda", "500l", "punto", "tipo"],
    "ford": ["focus", "fiesta", "mondeo", "tourneo-connect", "tourneo-courier", "kuga", "ecosport", "puma"],
    "volkswagen": ["passat", "polo", "golf", "jetta", "tiguan", "touran", "caddy", "t-roc"],
    "toyota": ["corolla", "auris", "yaris", "c-hr", "rav4", "chr", "hilux", "verso"],
    "hyundai": ["i10", "i20", "i30", "accent", "elantra", "tucson", "kona", "bayon"],
    "opel": ["astra", "corsa", "insignia", "mokka", "vectra", "zafira", "crossland"],
    "peugeot": ["208", "301", "307", "308", "508", "2008", "3008", "5008", "partner"],
    "citroen": ["c3", "c4", "c-elysee", "berlingo", "c5", "xsara"],
    "mercedes-benz": ["a-serisi", "b-serisi", "c-serisi", "e-serisi", "s-serisi", "cla", "gla", "glc"],
    "bmw": ["1-serisi", "2-serisi", "3-serisi", "4-serisi", "5-serisi", "x1", "x3", "x5"],
    "audi": ["a1", "a3", "a4", "a5", "a6", "q2", "q3", "q5", "q7"],
    "honda": ["civic", "city", "jazz", "cr-v", "hr-v", "accord"],
    "nissan": ["qashqai", "juke", "micra", "note", "x-trail", "navara"],
    "kia": ["rio", "ceed", "sportage", "picanto", "stonic", "cerato"],
    "skoda": ["fabia", "octavia", "superb", "rapid", "karoq", "kodiaq"],
    "seat": ["ibiza", "leon", "arona", "ateca"],
    "mitsubishi": ["lancer", "outlander", "asx", "l200"],
    "mazda": ["3", "6", "cx-3", "cx-5"],
    "dacia": ["sandero", "duster", "logan", "dokker"],
    "suzuki": ["swift", "vitara", "sx4", "jimny"],
    "mg": ["zs", "3", "5", "hs"],
    "cupra": ["formentor", "leon"],
    "jeep": ["renegade", "compass", "cherokee", "wrangler"],
    "chery": ["tiggo-7-pro", "tiggo-8-pro", "tiggo-2-pro"],
    "lexus": ["ux", "nx", "rx", "is", "es"],
    "porsche": ["cayenne", "macan", "panamera", "911", "taycan"],
    "tesla": ["model-3", "model-y", "model-s", "model-x"],
    "volvo": ["xc40", "xc60", "xc90", "v40", "v60", "s60", "s90"],
}


def arabam_brand_model_pairs() -> list[tuple[str, str | None]]:
    """Yield (brand, model) pairs. model=None means brand-wide search."""
    pairs: list[tuple[str, str | None]] = []
    for brand in ARABAM_BRANDS:
        models = ARABAM_BRAND_MODELS.get(brand)
        if models:
            for model in models:
                pairs.append((brand, model))
        else:
            pairs.append((brand, None))
    return pairs
