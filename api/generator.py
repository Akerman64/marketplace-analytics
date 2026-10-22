"""Générateur de données déterministe de l'API Marketplace (Maelys).

Règle clé : seed = md5(date) → une même date renvoie toujours exactement les
mêmes commandes. Le catalogue (vendeurs, produits, clients) est généré à
partir d'une graine fixe et ne change jamais.

Volumétrie calibrée sur le cahier des charges : 2 400 vendeurs, 180 000
produits, ~8 500 commandes/jour, CA mensuel ~4,2 M€.
"""

import hashlib
import random
from datetime import date, datetime, timedelta
from functools import lru_cache

N_SELLERS = 2_400
N_PRODUCTS = 180_000
N_CUSTOMERS = 1_200_000
BASE_ORDERS_PER_DAY = 7_400   # × facteurs saisonniers/croissance ≈ 8 500/jour
LAUNCH_DATE = date(2024, 1, 1)        # avant cette date : aucune commande
CATALOG_SEED = "maelys-catalog-v1"

# (catégorie, poids dans le catalogue, prix médian €)
CATEGORIES = [
    ("Mode & Accessoires", 0.20, 9.7),
    ("Maison & Déco", 0.16, 11.4),
    ("Beauté & Bien-être", 0.14, 7.9),
    ("Épicerie fine", 0.12, 5.3),
    ("Bijoux", 0.08, 13.2),
    ("Enfants & Jouets", 0.08, 8.8),
    ("Sport & Loisirs", 0.07, 10.6),
    ("High-Tech", 0.06, 19.4),
    ("Papeterie & Art", 0.05, 5.7),
    ("Animaux", 0.04, 7.0),
]

PRODUCT_NOUNS = {
    "Mode & Accessoires": ["T-shirt", "Sac cabas", "Écharpe", "Pull", "Casquette", "Ceinture", "Chemise", "Tote bag"],
    "Maison & Déco": ["Bougie", "Coussin", "Vase", "Plaid", "Mug", "Affiche", "Lampe", "Panier"],
    "Beauté & Bien-être": ["Savon", "Sérum", "Baume", "Huile", "Shampoing solide", "Crème", "Brume"],
    "Épicerie fine": ["Miel", "Confiture", "Thé", "Café", "Chocolat", "Huile d'olive", "Biscuits", "Épices"],
    "Bijoux": ["Bracelet", "Collier", "Boucles d'oreilles", "Bague", "Pendentif"],
    "Enfants & Jouets": ["Peluche", "Puzzle", "Jeu en bois", "Doudou", "Livre d'éveil", "Hochet"],
    "Sport & Loisirs": ["Tapis de yoga", "Gourde", "Kit pique-nique", "Ballon", "Sac de sport"],
    "High-Tech": ["Coque", "Chargeur", "Support", "Câble", "Enceinte", "Écouteurs"],
    "Papeterie & Art": ["Carnet", "Agenda", "Stylo", "Carte postale", "Aquarelle", "Stickers"],
    "Animaux": ["Collier", "Jouet", "Coussin", "Friandises", "Gamelle", "Laisse"],
}
ADJECTIVES = ["artisanal", "bio", "vintage", "premium", "essentiel", "recyclé", "fait main",
              "édition limitée", "naturel", "minimaliste", "coloré", "en lin", "en bois"]
SELLER_PREFIXES = ["Atelier", "Maison", "Comptoir", "La Fabrique", "Studio", "L'Échoppe",
                   "Manufacture", "Les Ateliers", "Boutique", "Collectif"]
SURNAMES = ["Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit", "Durand",
            "Leroy", "Moreau", "Simon", "Laurent", "Lefebvre", "Michel", "Garcia", "David",
            "Bertrand", "Roux", "Vincent", "Fournier", "Morel", "Girard", "André", "Mercier",
            "Dupont", "Lambert", "Bonnet", "François", "Martinez", "Legrand", "Garnier", "Faure"]
BRAND_SYLLABLES = ["lu", "mi", "no", "ka", "sa", "ré", "vo", "li", "ta", "zé", "ma", "o", "ri", "na"]
CITIES = {"FR": ["Paris", "Lyon", "Marseille", "Bordeaux", "Nantes", "Lille", "Toulouse", "Rennes",
                 "Strasbourg", "Montpellier", "Nice", "Annecy"],
          "BE": ["Bruxelles", "Liège", "Gand"], "ES": ["Barcelone", "Madrid"],
          "IT": ["Milan", "Turin"], "DE": ["Berlin", "Munich"]}
FIRST_NAMES = ["Camille", "Léa", "Manon", "Chloé", "Emma", "Inès", "Lucas", "Hugo", "Louis",
               "Nathan", "Jules", "Gabriel", "Sarah", "Yasmine", "Adam", "Mohamed", "Aminata",
               "Moussa", "Julie", "Thomas", "Clara", "Paul", "Zoé", "Noah"]


def _seed(value: str) -> int:
    return int(hashlib.md5(value.encode("utf-8")).hexdigest(), 16)


def _brand_name(rng: random.Random) -> str:
    return "".join(rng.choice(BRAND_SYLLABLES) for _ in range(rng.randint(2, 3))).capitalize()


@lru_cache(maxsize=1)
def sellers() -> tuple:
    rng = random.Random(_seed(CATALOG_SEED + "-sellers"))
    out = []
    for sid in range(1, N_SELLERS + 1):
        seller_type = rng.choices(["artisan", "dnvb", "revendeur"], [0.55, 0.20, 0.25])[0]
        if seller_type == "dnvb":
            name = _brand_name(rng) + rng.choice(["", " Paris", " Studio", " & Co"])
        else:
            name = f"{rng.choice(SELLER_PREFIXES)} {rng.choice(SURNAMES)}"
        name = f"{name} #{sid}"
        country = rng.choices(["FR", "BE", "ES", "IT", "DE"], [0.82, 0.07, 0.04, 0.04, 0.03])[0]
        joined = LAUNCH_DATE - timedelta(days=400) + timedelta(days=rng.randint(0, 1000))
        # Profil d'activité : 85 % normaux, 10 % très peu actifs, 5 % ayant arrêté de vendre
        profile = rng.choices(["normal", "low", "churned"], [0.85, 0.10, 0.05])[0]
        churn_date = None
        if profile == "churned":
            churn_date = date(2026, 6, 1) + timedelta(days=rng.randint(0, 140))
        out.append({
            "seller_id": sid,
            "name": name,
            "seller_type": seller_type,
            "country": country,
            "city": rng.choice(CITIES[country]),
            "joined_date": joined.isoformat(),
            "main_category": rng.choices([c[0] for c in CATEGORIES], [c[1] for c in CATEGORIES])[0],
            "is_active": profile != "churned",
            "_profile": profile,
            "_churn_date": churn_date,
        })
    return tuple(out)


@lru_cache(maxsize=1)
def products() -> tuple:
    rng = random.Random(_seed(CATALOG_SEED + "-products"))
    sell = sellers()
    # Taille de catalogue par vendeur (log-normale), normalisée à N_PRODUCTS
    sizes = [rng.lognormvariate(0, 0.9) for _ in sell]
    cum, total = [], 0.0
    for s in sizes:
        total += s
        cum.append(total)
    cat_names = [c[0] for c in CATEGORIES]
    cat_weights = [c[1] for c in CATEGORIES]
    medians = {c[0]: c[2] for c in CATEGORIES}
    out = []
    for pid in range(1, N_PRODUCTS + 1):
        seller = rng.choices(sell, cum_weights=cum)[0]
        category = seller["main_category"] if rng.random() < 0.75 else rng.choices(cat_names, cat_weights)[0]
        price = round(max(0.9, rng.lognormvariate(0, 0.55) * medians[category]), 2)
        noun = rng.choice(PRODUCT_NOUNS[category])
        out.append({
            "product_id": pid,
            "name": f"{noun} {rng.choice(ADJECTIVES)}",
            "category": category,
            "seller_id": seller["seller_id"],
            "price": price,
            "created_at": (LAUNCH_DATE + timedelta(days=rng.randint(-200, 600))).isoformat(),
        })
    return tuple(out)


@lru_cache(maxsize=1)
def _products_by_seller() -> dict:
    by = {}
    for p in products():
        by.setdefault(p["seller_id"], []).append(p)
    return by


def _seller_weights(day: date) -> list:
    """Popularité des vendeurs (loi de Zipf) + profil d'activité."""
    weights = []
    for rank, s in enumerate(sellers(), start=1):
        w = 1.0 / (rank ** 0.75)
        if s["_profile"] == "low":
            w *= 0.03
        elif s["_profile"] == "churned" and day >= s["_churn_date"]:
            w = 0.0
        weights.append(w)
    return weights


WEEKDAY_FACTOR = [0.94, 0.91, 0.96, 0.99, 1.03, 1.07, 1.10]           # lundi → dimanche
MONTH_FACTOR = {1: 0.86, 2: 0.90, 3: 0.97, 4: 0.98, 5: 1.0, 6: 0.97, 7: 0.92,
                8: 0.88, 9: 1.0, 10: 1.02, 11: 1.22, 12: 1.30}
HOUR_WEIGHTS = [1, 0.6, 0.3, 0.2, 0.2, 0.4, 1, 2, 3, 4, 4.5, 5, 6.5, 6.5, 5,
                4.5, 4.5, 5, 6, 7.5, 8, 7, 5, 2.5]


def orders_for(day: date) -> list:
    """Commandes d'une journée — déterministes : seed = md5(date)."""
    if day < LAUNCH_DATE:
        return []
    ds = day.isoformat()
    rng = random.Random(_seed(ds))

    n = BASE_ORDERS_PER_DAY * WEEKDAY_FACTOR[day.weekday()] * MONTH_FACTOR[day.month]
    n *= 1 + 0.004 * ((day - LAUNCH_DATE).days / 30)          # croissance ~0,4 %/mois
    n *= rng.uniform(0.96, 1.04)
    if rng.random() < 0.06:                                     # ~6 % des jours : chute de ventes
        n *= rng.uniform(0.45, 0.65)
    n = int(n)

    sell = sellers()
    by_seller = _products_by_seller()
    weights = _seller_weights(day)
    chosen_sellers = rng.choices(sell, weights=weights, k=n)
    hours = rng.choices(range(24), weights=HOUR_WEIGHTS, k=n)

    out = []
    for i, (seller, hour) in enumerate(zip(chosen_sellers, hours), start=1):
        product = rng.choice(by_seller[seller["seller_id"]])
        qty = rng.choices([1, 2, 3, 4, 5], [0.66, 0.24, 0.06, 0.03, 0.01])[0]
        unit_price = product["price"]
        if rng.random() < 0.12:                                 # promotion
            unit_price = round(unit_price * rng.uniform(0.7, 0.9), 2)
        r = rng.random()
        if r < 0.0004:                                          # prix suspect : erreur de saisie ×100
            unit_price = round(unit_price * 100, 2)
        elif r < 0.0007:                                        # prix suspect : quasi gratuit
            unit_price = 0.01
        ts = datetime(day.year, day.month, day.day, hour, rng.randint(0, 59), rng.randint(0, 59))
        out.append({
            "order_id": f"ORD-{day:%Y%m%d}-{i:06d}",
            "order_ts": ts.isoformat(),
            "customer_id": rng.randint(1, N_CUSTOMERS),
            "seller_id": seller["seller_id"],
            "product_id": product["product_id"],
            "quantity": qty,
            "unit_price": unit_price,
            "total_amount": round(unit_price * qty, 2),
            "status": rng.choices(["paid", "shipped", "delivered", "cancelled", "refunded"],
                                  [0.10, 0.20, 0.645, 0.04, 0.015])[0],
            "payment_method": rng.choices(["card", "paypal", "apple_pay", "bank_transfer"],
                                          [0.70, 0.18, 0.08, 0.04])[0],
            "shipping_country": rng.choices(["FR", "BE", "CH", "LU", "DE", "ES"],
                                            [0.82, 0.07, 0.03, 0.02, 0.03, 0.03])[0],
        })
    return out


def customer(cid: int) -> dict:
    rng = random.Random(_seed(f"customer-{cid}"))
    first = rng.choice(FIRST_NAMES)
    last = rng.choice(SURNAMES)
    country = rng.choices(["FR", "BE", "CH", "LU", "DE", "ES"], [0.82, 0.07, 0.03, 0.02, 0.03, 0.03])[0]
    return {
        "customer_id": cid,
        "first_name": first,
        "last_name": last,
        "email": f"{first}.{last}.{cid}@example.com".lower(),
        "country": country,
        "signup_date": (LAUNCH_DATE + timedelta(days=rng.randint(0, 900))).isoformat(),
    }


def customers(limit: int) -> list:
    return [customer(cid) for cid in range(1, limit + 1)]


def public(record: dict) -> dict:
    """Retire les champs internes (préfixés par _) avant exposition."""
    return {k: v for k, v in record.items() if not k.startswith("_")}
