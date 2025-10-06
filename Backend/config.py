from __future__ import annotations

# Thresholds for decision rules
DECISION_RULES = {
    'price_margin_pct': 0.10,
    'price_normal_margin_pct': 0.10,
}

# Mongo collections
COLLECTIONS = {
    'client_games': 'juegos_clientes',
    'steam_games': 'juegos_steam',
}

# Fields used by decision-rule pipelines
FIELDS = {
    'steam_price': 'price',
}
