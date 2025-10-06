from __future__ import annotations

from statistics import median
from typing import Any, Dict, List, Optional, Sequence

from pymongo.errors import PyMongoError

from config import COLLECTIONS, DECISION_RULES, FIELDS
from db.mongodb import MongoDBClient

PriceRuleResult = Dict[str, Any]


class DecisionRulesService:
    """Evaluates decision rules for PoC outputs."""

    def __init__(self, mongo_client: MongoDBClient) -> None:
        self.mongo_client = mongo_client

    def evaluate_price_rule(
        self,
        client_price: Optional[float],
        neighbor_appids: Sequence[Any],
    ) -> PriceRuleResult:
        """Classify the client price compared to neighbor prices."""
        result: PriceRuleResult = {
            "label": "sin_datos",
            "client_price": client_price,
            "neighbor_median_price": None,
            "neighbor_prices_count": 0,
        }

        if client_price is None:
            return result

        neighbor_prices = self._fetch_neighbor_prices(neighbor_appids)
        result["neighbor_prices_count"] = len(neighbor_prices)

        if not neighbor_prices:
            return result

        neighbor_median = median(neighbor_prices)
        result["neighbor_median_price"] = neighbor_median

        cheap_margin_pct = DECISION_RULES.get("price_margin_pct", 0.10)
        normal_margin_pct = DECISION_RULES.get("price_normal_margin_pct", cheap_margin_pct)
        if normal_margin_pct < cheap_margin_pct:
            normal_margin_pct = cheap_margin_pct

        cheap_threshold = neighbor_median + client_price * cheap_margin_pct
        normal_threshold = neighbor_median + client_price * normal_margin_pct

        if client_price < cheap_threshold:
            label = "barato"
        elif client_price <= normal_threshold:
            label = "normal"
        else:
            label = "caro"

        result.update(
            {
                "label": label,
                "thresholds": {
                    "cheap": cheap_threshold,
                    "normal": normal_threshold,
                },
            }
        )
        return result

    def _fetch_neighbor_prices(self, neighbor_appids: Sequence[Any]) -> List[float]:
        prices: List[float] = []
        unique_ids: List[str] = []
        for appid in neighbor_appids:
            candidate = appid.get("appid") if isinstance(appid, dict) else appid
            if candidate is None:
                continue
            text_id = str(candidate).strip()
            if not text_id:
                continue
            unique_ids.append(text_id)

        if not unique_ids:
            return prices

        str_ids = list({uid for uid in unique_ids})
        int_ids: List[int] = []
        for uid in str_ids:
            try:
                int_ids.append(int(uid))
            except ValueError:
                continue

        query_clauses: List[Dict[str, Any]] = []
        if int_ids:
            query_clauses.append({"appid": {"$in": int_ids}})
        if str_ids:
            query_clauses.append({"appid": {"$in": str_ids}})

        if not query_clauses:
            return prices

        if len(query_clauses) == 1:
            query: Dict[str, Any] = query_clauses[0]
        else:
            query = {"$or": query_clauses}

        projection = {FIELDS.get("price"): 1, "_id": 0}
        collection_name = (
            COLLECTIONS.get('juegos_steam')
        )
        try:
            collection = self.mongo_client.get_collection(collection_name)
            cursor = collection.find(query, projection)
        except PyMongoError:
            return prices

        price_key = FIELDS.get("steam_price", "price")
        for doc in cursor:
            value = doc.get(price_key)
            if value in (None, ""):
                continue
            try:
                prices.append(float(value))
            except (TypeError, ValueError):
                continue
        return prices
