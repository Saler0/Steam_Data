from __future__ import annotations

from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Sequence

from pymongo.errors import PyMongoError

from config import DECISION_RULES
from db.mongodb import MongoDBClient

PriceRuleResult = Dict[str, Any]


class DecisionRulesService:
    """Evaluates decision rules for PoC outputs."""

    def __init__(self, mongo_client: MongoDBClient) -> None:
        self.mongo_client = mongo_client
        self._allowed_platforms = {"windows", "mac", "linux"}

    def evaluate_price_rule(
        self,
        client_price: Optional[float],
        neighbor_appids: Sequence[Any],
        full_content_included: Any = False,
    ) -> PriceRuleResult:
        """Classify the client price compared to neighbor prices."""
        result: PriceRuleResult = {
            "label": "sin_datos",
            "client_price": client_price,
            "neighbor_median_price": None,
            "neighbor_prices_count": 0,
            "tag": "neutro",
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

        include_full_content = False
        if isinstance(full_content_included, bool):
            include_full_content = full_content_included
        elif isinstance(full_content_included, str):
            include_full_content = full_content_included.strip().lower() in {"true", "1", "yes", "si", "on"}
        elif isinstance(full_content_included, (int, float)):
            include_full_content = full_content_included != 0

        if client_price < cheap_threshold:
            label = "barato"
        elif client_price <= normal_threshold:
            label = "normal"
        else:
            label = "caro"

        if label == "caro" and include_full_content:
            label = "alto justificado"

        result.update(
            {
                "label": label,
                "thresholds": {
                    "cheap": cheap_threshold,
                    "normal": normal_threshold,
                },
            }
        )
        tag_by_label = {
            "barato": "fortaleza",
            "alto justificado": "fortaleza",
            "caro": "debilidad",
            "normal": "neutro",
            "sin_datos": "neutro",
        }
        result["tag"] = tag_by_label.get(label, "neutro")
        return result

    def evaluate_platform_rule(
        self,
        client_platforms: Any,
        neighbor_appids: Sequence[Any],
    ) -> Dict[str, Any]:
        """Return qualitative label and context comparing platform support to neighbors."""
        client_count = self._count_platforms(client_platforms)
        neighbor_counts = self._fetch_neighbor_platform_counts(neighbor_appids)

        result: Dict[str, Any] = {
            "label": "sin_datos",
            "client_platforms_count": client_count,
            "total_neighbors": len(neighbor_counts),
            "neighbors_with_more_platforms": 0,
            "neighbors_with_equal_platforms": 0,
            "neighbors_with_less_platforms": 0,
            "neighbor_max_platforms": max(neighbor_counts) if neighbor_counts else None,
            "tag": "neutro",
        }

        if not neighbor_counts:
            return result

        more = sum(1 for count in neighbor_counts if count > client_count)
        equal = sum(1 for count in neighbor_counts if count == client_count)
        less = sum(1 for count in neighbor_counts if count < client_count)

        result["neighbors_with_more_platforms"] = more
        result["neighbors_with_equal_platforms"] = equal
        result["neighbors_with_less_platforms"] = less

        if more > (len(neighbor_counts) / 2):
            result["label"] = "soporte limitado"
            result["tag"] = "debilidad"
        else:
            result["label"] = "soporte bueno"
            result["tag"] = "fortaleza"

        return result

    def evaluate_ram_rule(
        self,
        client_ram_gb: Any,
        neighbor_appids: Sequence[Any],
    ) -> PriceRuleResult:
        """Compare client RAM requirement against the neighbor median."""
        result: PriceRuleResult = {
            "label": "sin_datos",
            "client_ram_gb": client_ram_gb,
            "neighbor_median_ram_gb": None,
            "neighbor_ram_values_count": 0,
            "tag": "neutro",
        }

        if client_ram_gb in (None, ""):
            return result

        try:
            client_ram_value = float(client_ram_gb)
        except (TypeError, ValueError):
            return result

        result["client_ram_gb"] = client_ram_value

        neighbor_ram_values = self._fetch_neighbor_ram_requirements(neighbor_appids)
        result["neighbor_ram_values_count"] = len(neighbor_ram_values)
        if not neighbor_ram_values:
            return result

        neighbor_median = median(neighbor_ram_values)
        result["neighbor_median_ram_gb"] = neighbor_median
        result["label"] = "barrera tecnica" if client_ram_value > neighbor_median else "sin barrera tecnica"
        result["tag"] = "debilidad" if result["label"] == "barrera tecnica" else "fortaleza"

        return result

    def evaluate_size_rule(
        self,
        client_install_size_gb: Any,
        neighbor_appids: Sequence[Any],
    ) -> Dict[str, Any]:
        """Classify the install size compared to neighbor requirements with context."""
        result: Dict[str, Any] = {
            "label": "sin_datos",
            "client_install_size_gb": None,
            "neighbor_percentile_75": None,
            "neighbor_sizes_count": 0,
            "tag": "neutro",
        }

        if client_install_size_gb in (None, ""):
            return result
        try:
            client_size_value = float(client_install_size_gb)
        except (TypeError, ValueError):
            return result

        result["client_install_size_gb"] = client_size_value

        neighbor_sizes = self._fetch_neighbor_install_sizes(neighbor_appids)
        result["neighbor_sizes_count"] = len(neighbor_sizes)
        if not neighbor_sizes:
            return result

        percentile_75 = self._compute_percentile(neighbor_sizes, 75.0)
        if percentile_75 is None:
            return result

        result["neighbor_percentile_75"] = percentile_75
        if client_size_value > percentile_75:
            result["label"] = "juego muy pesado"
            result["tag"] = "debilidad"
        else:
            result["label"] = "juego liviano"
            result["tag"] = "fortaleza"

        return result

    def evaluate_steam_deck_rule(
        self,
        client_steam_deck_compatible: Any,
        neighbor_appids: Sequence[Any],
    ) -> Dict[str, Any]:
        """Evaluate visibility advantage based on Steam Deck compatibility."""
        result: Dict[str, Any] = {
            "label": "sin_datos",
            "client_steam_deck": None,
            "neighbors_total": 0,
            "neighbors_with_steam_deck": 0,
            "tag": "neutro",
        }

        client_flag: Optional[bool] = None
        if isinstance(client_steam_deck_compatible, bool):
            client_flag = client_steam_deck_compatible
        elif isinstance(client_steam_deck_compatible, (int, float)):
            client_flag = bool(client_steam_deck_compatible)
        elif isinstance(client_steam_deck_compatible, str):
            text = client_steam_deck_compatible.strip().lower()
            if text:
                if text in {"true", "1", "yes", "on", "si"}:
                    client_flag = True
                elif text in {"false", "0", "no", "off"}:
                    client_flag = False

        if client_flag is None:
            return result

        result["client_steam_deck"] = client_flag
        if client_flag:
            result["label"] = "mayor visibilidad"
            result["tag"] = "fortaleza"
            return result

        neighbor_flags = self._fetch_neighbor_steam_deck_flags(neighbor_appids)
        total_neighbors = len(neighbor_flags)
        result["neighbors_total"] = total_neighbors
        if total_neighbors == 0:
            return result

        with_steam_deck = sum(1 for flag in neighbor_flags if flag is True)
        result["neighbors_with_steam_deck"] = with_steam_deck

        if with_steam_deck > 0:
            result["label"] = "menor visibilidad"
            result["tag"] = "debilidad"
        else:
            result["label"] = "vecinos sin steam deck"
            result["tag"] = "neutro"

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

        price_field = 'price'
        projection = {price_field: 1, '_id': 0}
        collection_name = 'juegos_steam'
        try:
            collection = self.mongo_client.get_collection(collection_name)
            cursor = collection.find(query, projection)
        except PyMongoError:
            return prices

        price_key = price_field
        for doc in cursor:
            value = doc.get(price_key)
            if value in (None, ""):
                continue
            try:
                prices.append(float(value))
            except (TypeError, ValueError):
                continue
        return prices

    def _fetch_neighbor_ram_requirements(self, neighbor_appids: Sequence[Any]) -> List[float]:
        ram_values: List[float] = []
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
            return ram_values

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
            return ram_values

        if len(query_clauses) == 1:
            query: Dict[str, Any] = query_clauses[0]
        else:
            query = {"$or": query_clauses}

        ram_field = "RAM_req_GB"
        projection = {ram_field: 1, "_id": 0}
        collection_name = "juegos_steam"
        try:
            collection = self.mongo_client.get_collection(collection_name)
            cursor = collection.find(query, projection)
        except PyMongoError:
            return ram_values

        for doc in cursor:
            value = doc.get(ram_field)
            if value in (None, ""):
                continue
            try:
                ram_values.append(float(value))
            except (TypeError, ValueError):
                continue

        return ram_values

    def _fetch_neighbor_install_sizes(self, neighbor_appids: Sequence[Any]) -> List[float]:
        install_sizes: List[float] = []
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
            return install_sizes

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
            return install_sizes

        if len(query_clauses) == 1:
            query: Dict[str, Any] = query_clauses[0]
        else:
            query = {"$or": query_clauses}

        size_field = "almacenamiento_req_GB"
        projection = {size_field: 1, "_id": 0}
        collection_name = "juegos_steam"
        try:
            collection = self.mongo_client.get_collection(collection_name)
            cursor = collection.find(query, projection)
        except PyMongoError:
            return install_sizes

        for doc in cursor:
            value = doc.get(size_field)
            if value in (None, ""):
                continue
            try:
                install_sizes.append(float(value))
            except (TypeError, ValueError):
                continue

        return install_sizes

    def _fetch_neighbor_platform_counts(self, neighbor_appids: Sequence[Any]) -> List[int]:
        counts: List[int] = []
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
            return counts
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
            return counts
        query: Dict[str, Any]
        if len(query_clauses) == 1:
            query = query_clauses[0]
        else:
            query = {"$or": query_clauses}
        projection = {"platforms": 1, "_id": 0}
        collection_name = "juegos_steam"
        try:
            collection = self.mongo_client.get_collection(collection_name)
            cursor = collection.find(query, projection)
        except PyMongoError:
            return counts
        for doc in cursor:
            counts.append(self._count_platforms(doc.get("platforms")))
        return [count for count in counts if count is not None]

    def _count_platforms(self, platforms: Any) -> int:
        if not platforms:
            return 0
        if isinstance(platforms, dict):
            return sum(1 for value in platforms.values() if bool(value))
        if isinstance(platforms, (list, set, tuple)):
            normalized = {
                str(item).strip().lower()
                for item in platforms
                if item is not None and str(item).strip()
            }
            return sum(1 for plat in normalized if plat in self._allowed_platforms)
        if isinstance(platforms, str):
            tokens = {token.strip().lower() for token in platforms.split(",")}
            return sum(1 for token in tokens if token in self._allowed_platforms)
        return 0

    def _compute_percentile(self, values: Sequence[float], percentile: float) -> Optional[float]:
        if not values:
            return None
        if percentile < 0 or percentile > 100:
            return None
        sorted_values = sorted(values)
        if len(sorted_values) == 1:
            return sorted_values[0]
        position = (len(sorted_values) - 1) * (percentile / 100.0)
        lower_index = int(position)
        upper_index = min(lower_index + 1, len(sorted_values) - 1)
        interpolation = position - lower_index
        lower_value = sorted_values[lower_index]
        upper_value = sorted_values[upper_index]
        return lower_value + (upper_value - lower_value) * interpolation

    def _fetch_neighbor_steam_deck_flags(self, neighbor_appids: Sequence[Any]) -> List[bool]:
        flags: List[bool] = []
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
            return flags

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
            return flags

        if len(query_clauses) == 1:
            query: Dict[str, Any] = query_clauses[0]
        else:
            query = {"$or": query_clauses}

        projection = {"Steam_Deck": 1, "_id": 0}
        collection_name = "juegos_steam"
        try:
            collection = self.mongo_client.get_collection(collection_name)
            cursor = collection.find(query, projection)
        except PyMongoError:
            return flags

        for doc in cursor:
            value = doc.get("Steam_Deck")
            if isinstance(value, bool):
                flags.append(value)
            elif isinstance(value, (int, float)):
                flags.append(bool(value))
            elif isinstance(value, str):
                text = value.strip().lower()
                if text in {"true", "1", "yes", "on", "si"}:
                    flags.append(True)
                elif text in {"false", "0", "no", "off"}:
                    flags.append(False)

        return flags
