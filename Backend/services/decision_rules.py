from __future__ import annotations

from statistics import median
from datetime import datetime, date, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Mapping

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
            "label": "no data",
            "client_price": client_price,
            "neighbor_median_price": None,
            "neighbor_prices_count": 0,
            "tag": "neutral",
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
            label = "cheap"
        elif client_price <= normal_threshold:
            label = "fair"
        else:
            label = "expensive"

        if label == "expensive" and include_full_content:
            label = "expensive justified"

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
            "cheap": "advantage",
            "expensive justified": "advantage",
            "expensive": "weakness",
            "fair": "neutral",
            "no data": "neutral",
        }
        result["tag"] = tag_by_label.get(label, "neutral")
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
            "label": "no data",
            "client_platforms_count": client_count,
            "total_neighbors": len(neighbor_counts),
            "neighbors_with_more_platforms": 0,
            "neighbors_with_equal_platforms": 0,
            "neighbors_with_less_platforms": 0,
            "neighbor_max_platforms": max(neighbor_counts) if neighbor_counts else None,
            "tag": "neutral",
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
            result["label"] = "limited support"
            result["tag"] = "weakness"
        else:
            result["label"] = "good support"
            result["tag"] = "advantage"

        return result

    def evaluate_ram_rule(
        self,
        client_ram_gb: Any,
        neighbor_appids: Sequence[Any],
    ) -> PriceRuleResult:
        """Compare client RAM requirement against the neighbor median."""
        result: PriceRuleResult = {
            "label": "no data",
            "client_ram_gb": client_ram_gb,
            "neighbor_median_ram_gb": None,
            "neighbor_ram_values_count": 0,
            "tag": "neutral",
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
        result["label"] = "technical barrier" if client_ram_value > neighbor_median else "no technical barrier"
        result["tag"] = "weakness" if result["label"] == "technical barrier" else "advantage"

        return result

    def evaluate_size_rule(
        self,
        client_install_size_gb: Any,
        neighbor_appids: Sequence[Any],
    ) -> Dict[str, Any]:
        """Classify the install size compared to neighbor requirements with context."""
        result: Dict[str, Any] = {
            "label": "no_data",
            "client_install_size_gb": None,
            "neighbor_percentile_75": None,
            "neighbor_sizes_count": 0,
            "tag": "neutral",
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
            result["label"] = "very heavy game"
            result["tag"] = "weakness"
        else:
            result["label"] = "lightweight game"
            result["tag"] = "advantage"

        return result

    def evaluate_steam_deck_rule(
        self,
        client_steam_deck_compatible: Any,
        neighbor_appids: Sequence[Any],
    ) -> Dict[str, Any]:
        """Evaluate visibility advantage based on Steam Deck compatibility."""
        result: Dict[str, Any] = {
            "label": "no data",
            "client_steam_deck": None,
            "neighbors_total": 0,
            "neighbors_with_steam_deck": 0,
            "tag": "neutral",
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
            result["label"] = "higher visibility"
            result["tag"] = "advantage"
            return result

        neighbor_flags = self._fetch_neighbor_steam_deck_flags(neighbor_appids)
        total_neighbors = len(neighbor_flags)
        result["neighbors_total"] = total_neighbors
        if total_neighbors == 0:
            return result

        with_steam_deck = sum(1 for flag in neighbor_flags if flag is True)
        result["neighbors_with_steam_deck"] = with_steam_deck

        if with_steam_deck > 0:
            result["label"] = "lower visibility"
            result["tag"] = "weakness"
        else:
            result["label"] = "neighbors without steam deck"
            result["tag"] = "neutral"

        return result

    def evaluate_segment_rule(
        self,
        neighbors_with_ratings: Sequence[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        """Classify segment reputation based on aggregated neighbor ratings."""
        total_reviews = 0
        weighted_positive_sum = 0.0

        for neighbor in neighbors_with_ratings:
            rating = neighbor.get("rating")
            if not isinstance(rating, Mapping):
                continue

            total = rating.get("total_reviews")
            if isinstance(total, (int, float)):
                try:
                    review_count = max(int(total), 0)
                except (TypeError, ValueError):
                    review_count = 0
            else:
                review_count = 0

            total_reviews += review_count

            pct = rating.get("positive_percentage")
            if isinstance(pct, (int, float)):
                weighted_positive_sum += float(pct) * review_count

        average_pct: Optional[float]
        if total_reviews > 0:
            average_pct = weighted_positive_sum / total_reviews
        else:
            average_pct = None

        result: Dict[str, Any] = {
            "label": "",
            "average_positive_percentage": round(average_pct, 2) if average_pct is not None else None,
            "total_reviews": int(total_reviews),
        }

        if total_reviews < 1000:
            result["label"] = "sample size low"
            return result

        avg_value = average_pct if average_pct is not None else 0.0
        if avg_value < 70.0:
            result["label"] = "segment historically negative"
            return result

        result["label"] = "segment high expectations"
        return result

    def evaluate_competencia_rule(
        self,
        neighbor_appids: Sequence[Any],
    ) -> str:
        """Evaluate recent competitive activity based on neighbor release dates."""
        release_map = self._fetch_neighbor_release_dates(neighbor_appids)
        if not release_map:
            return "market disinterest"

        today = date.today()
        cutoff_date = today - timedelta(days=365)
        for rel_date in release_map.values():
            if rel_date and rel_date >= cutoff_date:
                return "recent high competition"
        return "market disinterest"

    def build_release_info_map(
        self,
        neighbor_appids: Sequence[Any],
    ) -> Dict[str, Dict[str, Any]]:
        """Return release date info and age (in years) per neighbor appid."""
        release_map = self._fetch_neighbor_release_dates(neighbor_appids)
        today = date.today()
        info: Dict[str, Dict[str, Any]] = {}

        def _normalized_appid(entry: Any) -> Optional[str]:
            if isinstance(entry, dict):
                candidate = entry.get("appid")
            else:
                candidate = entry
            if candidate is None:
                return None
            text = str(candidate).strip()
            return text or None

        for entry in neighbor_appids:
            appid_key = _normalized_appid(entry)
            if not appid_key:
                continue
            rel_date = release_map.get(appid_key)
            if rel_date is None:
                info[appid_key] = {
                    "release_date": None,
                    "age_years": None,
                }
                continue
            age_days = (today - rel_date).days
            age_years = round(max(age_days, 0) / 365.25, 2)
            info[appid_key] = {
                "release_date": rel_date.isoformat(),
                "age_years": age_years,
            }
        return info

    def evaluate_segment_saturation_age_rule(
        self,
        neighbor_appids: Sequence[Any],
    ) -> Dict[str, Any]:
        """Evaluate saturation vs. age profile for a neighbor cluster."""
        release_map = self._fetch_neighbor_release_dates(neighbor_appids)
        if not release_map:
            return {
                "label": "market disinterest",
                "average_age_years": None,
                "neighbor_count": len(neighbor_appids),
            }

        valid_dates = [d for d in release_map.values() if isinstance(d, date)]
        neighbor_count = len(neighbor_appids)

        average_age_years: Optional[float]
        if valid_dates:
            today = date.today()
            total_days = sum((today - rel_date).days for rel_date in valid_dates)
            average_age_years = total_days / len(valid_dates) / 365.25
        else:
            average_age_years = None

        result: Dict[str, Any] = {
            "label": "",
            "average_age_years": round(average_age_years, 2) if average_age_years is not None else None,
            "neighbor_count": neighbor_count,
        }

        if average_age_years is None:
            result["label"] = "insufficient_data"
            return result

        if neighbor_count >= 20 and average_age_years >= 6:
            result["label"] = "Very saturated and aged segment"
        elif neighbor_count >= 20 and average_age_years < 6:
            result["label"] = "Very saturated and new segment"
        elif neighbor_count < 20 and average_age_years >= 6:
            result["label"] = "Low saturated and aged segment"
        else:
            result["label"] = "Low saturated and new segment"

        return result


    def evaluate_publisher_rule(
        self,
        neighbor_appids: Sequence[Any],
    ) -> Dict[str, Any]:
        """Determine publisher concentration among neighbors."""
        total_neighbors = len(neighbor_appids)
        if total_neighbors == 0:
            return {"label": "no data", "top_publishers": [], "total_neighbors": 0}

        publisher_counts = self._aggregate_company_counts(neighbor_appids, field="publishers")
        top_publishers = sorted(
            (
                {
                    "publisher": name,
                    "neighbor_count": count,
                    "share": count / total_neighbors,
                }
                for name, count in publisher_counts.items()
            ),
            key=lambda entry: entry["neighbor_count"],
            reverse=True,
        )

        label = "no data"
        if top_publishers:
            if top_publishers[0]["share"] >= 0.5:
                label = f"concentrated on {top_publishers[0]['publisher']}"
            else:
                label = "diversified"

        return {
            "label": label,
            "top_publishers": top_publishers,
            "total_neighbors": total_neighbors,
        }

    def evaluate_dev_rule(
        self,
        neighbor_appids: Sequence[Any],
    ) -> Dict[str, Any]:
        """Determine developer concentration among neighbors."""
        total_neighbors = len(neighbor_appids)
        if total_neighbors == 0:
            return {"label": "no data", "top_developers": [], "total_neighbors": 0}

        developer_counts = self._aggregate_company_counts(neighbor_appids, field="developers")
        top_devs = sorted(
            (
                {
                    "developer": name,
                    "neighbor_count": count,
                    "share": count / total_neighbors,
                }
                for name, count in developer_counts.items()
            ),
            key=lambda entry: entry["neighbor_count"],
            reverse=True,
        )

        label = "no data"
        if top_devs:
            if top_devs[0]["share"] >= 0.5:
                label = f"concentrated on {top_devs[0]['developer']}"
            else:
                label = "diversified"

        return {
            "label": label,
            "top_developers": top_devs,
            "total_neighbors": total_neighbors,
        }

    def evaluate_activity_rule(
        self,
        neighbor_appids: Sequence[Any],
    ) -> Dict[str, Dict[str, Any]]:
        """Classify neighbors as active/inactive based on recent playtime."""
        threshold_cfg = DECISION_RULES.get("activity_inactive_threshold_hours", 10.0)
        try:
            threshold_hours = float(threshold_cfg)
        except (TypeError, ValueError):
            threshold_hours = 10.0

        normalized_ids: Dict[str, str] = {}
        str_ids: set[str] = set()
        int_ids: set[int] = set()
        for entry in neighbor_appids:
            candidate = entry.get("appid") if isinstance(entry, dict) else entry
            if candidate is None:
                continue
            text_id = str(candidate).strip()
            if not text_id:
                continue
            normalized_ids[text_id] = text_id
            str_ids.add(text_id)
            try:
                int_ids.add(int(text_id))
            except ValueError:
                continue

        if not normalized_ids:
            return {}

        query_clauses: List[Dict[str, Any]] = []
        if int_ids:
            int_list = list(int_ids)
            query_clauses.append({"appid": {"$in": int_list}})
            query_clauses.append({"app_id": {"$in": int_list}})
        if str_ids:
            str_list = list(str_ids)
            query_clauses.append({"appid": {"$in": str_list}})
            query_clauses.append({"app_id": {"$in": str_list}})

        if not query_clauses:
            return {
                appid: {
                    "label": "no data",
                    "average_playtime_hours": None,
                    "samples": 0,
                    "threshold_hours": threshold_hours,
                }
                for appid in normalized_ids
            }

        if len(query_clauses) == 1:
            query: Dict[str, Any] = query_clauses[0]
        else:
            query = {"$or": query_clauses}

        projection = {
            "appid": 1,
            "app_id": 1,
            "author.playtime_last_two_weeks": 1,
            "author_playtime_last_two_weeks": 1,
            "_id": 0,
        }

        def _safe_float(value: Any) -> Optional[float]:
            if value is None:
                return None
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    return None
                try:
                    return float(text)
                except ValueError:
                    return None
            return None

        sums: Dict[str, float] = {appid: 0.0 for appid in normalized_ids}
        counts: Dict[str, int] = {appid: 0 for appid in normalized_ids}

        try:
            collection = self.mongo_client.get_collection("steam_reviews")
            cursor = collection.find(query, projection)
        except PyMongoError:
            return {
                appid: {
                    "label": "no data",
                    "average_playtime_hours": None,
                    "samples": 0,
                    "threshold_hours": threshold_hours,
                }
                for appid in normalized_ids
            }

        for doc in cursor:
            appid_value = doc.get("appid", doc.get("app_id"))
            if appid_value is None:
                continue
            appid_key = str(appid_value).strip()
            if not appid_key or appid_key not in normalized_ids:
                continue
            author = doc.get("author") or {}
            playtime = author.get("playtime_last_two_weeks")
            if playtime is None:
                playtime = doc.get("author_playtime_last_two_weeks")
            hours = _safe_float(playtime)
            if hours is None:
                continue
            sums[appid_key] += hours
            counts[appid_key] += 1

        results: Dict[str, Dict[str, Any]] = {}
        for appid_key in normalized_ids:
            samples = counts.get(appid_key, 0)
            average = None
            if samples > 0:
                average = round(sums[appid_key] / samples, 2)
            if average is None:
                label = "no data"
            elif average < threshold_hours:
                label = "inactive game"
            else:
                label = "active game"
            results[appid_key] = {
                "label": label,
                "average_playtime_hours": average,
                "samples": samples,
                "threshold_hours": threshold_hours,
            }
        return results

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

    def _fetch_neighbor_release_dates(self, neighbor_appids: Sequence[Any]) -> Dict[str, Optional[date]]:
        releases: Dict[str, Optional[date]] = {}
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
            return releases

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
            query_clauses.append({"app_id": {"$in": int_ids}})
        if str_ids:
            query_clauses.append({"appid": {"$in": str_ids}})
            query_clauses.append({"app_id": {"$in": str_ids}})
        if not query_clauses:
            return releases

        if len(query_clauses) == 1:
            query: Dict[str, Any] = query_clauses[0]
        else:
            query = {"$or": query_clauses}

        projection = {"release_date": 1, "appid": 1, "app_id": 1, "_id": 0}
        collection_name = "juegos_steam"
        try:
            collection = self.mongo_client.get_collection(collection_name)
            cursor = collection.find(query, projection)
        except PyMongoError:
            return releases

        for doc in cursor:
            appid_value = doc.get("appid", doc.get("app_id"))
            if appid_value is None:
                continue
            appid_key = str(appid_value).strip()
            if not appid_key:
                continue
            parsed = self._parse_release_date(doc.get("release_date"))
            releases[appid_key] = parsed
        return releases

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


    def _aggregate_company_counts(self, neighbor_appids: Sequence[Any], field: str) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        canonical_names: Dict[str, str] = {}
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

        if len(query_clauses) == 1:
            query: Dict[str, Any] = query_clauses[0]
        else:
            query = {"$or": query_clauses}

        projection = {field: 1, "_id": 0}
        collection_name = "juegos_steam"
        try:
            collection = self.mongo_client.get_collection(collection_name)
            cursor = collection.find(query, projection)
        except PyMongoError:
            return counts

        for doc in cursor:
            values = doc.get(field)
            if not values:
                continue
            if isinstance(values, str):
                iterable = [values]
            else:
                iterable = values if isinstance(values, Iterable) else []
            for value in iterable:
                if value is None:
                    continue
                text = str(value).strip()
                if not text:
                    continue
                key = text.lower()
                if key not in canonical_names:
                    canonical_names[key] = text
                counts[key] = counts.get(key, 0) + 1

        return {canonical_names[key]: count for key, count in counts.items()}

    def _parse_release_date(self, value: Any) -> Optional[date]:
        if value is None:
            return None

        if isinstance(value, dict):
            # Steam API typically stores release_date as {"coming_soon": bool, "date": "May 1, 2024"}
            coming_soon = value.get("coming_soon")
            possible = value.get("date")
            if coming_soon is True and (not possible or not str(possible).strip()):
                today = date.today()
                return date(today.year, 12, 31)
            return self._parse_release_date(possible)

        if isinstance(value, (int, float)):
            try:
                return datetime.utcfromtimestamp(float(value)).date()
            except (OSError, OverflowError, ValueError):
                return None

        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            normalized = text.replace("Z", "")
            lower_text = normalized.lower()
            if "coming soon" in lower_text:
                today = date.today()
                return date(today.year, 12, 31)
            if len(normalized) == 4 and normalized.isdigit():
                try:
                    return date(int(normalized), 1, 1)
                except ValueError:
                    return None
            date_formats = [
                "%Y-%m-%d",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
                "%Y/%m/%d",
                "%B %Y",
                "%b %Y",
                "%d/%m/%Y",
                "%b %d, %Y",
                "%B %d, %Y",
                "%d %b %Y",
                "%d %B %Y",
            ]
            for fmt in date_formats:
                try:
                    return datetime.strptime(normalized, fmt).date()
                except ValueError:
                    continue
            try:
                return datetime.fromisoformat(normalized).date()
            except ValueError:
                return None
        return None

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
