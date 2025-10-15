from __future__ import annotations

from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Sequence, Tuple

from pymongo.errors import PyMongoError

from db.mongodb import MongoDBClient


class NeighborRatingService:
    """Aggregates review-based ratings for Steam neighbors."""

    def __init__(self, mongo_client: MongoDBClient, collection_name: str = "steam_reviews") -> None:
        self.mongo_client = mongo_client
        self.collection_name = collection_name

    def compute_ratings(self, neighbors: Sequence[MutableMapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Compute rating metrics for each neighbor in the provided sequence."""
        appid_map = self._collect_appids(neighbors)
        if not appid_map:
            return {}

        review_docs = self._fetch_reviews(appid_map)
        if not review_docs:
            return {}

        aggregates: Dict[str, Dict[str, Any]] = {}
        for doc in review_docs:
            appid_key = self._extract_appid_key(doc, appid_map)
            if appid_key is None:
                continue

            voted_up = self._extract_bool(doc.get("voted_up"))
            weight_score = self._compute_weighted_score(doc)
            if weight_score is None:
                continue

            entry = aggregates.setdefault(
                appid_key,
                {
                    "positive_reviews": 0,
                    "negative_reviews": 0,
                    "weighted_positive": 0.0,
                    "weighted_negative": 0.0,
                },
            )

            if voted_up is True:
                entry["positive_reviews"] += 1
                entry["weighted_positive"] += weight_score
            elif voted_up is False:
                entry["negative_reviews"] += 1
                entry["weighted_negative"] += weight_score
            else:
                # Unknown sentiment, skip aggregation.
                continue

        ratings: Dict[str, Dict[str, Any]] = {}
        for key, entry in aggregates.items():
            total_weight = entry["weighted_positive"] + entry["weighted_negative"]
            total_reviews = entry["positive_reviews"] + entry["negative_reviews"]
            if total_weight > 0:
                positive_percentage = (entry["weighted_positive"] / total_weight) * 100.0
            elif total_reviews > 0:
                # fallback to unweighted ratio when scores failed but counts exist
                positive_percentage = (entry["positive_reviews"] / total_reviews) * 100.0
            else:
                positive_percentage = None

            ratings[key] = {
                "positive_reviews": entry["positive_reviews"],
                "negative_reviews": entry["negative_reviews"],
                "total_reviews": total_reviews,
                "weighted_positive": round(entry["weighted_positive"], 4),
                "weighted_negative": round(entry["weighted_negative"], 4),
                "positive_percentage": round(positive_percentage, 2) if positive_percentage is not None else None,
            }

        return ratings

    def _collect_appids(self, neighbors: Sequence[MutableMapping[str, Any]]) -> Dict[str, Tuple[str, Any]]:
        appid_map: Dict[str, Tuple[str, Any]] = {}
        for neighbor in neighbors:
            if not isinstance(neighbor, MutableMapping):
                continue
            candidate = neighbor.get("appid")
            if candidate is None:
                continue
            str_key = str(candidate).strip()
            if not str_key:
                continue
            if str_key not in appid_map:
                appid_map[str_key] = (str_key, candidate)
        return appid_map

    def _fetch_reviews(self, appid_map: Dict[str, Tuple[str, Any]]) -> List[Dict[str, Any]]:
        str_ids = list(appid_map.keys())
        int_ids: List[int] = []
        for key, original in appid_map.values():
            try:
                int_ids.append(int(original))
            except (TypeError, ValueError):
                continue

        query_clauses: List[Dict[str, Any]] = []
        if int_ids:
            query_clauses.append({"appid": {"$in": int_ids}})
        if str_ids:
            query_clauses.append({"appid": {"$in": str_ids}})

        if not query_clauses:
            return []

        if len(query_clauses) == 1:
            query: Dict[str, Any] = query_clauses[0]
        else:
            query = {"$or": query_clauses}

        projection = {
            "_id": 0,
            "appid": 1,
            "voted_up": 1,
            "votes_up": 1,
            "votes_funny": 1,
        }

        try:
            collection = self.mongo_client.get_collection(self.collection_name)
            cursor = collection.find(query, projection)
            return list(cursor)
        except PyMongoError:
            return []

    def _extract_appid_key(self, doc: Dict[str, Any], appid_map: Dict[str, Tuple[str, Any]]) -> Optional[str]:
        raw_appid = doc.get("appid")
        if raw_appid is None:
            return None

        str_key = str(raw_appid).strip()
        if not str_key:
            return None

        if str_key in appid_map:
            return appid_map[str_key][0]

        try:
            as_int = int(raw_appid)
        except (TypeError, ValueError):
            return None

        int_key = str(as_int)
        if int_key in appid_map:
            return appid_map[int_key][0]
        return None

    def _compute_weighted_score(self, doc: Dict[str, Any]) -> Optional[float]:
        votes_up = self._safe_int(doc.get("votes_up"))
        votes_funny = self._safe_int(doc.get("votes_funny"))

        multiplier = 0
        if votes_up is not None:
            multiplier += votes_up
        if votes_funny is not None:
            multiplier += votes_funny

        if multiplier <= 0:
            multiplier = 1
        return float(multiplier)

    def _extract_bool(self, value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            text = value.strip().lower()
            if text in {"true", "1", "yes", "on", "si"}:
                return True
            if text in {"false", "0", "no", "off"}:
                return False
        return None

    def _safe_int(self, value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
