"""Utility functions to generate narrative text blocks for client reports."""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence


def _first(values: Sequence[str]) -> str:
    for value in values:
        token = (value or "").strip().lower()
        if token:
            return token
    return ""


def build_topic_slug(index: int, keywords: Iterable[Any], max_tokens: int = 3) -> str:
    tokens: List[str] = []
    for value in keywords:
        token = (str(value or "").strip().lower().replace(" ", "_"))
        if token and token not in tokens:
            tokens.append(token)
        if len(tokens) >= max_tokens:
            break
    if not tokens:
        return f"topic_{index}"
    return f"{index}_" + "_".join(tokens)


def describe_metadata(name: Optional[str], genres: Sequence[str], tags: Sequence[str], modes: Sequence[str]) -> str:
    genre = _first(genres) or _first(tags) or "juego"
    loop = _first(tags[1:]) if len(tags) > 1 else "experiencias"
    mode = _first(modes) or "multijugador"
    base_name = name or "El juego"
    return f"{base_name} combina {genre} con {loop} en un enfoque {mode}."


def describe_cluster_note(saturation: Optional[str], micro_desc: str, silhouette: Optional[float]) -> str:
    parts: List[str] = []
    if saturation == "alto":
        parts.append("El cluster muestra alta saturacion competitiva")
    elif saturation == "medio":
        parts.append("El cluster presenta competencia moderada")
    else:
        parts.append("El cluster conserva espacio competitivo")
    if micro_desc:
        parts.append(micro_desc)
    if silhouette is not None:
        parts.append(f"Silhouette proxy={silhouette:.2f}")
    return ". ".join(parts) + "."


def describe_peak_reasons(event_context: Dict[str, Any], social_context: Dict[str, Any]) -> str:
    reasons: List[str] = []
    steam = event_context.get("steam") or {}
    sale = steam.get("sale") or {}
    if steam.get("dlc"):
        reasons.append("DLC")
    if sale.get("active"):
        reasons.append("rebaja")
    youtube = (social_context.get("youtube") or {})
    twitch = (social_context.get("twitch") or {})
    if youtube.get("z_views", 0) and youtube["z_views"] > 2.0:
        reasons.append("creadores")
    if twitch.get("z_concurrent", 0) and twitch["z_concurrent"] > 2.0:
        reasons.append("Twitch")
    if not reasons:
        return "Sin causa clara (posible variacion estacional)"
    return " + ".join(reasons)


def summarize_context(event_context: Dict[str, Any], social_context: Dict[str, Any]) -> str:
    steam = event_context.get("steam") or {}
    sale = steam.get("sale") or {}
    parts: List[str] = []
    if steam.get("dlc"):
        parts.append(f"DLC \"{steam['dlc']}\"")
    if sale.get("active"):
        discount = sale.get("discount_pct")
        if discount is not None:
            parts.append(f"rebaja ({discount}%)")
        else:
            parts.append("rebaja")
    youtube = (social_context.get("youtube") or {})
    twitch = (social_context.get("twitch") or {})
    if youtube.get("z_views", 0) > 2.0 or twitch.get("z_concurrent", 0) > 2.0:
        parts.append("impulso de creadores")
    if not parts:
        parts.append("variacion de demanda")
    return f"Pico asociado a {' + '.join(parts)}."


def generate_review_highlights(by_experience: Dict[str, Dict[str, Any]]) -> List[str]:
    highlights: List[str] = []
    new_seg = by_experience.get("new")
    if new_seg and new_seg.get("abandon_rate_30d") is not None:
        rate = new_seg["abandon_rate_30d"]
        pct = int(round(rate * 100))
        if rate >= 0.7:
            highlights.append(f"Abandono muy alto en nuevos ({pct}%): revisar onboarding/rendimiento.")
        elif rate >= 0.4:
            highlights.append(f"Abandono elevado en nuevos ({pct}%).")
    expert_seg = by_experience.get("expert")
    veteran_seg = by_experience.get("veteran")
    if expert_seg and veteran_seg:
        pos_exp = expert_seg.get("pos")
        pos_vet = veteran_seg.get("pos")
        abandon_exp = expert_seg.get("abandon_rate_30d")
        abandon_vet = veteran_seg.get("abandon_rate_30d")
        if pos_exp and pos_exp > 0.5 and pos_vet and pos_vet > 0.5:
            if (abandon_exp is None or abandon_exp < 0.3) and (abandon_vet is None or abandon_vet < 0.3):
                highlights.append("Expertos y veteranos mantienen percepcion positiva pese a criticas de balance.")
    if not highlights:
        dominant = max(by_experience.items(), key=lambda item: item[1].get("share", 0.0))
        key, data = dominant
        pct = data.get("share")
        if pct:
            labels = {
                "new": "jugadores nuevos",
                "intermediate": "segmento intermedio",
                "expert": "jugadores expertos",
                "veteran": "veteranos",
            }
            highlights.append(f"{labels.get(key, key)} concentran {int(round(pct * 100))}% de las resenas del pico.")
    return highlights


def describe_takeaways(date_label: Optional[str], zscore: Optional[float], why: str, granger: Dict[str, Any]) -> List[str]:
    takeaways: List[str] = []
    descriptor = "pico"
    if zscore is not None:
        if zscore >= 3.0:
            descriptor = "pico muy alto"
        elif zscore >= 2.0:
            descriptor = "pico alto"
    if date_label and zscore is not None:
        takeaways.append(f"{descriptor.capitalize()} en {date_label} (z={zscore:.1f}) explicado por {why}.")
    elif date_label:
        takeaways.append(f"{descriptor.capitalize()} en {date_label} explicado por {why}.")
    else:
        takeaways.append(f"{descriptor.capitalize()} explicado por {why}.")
    if granger.get("granger_xy_sig_fdr"):
        lag = granger.get("best_lag")
        best_ccf = granger.get("best_ccf")
        if best_ccf is not None:
            takeaways.append(f"Las resenas anteceden a los jugadores (lag={lag}, ccf={best_ccf:.2f}).")
        else:
            takeaways.append("Las resenas anteceden a los jugadores (Granger significativa).")
    else:
        takeaways.append("No hay evidencia robusta de causalidad resenas?jugadores.")
    return takeaways


def describe_pricing(delta_pct: Optional[float]) -> str:
    if delta_pct is None:
        return "En linea con el promedio del segmento"
    if delta_pct <= -0.1:
        return "Ligeramente por debajo del promedio de los competidores reales"
    if delta_pct >= 0.1:
        return "Por encima del promedio de los competidores reales"
    return "En linea con el promedio del segmento"


def describe_topic_trend(delta: Optional[float], sentiment: Optional[float]) -> str:
    if delta is None:
        return "Sin cambio relevante"
    if delta >= 0.05 and (sentiment is None or sentiment > -0.2):
        return "Gana relevancia reciente"
    if delta <= -0.05:
        return "Pierde relevancia"
    if sentiment is not None and sentiment < -0.3:
        return "Sentimiento negativo relevante"
    return "Estable"


def describe_topic_reason(topic: Dict[str, Any]) -> str:
    delta = topic.get("recent_share_delta") or 0.0
    sentiment = topic.get("avg_sentiment") or 0.0
    trend = describe_topic_trend(delta, sentiment)
    return f"{trend} (?={delta:+.2f}, sentimiento={sentiment:+.2f})."


def describe_key_signal(peak: Dict[str, Any]) -> str:
    date = peak.get("date_or_month") or peak.get("year_month")
    z = peak.get("zscore")
    why = peak.get("why")
    label = "Pico"
    if z is not None:
        if z >= 3.0:
            label = "Pico muy alto"
        elif z >= 2.0:
            label = "Pico alto"
    if date and why:
        return f"{label} {date} asociado a {why}."
    if why:
        return f"{label} asociado a {why}."
    if date:
        return f"{label} {date}."
    return "Pico relevante registrado."


def summarize_global_relevance(stats: Dict[str, Any]) -> List[str]:
    notes: List[str] = []
    negative_ratio = stats.get("negative_ratio")
    if negative_ratio is not None:
        if negative_ratio >= 0.5:
            notes.append("Alta proporcion de topicos negativos en competidores.")
        elif negative_ratio >= 0.2:
            notes.append("Presencia moderada de topicos negativos en competidores.")
    if stats.get("competitors_with_negative"):
        notes.append(f"{stats['competitors_with_negative']} competidores con senales negativas recientes.")
    if not notes:
        notes.append("Sin alertas relevantes en topicos globales.")
    return notes


def describe_topic_insight(topic: Dict[str, Any]) -> str:
    return describe_topic_reason(topic)


def _fmt_pct(value: Optional[float]) -> str:
    try:
        return f"{value*100:.0f}%" if value is not None else "—"
    except Exception:
        return "—"


def compose_client_narrative(report: Dict[str, Any], lang: str = "en") -> str:
    """Compose a concise narrative in the requested language (en/es).

    Uses cluster context, competitors, peak_analysis, pricing, and topic insights.
    Falls back gracefully if pieces are missing.
    """
    lang = (lang or "en").lower()
    md = report.get("metadata", {}) or {}
    name = md.get("name") or "The game"
    cluster = report.get("cluster", {}) or {}
    cc = report.get("cluster_context", {}) or {}
    rc_summary = report.get("real_competitors_summary", {}) or {}
    topic_insights = report.get("topic_insights", {}) or {}
    peaks = report.get("peak_analysis", []) or []
    rules = report.get("rules_analysis", {}) or {}
    pricing = rules.get("pricing_vs_real_competitors", {}) or {}

    def t(en_text: str, es_text: str) -> str:
        return en_text if lang == "en" else es_text

    # 1) Opening
    parts: List[str] = []
    csize = cc.get("cluster_size")
    sat = cc.get("saturation")
    opening = t(
        f"{name} sits in cluster {cluster.get('cluster_id')} (size {csize}) with {sat or 'balanced'} competition.",
        f"{name} se ubica en el clúster {cluster.get('cluster_id')} (tamaño {csize}) con competencia {sat or 'balanceada'}."
    )
    parts.append(opening)

    # 2) Pricing position
    pos = pricing.get("position")
    avg_price = pricing.get("avg_competitors_price")
    delta = pricing.get("delta_pct")
    if pos or avg_price is not None:
        parts.append(t(
            f"Pricing: {pos or 'in-line'} (vs avg ${avg_price:.2f} | Δ={delta:+.0%}).",
            f"Precio: {pos or 'en línea'} (vs promedio ${avg_price:.2f} | Δ={delta:+.0%})."
        ))

    # 3) Peaks & drivers (take the strongest)
    if peaks:
        p0 = peaks[0]
        ym = p0.get("year_month") or p0.get("date_or_month")
        z = p0.get("zscore")
        why = (p0.get("context") or {}).get("summary") or p0.get("why")
        parts.append(t(
            f"Key peak {ym} (z={z}). Driver: {why or 'demand variation' }.",
            f"Pico clave {ym} (z={z}). Motivo: {why or 'variación de demanda' }."
        ))

    # 4) Topics signals
    tr = topic_insights.get("trending_topics", [])
    risk = topic_insights.get("risk_topics", [])
    if tr:
        parts.append(t(
            f"Trending topics: {', '.join(str(x.get('topic_id')) for x in tr[:3])}.",
            f"Tópicos en alza: {', '.join(str(x.get('topic_id')) for x in tr[:3])}."
        ))
    if risk:
        parts.append(t(
            f"Risk topics: {', '.join(str(x.get('topic_id')) for x in risk[:2])}.",
            f"Tópicos de riesgo: {', '.join(str(x.get('topic_id')) for x in risk[:2])}."
        ))

    # 5) Competitors snapshot
    counts = (rc_summary.get("counts") or {})
    total = counts.get("total")
    intra = counts.get("intra_cluster")
    cross = counts.get("cross_cluster")
    if total is not None:
        parts.append(t(
            f"Competitive set: {total} titles (intra {intra}, cross {cross}).",
            f"Conjunto competitivo: {total} títulos (intra {intra}, cross {cross})."
        ))

    return " " .join([p for p in parts if p])
