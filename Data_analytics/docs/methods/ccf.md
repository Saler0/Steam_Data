# CCF y Causalidad de Granger

Analiza relaciones temporales entre variables (players, reseñas positivas/negativas) por juego, con control de estacionariedad y corrección por comparaciones múltiples.

- Script: `src/pipelines/ccf_analysis/analyze_competitors_ccf.py`
- Config: `configs/ccf_analysis.yaml`
- Entradas: `data/processed/clusters.parquet`, preagregados Parquet (si existen) `data/warehouse/reviews_monthly.parquet` y `data/warehouse/players_monthly.parquet`, o bien Mongo (reseñas) y CSVs de players.
- Salidas: `outputs/ccf_analysis/summary.parquet` y `summary.csv`

Flujo
1) Lectura de preagregados mensuales (preferido) o agregación mensual al vuelo de players y reseñas.
2) Transformaciones para estacionariedad (ADF): `dlog`, `diff`, `diff2`, `sqrt`, `sqrt_diff`, `log1p_diff`, `seasonal_diff`.
3) Preblanqueo AR(1) del predictor y filtrado del target (si procede).
4) CCF y selección de `best_lag` y `best_ccf`.
5) Tests de Granger en ambos sentidos (min p-value por lag) y corrección FDR (Benjamini–Hochberg).

Columnas de salida destacadas
- `best_lag`, `best_ccf`
- `granger_xy_pmin`, `granger_xy_sig` (antes de corrección)
- `granger_yx_pmin`, `granger_yx_sig` (antes de corrección)
- `granger_xy_p_fdr`, `granger_xy_sig_fdr` (tras FDR)
- `granger_yx_p_fdr`, `granger_yx_sig_fdr` (tras FDR)

Parámetros relevantes
- `stationarity.transforms`, `stationarity.adf_alpha`, `stationarity.seasonal.period`
- `ccf_lags`
- `granger.maxlag`, `granger.alpha`
- `parallel_mode`: `multiprocessing` | `ray` | `sequential`
- `cluster_filter`: lista opcional de cluster_id para limitar el análisis.

Ejecución
- Makefile: `make ccf` o `make ccf-check`
- DVC: etapa `ccf`
 - MLflow: registra métricas agregadas (juegos analizados, % significativos, FDR) y artefactos (`summary.parquet`/`.csv`).

Evidencias gráficas sugeridas
- Mapa de calor de `best_ccf` por juego/par.
- Histograma de `best_lag`.
- Barras del % de significativos antes y después de FDR.
