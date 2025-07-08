import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import MultiLabelBinarizer

class GameFeatureExtractor:
    def __init__(self, model_name='all-MiniLM-L6-v2', mode='clustering',
                 genres_vocab=None, categories_vocab=None):
        """
        model_name: modelo de Sentence-BERT a utilizar.
        mode: tipo de salida ('clustering', 'dafo', 'ciclo_vida', etc.).
        vocab opcional para reproducibilidad en inferencia.
        """
        self.model = SentenceTransformer(model_name)
        self.mode = mode
        self.mlb_genres = MultiLabelBinarizer(classes=genres_vocab) if genres_vocab else MultiLabelBinarizer()
        self.mlb_categories = MultiLabelBinarizer(classes=categories_vocab) if categories_vocab else MultiLabelBinarizer()

    def combine_text_fields(self, df: pd.DataFrame) -> pd.Series:
        return (
            df["detailed_description"].fillna("") + " " +
            df["about_the_game"].fillna("") + " " +
            df["short_description"].fillna("")
        ).str.strip()

    def generate_text_embeddings(self, texts: list[str]) -> np.ndarray:
        return self.model.encode(texts, show_progress_bar=True)

    def encode_multilabel_fields(self, df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        genre_vec = self.mlb_genres.fit_transform(df["genres"])
        cat_vec = self.mlb_categories.fit_transform(df["categories"])
        return genre_vec, cat_vec

    def normalize_required_age(self, df: pd.DataFrame) -> np.ndarray:
        edad = df["required_age"].fillna(0).astype(float).clip(0, 100).values.reshape(-1, 1)
        return edad / 100.0

    def extract_structured_fields(self, df: pd.DataFrame) -> np.ndarray:
        """
        Aquí se procesarán otros campos estructurados como:
        - is_free
        - metacritic_score
        - recommendations_total
        - price_overview
        - platforms
        """
        if self.mode == "clustering":
            return np.array([]).reshape(len(df), 0)  # sin campos estructurados por ahora
        elif self.mode == "dafo":
            pass  # añadir más adelante
        elif self.mode == "ciclo_vida":
            pass
        return np.array([]).reshape(len(df), 0)

    def transform(self, df: pd.DataFrame) -> tuple[np.ndarray, pd.DataFrame]:
        """
        Procesa el dataframe y devuelve:
        - embeddings combinados para clustering/KNN u otros
        - dataframe enriquecido con columnas limpias (opcional)
        """
        df = df.copy()
        df["full_description"] = self.combine_text_fields(df)

        # Modo clustering (activo)
        if self.mode == "clustering":
            emb_text = self.generate_text_embeddings(df["full_description"].tolist())
            genre_vec, cat_vec = self.encode_multilabel_fields(df)
            age_vec = self.normalize_required_age(df)
            return np.hstack([emb_text, genre_vec, cat_vec, age_vec]), df

        # Placeholder para otros modos
        elif self.mode == "dafo":
            # TODO: Incluir is_free, price_overview, platforms, etc.
            pass

        elif self.mode == "ciclo_vida":
            # TODO: Incluir release_date, recommendations_total, etc.
            pass

        # Si el modo no está definido o está vacío
        return np.array([]), df

    def transform_new_game(self, game_dict: dict) -> np.ndarray:
        """
        Transforma un único juego (input cliente) en su embedding combinado.
        """
        df = pd.DataFrame([game_dict])
        df["full_description"] = self.combine_text_fields(df)

        emb_text = self.generate_text_embeddings(df["full_description"].tolist())
        genre_vec = self.mlb_genres.transform(df["genres"])
        cat_vec = self.mlb_categories.transform(df["categories"])
        age_vec = self.normalize_required_age(df)

        return np.hstack([emb_text, genre_vec, cat_vec, age_vec])
