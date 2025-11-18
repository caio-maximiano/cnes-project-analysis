# src/main/data_domains/cnes/models/cnes_linear_regression.py
from main.core.layers.models import Model
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LinearRegression
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
import pandas as pd

from core.layers.models import Model

class CnesLinearRegression(Model):
    def __init__(self, artifact_name: str = "cnes_linear_regression"):
        super().__init__(artifact_name)

    def pipeline(self) -> Pipeline:
        df_input = self.read_gold_parquet("cnes_estabelecimentos_metrics", year_month=None)

        df_filtered = df_input.query("POPULACAO_MENSAL >= 50000 and NO_MUNICIPIO not in ['SAO PAULO','JERIQUARA'] and PROFISSIONAIS_POR_1000 <= 10 ") # filtrar outliers

        df_featured = df_filtered.copy()

        # Data feature
        df_featured['date'] = pd.to_datetime(df_featured['YYYY'].astype(str) + '-' + df_featured['MM'].astype(str) + '-01')

        # Índice temporal (para regressão linear)
        df_featured["time_index"] = df_featured.groupby(["CO_MUNICIPIO_SEM_DIGITO","DS_ATIVIDADE_PROFISSIONAL"]).cumcount() + 1

        # Lags e rolling
        df_featured["lag1"] = df_featured.groupby(["CO_MUNICIPIO_SEM_DIGITO","DS_ATIVIDADE_PROFISSIONAL"])["PROFISSIONAIS_POR_1000"].shift(1)
        df_featured["rolling3"] = df_featured.groupby(["CO_MUNICIPIO_SEM_DIGITO","DS_ATIVIDADE_PROFISSIONAL"])["PROFISSIONAIS_POR_1000"].transform(lambda x: x.rolling(3).mean())

        # Target e features
        TARGET_COL = 'PROFISSIONAIS_POR_1000'   # alvo contínuo
        CAT_COLS   = ['NO_MUNICIPIO', 'DS_ATIVIDADE_PROFISSIONAL']
        NUM_COLS   = ['POPULACAO_MENSAL', 'GROWTH_PCT', 'lag1', 'rolling3', 'time_index']

        FEATURES = CAT_COLS + NUM_COLS

        # Sanidade: remover linhas sem as features ou sem alvo
        df_model = df_featured.dropna(subset=FEATURES + [TARGET_COL]).copy()


        # Split Temporal (80% treino, 20% teste)
        # Data de corte (80% das datas distintas)
        unique_dates = sorted(df_model['date'].unique())
        cut_idx = int(len(unique_dates) * 0.8)
        cut_date = unique_dates[cut_idx]

        X = df_model[FEATURES].copy()
        y = df_model[TARGET_COL].copy()

        X_train = X[df_model['date'] <  cut_date]
        y_train = y[df_model['date'] <  cut_date]
        X_test  = X[df_model['date'] >= cut_date]
        y_test  = y[df_model['date'] >= cut_date]

        pre = ColumnTransformer([
            ('cat', OneHotEncoder(handle_unknown='ignore'), CAT_COLS),
            ('num', 'passthrough', NUM_COLS)
        ])

        pipe = Pipeline([
            ('pre', pre),
            ('lr', LinearRegression())
        ])

        pipe.fit(X_train, y_train)

        self.execute_quality_check(
            pipe,
            X_test,
            y_test,
            thresholds={"MAE": 0.008, "RMSE": 0.01, "R2": 0.25}
        )


        return pipe
