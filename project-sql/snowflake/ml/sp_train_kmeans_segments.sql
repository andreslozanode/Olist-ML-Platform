-- =============================================================================
-- Modelo de ML EN Snowflake con Snowpark ML (snowflake.ml.modeling).
-- Entrena K-Means sobre los features (grano persona) y materializa la tabla
-- ANALYTICS.ML.CUSTOMER_SEGMENTS_ML que consume el mart dbt fct_customer_segments.
--
-- Deploy:  snow sql -f snowflake/ml/sp_train_kmeans_segments.sql
-- Run:     CALL ANALYTICS.ML.SP_TRAIN_KMEANS_SEGMENTS('ANALYTICS.MARTS.INT_CUSTOMER_FEATURES', 5);
-- =============================================================================
CREATE OR REPLACE PROCEDURE ANALYTICS.ML.SP_TRAIN_KMEANS_SEGMENTS(
    FEATURES_RELATION VARCHAR,
    N_CLUSTERS INTEGER DEFAULT 5
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'snowflake-ml-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark import Session
from snowflake.ml.modeling.pipeline import Pipeline
from snowflake.ml.modeling.preprocessing import StandardScaler
from snowflake.ml.modeling.cluster import KMeans

FEATURES = [
    "NUM_ORDERS", "IS_REPEAT_CUSTOMER", "NUM_DISTINCT_STATES",
    "NUM_DISTINCT_CITIES", "ZIP_MACRO_REGION", "STATE_MARKET_SHARE", "REGION_IDX",
]
SCALED = [f"{c}_SCALED" for c in FEATURES]


def run(session: Session, features_relation: str, n_clusters: int) -> str:
    df = session.table(features_relation)

    pipeline = Pipeline(steps=[
        ("scaler", StandardScaler(input_cols=FEATURES, output_cols=SCALED)),
        ("kmeans", KMeans(
            input_cols=SCALED,
            output_cols=["SEGMENT_ID"],
            n_clusters=int(n_clusters),
            max_iter=50,
            random_state=42,
        )),
    ])

    model = pipeline.fit(df)
    scored = model.predict(df)

    # Etiquetado de negocio por perfil de cluster (estable para BI).
    profile = scored.group_by("SEGMENT_ID").agg(
        {"NUM_ORDERS": "avg", "NUM_DISTINCT_STATES": "avg", "STATE_MARKET_SHARE": "avg"}
    ).to_pandas()

    def label(row):
        if row["AVG(NUM_ORDERS)"] > 1.3:
            return "Recurrente"
        if row["AVG(NUM_DISTINCT_STATES)"] > 1.05:
            return "Itinerante"
        if row["AVG(STATE_MARKET_SHARE)"] > 0.30:
            return "Metropolitano"
        return "Estándar regional"

    profile["SEGMENT_LABEL"] = profile.apply(label, axis=1)
    label_df = session.create_dataframe(
        profile[["SEGMENT_ID", "SEGMENT_LABEL"]].values.tolist(),
        schema=["SEGMENT_ID", "SEGMENT_LABEL"],
    )

    out = (
        scored.select("CUSTOMER_UNIQUE_ID", "SEGMENT_ID")
        .join(label_df, on="SEGMENT_ID", how="left")
        .select("CUSTOMER_UNIQUE_ID", "SEGMENT_ID", "SEGMENT_LABEL")
    )
    out.write.mode("overwrite").save_as_table("ANALYTICS.ML.CUSTOMER_SEGMENTS_ML")

    n = out.count()
    return f"OK: {n} clientes segmentados en {n_clusters} clusters."
$$;
