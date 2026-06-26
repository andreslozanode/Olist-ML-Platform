-- REPETIBLE (R__): se re-aplica cuando cambia el hash del archivo.
-- Cluster keys + automatic clustering para optimizar pruning en lecturas de BI.
ALTER TABLE IF EXISTS ANALYTICS.MARTS.MART_BI_CUSTOMER_SEGMENTS
    CLUSTER BY (PRIMARY_REGION, SEGMENT_ID);

ALTER TABLE IF EXISTS ANALYTICS.MARTS.MART_BI_CUSTOMER_SEGMENTS
    RESUME RECLUSTER;

-- Diagnóstico de calidad de clustering (revisar antes de habilitar en tablas grandes):
--   SELECT SYSTEM$CLUSTERING_INFORMATION('ANALYTICS.MARTS.MART_BI_CUSTOMER_SEGMENTS',
--          '(PRIMARY_REGION, SEGMENT_ID)');
