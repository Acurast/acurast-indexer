-- Inverse of this migration's up (which only dropped the superseded per-date
-- churn table): recreate processor_churn. The processor_active_bucket table is
-- created and dropped by 20260727000000_create_processor_active_bucket, so it is
-- not touched here.
CREATE TABLE IF NOT EXISTS public.processor_churn (
    as_of_date         DATE PRIMARY KEY,
    as_of              TIMESTAMP WITH TIME ZONE NOT NULL,
    onboarded_total    BIGINT NOT NULL,
    onboarded_distinct BIGINT NOT NULL,
    active_3m          BIGINT NOT NULL,
    active_12m         BIGINT NOT NULL,
    computed_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
