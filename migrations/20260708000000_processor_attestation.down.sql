DROP INDEX IF EXISTS accounts_processor_type_idx;
ALTER TABLE public.accounts
    DROP COLUMN IF EXISTS attestation_block_number,
    DROP COLUMN IF EXISTS device_type,
    DROP COLUMN IF EXISTS processor_type;
