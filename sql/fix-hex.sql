-- Convert script byte arrays to hex in extrinsics.data->'script'

BEGIN;

-- Convert integer array to hex string
CREATE OR REPLACE FUNCTION pg_temp.int_array_to_hex(arr jsonb) RETURNS text AS $$
DECLARE
    result text := '0x';
    elem jsonb;
BEGIN
    FOR elem IN SELECT * FROM jsonb_array_elements(arr) LOOP
        result := result || lpad(to_hex((elem #>> '{}')::int), 2, '0');
    END LOOP;
    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Preview
SELECT block_number, index, pallet, method,
       data->'script' as original_script,
       pg_temp.int_array_to_hex(data->'script') as converted_script
FROM extrinsics
WHERE jsonb_typeof(data->'script') = 'array'
  AND jsonb_array_length(data->'script') > 4
LIMIT 10;

-- Uncomment below for actual update
-- UPDATE extrinsics
-- SET data = jsonb_set(data, '{script}', to_jsonb(pg_temp.int_array_to_hex(data->'script')))
-- WHERE jsonb_typeof(data->'script') = 'array'
--   AND jsonb_array_length(data->'script') > 4;

COMMIT;
