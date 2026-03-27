WITH converted AS (
  SELECT 
    ctid,
    storage_keys as old_keys,
    (
      SELECT jsonb_agg(
        CASE 
          -- Handle hex strings at first level: only convert single-byte values
          WHEN jsonb_typeof(elem) = 'string' AND elem #>> '{}' LIKE '0x%'
          THEN 
            CASE 
              -- Only convert single-byte hex (0x00 through 0xFF = length 4)
              WHEN length(elem #>> '{}') = 4
              THEN jsonb_build_array(
                to_jsonb(get_byte(decode(substring(elem #>> '{}' from 3), 'hex'), 0)::text)
              )
              -- Keep longer hex strings unchanged
              ELSE elem
            END
          
          -- Handle nested arrays with hex strings
          WHEN jsonb_typeof(elem) = 'array'
          THEN (
            SELECT jsonb_agg(
              CASE
                WHEN jsonb_typeof(nested) = 'string' AND nested #>> '{}' LIKE '0x%'
                THEN
                  CASE
                    -- Only convert single-byte hex values
                    WHEN length(nested #>> '{}') = 4
                    THEN to_jsonb(get_byte(decode(substring(nested #>> '{}' from 3), 'hex'), 0)::text)
                    -- Keep longer hex strings unchanged
                    ELSE nested
                  END
                ELSE nested
              END
            )
            FROM jsonb_array_elements(elem) nested
          )
          
          -- Leave other elements unchanged
          ELSE elem
        END
      )
      FROM jsonb_array_elements(storage_keys) elem
    ) as new_keys
  FROM storage_snapshots
  WHERE storage_keys::text LIKE '%0x%'
)
UPDATE storage_snapshots s
SET storage_keys = c.new_keys
FROM converted c
WHERE s.ctid = c.ctid
  AND c.old_keys != c.new_keys;
