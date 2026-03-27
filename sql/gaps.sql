WITH ordered_blocks AS (
        SELECT block_number, "hash", LEAD(block_number) OVER (ORDER BY block_number) AS next_block_number,
                LEAD("hash") OVER (ORDER BY block_number) AS next_low_hash
        FROM blocks
        ORDER BY block_number desc
        )
        SELECT 
            block_number + 1 AS start,
            next_low_hash
        FROM ordered_blocks
        WHERE next_block_number IS NOT NULL AND next_block_number - block_number > 0
        LIMIT 1000;

-- WITH ordered_blocks AS (
--         SELECT block_number, "hash", LEAD(block_number) OVER (ORDER BY block_number) AS next_block_number,
--                 LEAD("hash") OVER (ORDER BY block_number) AS next_low_hash
--         FROM blocks
--         ORDER BY block_number desc
--         )
--         SELECT 
--             count(*)
--         FROM ordered_blocks
--         WHERE next_block_number IS NOT NULL AND next_block_number - block_number > 0
--         LIMIT 1000;