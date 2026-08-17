-- Partial composite index for `getEvents` filtered by source="system".
-- Without it, source=system queries combined with pallet/variant/block_range
-- heap-filter event_phase after scanning the full pallet+variant range, which
-- times out on broad ranges of high-frequency events (e.g. Balances.Issued).
-- Non-extrinsic events are a small fraction of the table, so the partial
-- index stays small while covering every source=system query shape.
CREATE INDEX "events_pallet_variant_system_idx"
ON public.events USING btree (pallet, variant, block_number, index)
WHERE event_phase <> 'ApplyExtrinsic'::event_phase_type;
