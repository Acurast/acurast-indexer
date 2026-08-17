-- ============================================
-- accounts.vesting_epoch
-- Latest-wins guard for the `remaining_vesting` / `remaining_token_claim`
-- columns, which are now written by `epoch_totals::process_epoch_totals`
-- (full-map iteration at the epoch boundary) rather than the per-account
-- materializer.
--
-- Epochs are processed out of order across many workers, so an older epoch's
-- write must not clobber a newer one's. The balance columns guard on
-- `block_number` (owned by the System.Account materializer); the vesting /
-- token-claim columns get their own epoch-numbered guard so the two writers
-- don't interfere. A write applies only when its epoch >= the stored
-- `vesting_epoch`.
-- ============================================

ALTER TABLE public.accounts
    ADD COLUMN vesting_epoch BIGINT NOT NULL DEFAULT 0;
