-- Add up migration script here
CREATE INDEX "extrinsics_block_number_idx" ON public.extrinsics USING btree (block_number DESC, index DESC);
