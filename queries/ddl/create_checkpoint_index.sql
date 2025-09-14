CREATE INDEX IF NOT EXISTS checkpoints_metadata_gin_idx ON checkpoints USING GIN (metadata);
