BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'event_comment'
          AND column_name = 'sequence'
          AND data_type = 'integer'
    ) THEN
        ALTER TABLE event_comment
            ALTER COLUMN sequence TYPE BIGINT
            USING sequence::BIGINT;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'event_comment'
          AND column_name = 'event_id'
          AND data_type <> 'bigint'
    ) THEN
        ALTER TABLE event_comment
            ALTER COLUMN event_id TYPE BIGINT
            USING event_id::BIGINT;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'event_comment'
          AND column_name = 'comment_id'
          AND data_type <> 'bigint'
    ) THEN
        ALTER TABLE event_comment
            ALTER COLUMN comment_id TYPE BIGINT
            USING comment_id::BIGINT;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'event_comment'
          AND column_name = 'player_id'
          AND data_type <> 'bigint'
    ) THEN
        ALTER TABLE event_comment
            ALTER COLUMN player_id TYPE BIGINT
            USING player_id::BIGINT;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'event_comment_feed'
          AND column_name = 'event_id'
          AND data_type <> 'bigint'
    ) THEN
        ALTER TABLE event_comment_feed
            ALTER COLUMN event_id TYPE BIGINT
            USING event_id::BIGINT;
    END IF;
END
$$;

COMMIT;
