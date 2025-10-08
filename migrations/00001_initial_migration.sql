-- SQL code is mostly taken or adapted from PGMQ https://github.com/pgmq/pgmq/blob/main/pgmq-extension/sql/pgmq.sql 

-- +goose up

CREATE TYPE tonga_message AS (
    id bigint,
    topic text,
    payload jsonb,
    created_at timestamptz,
    deliver_at timestamptz
);

CREATE TABLE tonga_queues (
    name text UNIQUE NOT NULL,
    topics text[] NOT NULL,
    unlogged boolean NOT null,
    delete_at timestamptz
);

CREATE INDEX tonga_queues_topics_idx ON tonga_queues USING gin (topics);
CREATE INDEX tonga_queues_delete_at_idx ON tonga_queues (delete_at);

-- +goose statementbegin
CREATE FUNCTION _tonga_acquire_queue_lock(name text) 
RETURNS void AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext(_tonga_queue_table(name)));
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION _tonga_queue_table(name text)
RETURNS text AS $$
BEGIN
    IF name !~ '^[a-z0-9_]+$' THEN
        RAISE EXCEPTION 'tonga: queue name can only contain characters: a-z, 0-9 or _';
    END IF;
    IF length(name) >= 55 THEN
        raise exception 'tonga: queue name is too long, maximum length is 55';
    END IF;
    RETURN 'tonga_q_' || lower(name);
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION tonga_create_queue(
    name text,
    topics text[],
    delete_at timestamptz = null,
    unlogged boolean = false
)
RETURNS void AS $$
DECLARE
    _q_table text = _tonga_queue_table(tonga_create_queue.name);
BEGIN
    PERFORM _tonga_acquire_queue_lock(tonga_create_queue.name);

    IF unlogged THEN
        EXECUTE format(
            $QUERY$
            CREATE UNLOGGED TABLE IF NOT EXISTS %I (
                id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                topic text NOT NULL,
                payload jsonb NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now(),
                deliver_at timestamptz NOT NULL DEFAULT now()
            )
            $QUERY$,
            _q_table
        );
    ELSE
        EXECUTE format(
            $QUERY$
            CREATE TABLE IF NOT EXISTS %I (
                id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                topic text NOT NULL,
                payload jsonb NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now(),
                deliver_at timestamptz NOT NULL DEFAULT now()
            )
            $QUERY$,
            _q_table
        );
    END IF;    

    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (deliver_at);', _q_table || '_deliver_at_idx', _q_table);

    -- TODO should check topic and delete_at is same
    INSERT INTO tonga_queues (name, topics, unlogged, delete_at)
    VALUES (
        tonga_create_queue.name,
        tonga_create_queue.topics,
        tonga_create_queue.unlogged,
        tonga_create_queue.delete_at
    )
    ON CONFLICT DO NOTHING;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION tonga_delete_queue(name text)
RETURNS boolean AS $$
DECLARE
    _q_table text = _tonga_queue_table(tonga_delete_queue.name);
    _res boolean;
BEGIN
    PERFORM _tonga_acquire_queue_lock(tonga_delete_queue.name);

    EXECUTE format('drop table if exists %I;', _q_table);

    DELETE FROM tonga_queues q
    WHERE q.name = tonga_delete_queue.name
    RETURNING true
    INTO _res;
    
    RETURN coalesce(_res, false);
end
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION tonga_send(topic text, payload jsonb, deliver_at timestamptz = null)
RETURNS void AS $$
DECLARE
    _deliver_at timestamptz = coalesce(tonga_send.deliver_at, now());
    _rec record;
    _q_table text;
BEGIN
    FOR _rec IN 
        SELECT q.name
        FROM tonga_queues q
        WHERE tonga_send.topic = any(q.topics) AND (q.delete_at IS NULL OR q.delete_at > now())
    LOOP
        _q_table = _tonga_queue_table(_rec.name);
        EXECUTE format('insert into %I (topic, payload, deliver_at) values ($1, $2, $3);', _q_table)
        USING tonga_send.topic, tonga_send.payload, _deliver_at;
    END LOOP;
END
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
-- TODO return or error if deleted
CREATE FUNCTION tonga_read(queue text, quantity int = 1, hide_for int = 10)
RETURNS SETOF tonga_message as $$
DECLARE
    _q_table text = _tonga_queue_table(queue);
    _q text;
BEGIN
    _q = format(
        $QUERY$
        WITH msgs AS (
			SELECT id
			FROM %I
			WHERE deliver_at <= clock_timestamp()
			ORDER BY id ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE %I m
		SET deliver_at = clock_timestamp() + $2
		FROM msgs
		WHERE m.id = msgs.id
		RETURNING m.id, m.topic, m.payload, m.created_at, m.deliver_at;
        $QUERY$,
        _q_table, _q_table
    );
    RETURN QUERY EXECUTE _q USING tonga_read.quantity, make_interval(secs => tonga_read.hide_for);
end
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION tonga_delete(queue text, id bigint)
RETURNS boolean AS $$
DECLARE
    _q_table text = _tonga_queue_table(queue);
    _res boolean;
BEGIN
    EXECUTE format('delete from %I where id = $1 returning true;', _q_table)
    USING tonga_delete.id
    INTO _res;
    return coalesce(_res, false);
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION tonga_gc()
RETURNS void AS $$
DECLARE
    _rec record;
BEGIN
    FOR _rec IN 
        DELETE FROM tonga_queues
		WHERE delete_at IS NOT NULL AND delete_at <= now()
        RETURNING name
    LOOP
        PERFORM _tonga_acquire_queue_lock(_rec.name);
        EXECUTE format('drop table %I;', _tonga_queue_table(_rec.name));
    END LOOP;
END
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose down

DROP FUNCTION tonga_create_queue;
DROP FUNCTION tonga_delete_queue;
DROP FUNCTION tonga_send;
DROP FUNCTION tonga_read;
DROP FUNCTION tonga_delete;
DROP FUNCTION tonga_gc;
DROP FUNCTION _tonga_queue_table;
DROP TABLE tonga_queues CASCADE;
DROP TYPE tonga_message;
