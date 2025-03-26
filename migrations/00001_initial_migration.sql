-- +goose up

create extension if not exists ltree;
create extension if not exists btree_gist;

create type tonga_message as (
    id bigint,
    topic ltree,
    body jsonb,
    created_at timestamptz,
    deliver_at timestamptz
);

create table tonga_channels (
    queue_name text unique not null,
    topic ltree not null,
    unlogged boolean not null,
    delete_at timestamptz
);

create index tonga_channels_topic_gist_idx on tonga_channels using gist (topic);
create index tonga_channels_topic_delete_at_idx on tonga_channels (delete_at);

-- +goose statementbegin
create function _tonga_queue_table(queue_name text)
returns text as $$
begin
    if queue_name ~ '\$|;|--|''' then
        raise exception 'tonga: queue name contains invalid characters $, ;, --, or \''';
    end if;
    return 'tonga_q_' || lower(queue_name);
end;
$$ language plpgsql;

create function _tonga_validate_queue_name(queue_name text)
returns void as $$
begin
  if length(queue_name) >= 48 then
    raise exception 'tonga: queue name is too long, maximum length is 48 characters';
  end if;
end;
$$ language plpgsql;
-- +goose statementend

-- +goose statementbegin
create function tonga_create_channel(
    queue_name text,
    topic ltree,
    delete_at timestamptz = null,
    unlogged boolean = false
)
returns void as $$
declare
    _q_table text = _tonga_queue_table(queue_name);
    _q text;
begin
    perform _tonga_validate_queue_name(queue_name);

    if unlogged then
        execute format(
            $QUERY$
            create unlogged table if not exists %I (
                id bigint primary key generated always as identity,
                topic ltree not null,
                body jsonb not null,
                created_at timestamptz not null default now(),
                deliver_at timestamptz not null default now()
            )
            $QUERY$,
            _q_table
        );
    else
        execute format(
            $QUERY$
            create table if not exists %I (
                id bigint primary key generated always as identity,
                topic ltree not null,
                body jsonb not null,
                created_at timestamptz not null default now(),
                deliver_at timestamptz not null default now()
            )
            $QUERY$,
            _q_table
        );
    end if;    

    execute format('create index if not exists %I on %I (deliver_at);', _q_table || '_deliver_at_idx', _q_table);

    -- TODO should check topic and delete_at is same
    insert into tonga_channels (queue_name, topic, unlogged, delete_at)
    values (
        tonga_create_channel.queue_name,
        tonga_create_channel.topic,
        tonga_create_channel.unlogged,
        tonga_create_channel.delete_at
    )
    on conflict do nothing;
end;
$$ language plpgsql;
-- +goose statementend

-- +goose statementbegin
create function tonga_delete_channel(queue_name text)
returns boolean as $$
declare
    _q_table text = _tonga_queue_table(queue_name);
    _res boolean;
begin
    execute format('drop table if exists %I;', _q_table);

    delete from tonga_channels c
    where c.queue_name = tonga_delete_channel.queue_name
    returning true
    into _res;
    
    return coalesce(_res, false);
end
$$ language plpgsql;
-- +goose statementend

-- +goose statementbegin
create function tonga_send(topic ltree, body jsonb, deliver_at timestamptz = null)
returns void as $$
declare
    _deliver_at timestamptz = coalesce(tonga_send.deliver_at, now());
    _rec record;
    _q text;
begin
    for _rec in 
        select c.queue_name
        from tonga_channels c
        where c.topic @> tonga_send.topic and (c.delete_at is null or c.delete_at > now())
    loop
        _q = _tonga_queue_table(_rec.queue_name);
        execute format('insert into %I (topic, body, deliver_at) values ($1, $2, $3);', _q)
        using tonga_send.topic, tonga_send.body, _deliver_at;
    end loop;
end
$$ language plpgsql;
-- +goose statementend

-- +goose statementbegin
-- TODO return or error if deleted
create function tonga_read(queue_name text, quantity int = 1, hide_for int = 30)
returns setof tonga_message as $$
declare
    _q_table text = _tonga_queue_table(queue_name);
    _q text;
begin
    _q = format(
        $QUERY$
        with msgs as (
			select id
			from %I
			where deliver_at <= clock_timestamp()
			order by id asc
			limit $1
			for update skip locked
		)
		update %I m
		set deliver_at = clock_timestamp() + $2
		from msgs
		where m.id = msgs.id
		returning m.id, m.topic, m.body, m.created_at, m.deliver_at;
        $QUERY$,
        _q_table,
        _q_table
    );
    return query execute _q using tonga_read.quantity, make_interval(secs => tonga_read.hide_for);
end
$$ language plpgsql;
-- +goose statementend

-- +goose statementbegin
create function tonga_delete(queue_name text, id bigint)
returns boolean as $$
declare
    _q_table text = _tonga_queue_table(queue_name);
    _res boolean;
begin
    execute format('delete from %I where id = $1 returning true;', _q_table)
    using tonga_delete.id
    into _res;
    return coalesce(_res, false);
end;
$$ language plpgsql;
-- +goose statementend

-- +goose statementbegin
create function tonga_gc()
returns void as $$
declare
    _rec record;
begin
    for _rec in 
        delete from tonga_channels
		where delete_at is not null and delete_at <= now()
        returning queue_name
    loop
        execute format('drop table %I;', _tonga_queue_table(_rec.queue_name));
    end loop;
end
$$ language plpgsql;
-- +goose statementend

-- +goose down

drop function tonga_create_channel;
drop function tonga_delete_channel;
drop function tonga_send
drop function tonga_read;
drop function tonga_delete;
drop function tonga_gc;
drop function _tonga_queue_table;
drop function _tonga_validate_queue_name;
drop table tonga_channels cascade;
drop type tonga_message;
