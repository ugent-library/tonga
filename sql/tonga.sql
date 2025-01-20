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
    expires_at timestamptz
);

create index tonga_channels_topic_gist_idx on tonga_channels using gist (topic);
create index tonga_channels_topic_expires_at_idx on tonga_channels (expires_at);

create function _tonga_table(queue_name text, prefix text)
returns text as $$
begin
    if queue_name ~ '\$|;|--|''' then
        raise exception 'tonga: queue name contains invalid characters $, ;, --, or \''';
    end if;
    return lower(prefix || queue_name);
end;
$$ language plpgsql;

create function _tonga_queue_table(queue_name text)
returns text as $$
begin
    return _tonga_table(queue_name, 'tonga_q_');
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

create function tonga_add_channel(queue_name text, topic ltree, expires_after interval = null)
returns void as $$
declare
    _q_table text = _tonga_table(queue_name, 'tonga_q_');
begin
    perform _tonga_validate_queue_name(queue_name);

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

    execute format('create index if not exists %I on %I (deliver_at);', _q_table || '_deliver_at_idx', _q_table);

    -- TODO should check topic is same
    insert into tonga_channels (queue_name, topic, expires_at)
    values (
        tonga_add_channel.queue_name,
        tonga_add_channel.topic,
        (case when expires_after is not null then clock_timestamp() + tonga_add_channel.expires_after
         else null
         end)
    )
    on conflict do nothing;
end;
$$ language plpgsql;

create function tonga_remove_channel(queue_name text)
returns void as $$
declare
    _q_table text = _tonga_queue_table(queue_name);
begin
    execute format('drop table if exists %I;', _q_table);

    delete from tonga_channels where queue_name = tonga_remove_channel.queue_name;
end
$$ language plpgsql;

create function tonga_send(topic ltree, body jsonb, deliver_at timestamptz = now())
returns void as $$
declare
    _rec record;
    _q text;
begin
    for _rec in 
        select c.queue_name
        from tonga_channels c
        where c.topic @> tonga_send.topic and (c.expires_at is null or c.expires_at > now())
    loop
        _q = _tonga_queue_table(_rec.queue_name);
        execute format('insert into %I (topic, body, deliver_at) values ($1, $2, $3);', _q)
        using tonga_send.topic, tonga_send.body, tonga_send.deliver_at;
    end loop;
end
$$ language plpgsql;

-- TODO return or error if expired
create function tonga_read(queue_name text, hide_for interval, quantity int = 1)
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
			limit $2
			for update skip locked
		)
		update %I m
		set deliver_at = clock_timestamp() + $1
		from msgs
		where m.id = msgs.id
		returning m.id, m.topic, m.body, m.created_at, m.deliver_at;
        $QUERY$,
        _q_table,
        _q_table
    );
    return query execute _q using tonga_read.hide_for, tonga_read.quantity;
end
$$ language plpgsql;

create function tonga_delete(queue_name text, id bigint)
returns boolean as $$
declare
    _q_table text = _tonga_queue_table(queue_name);
    _res boolean;
begin
    execute format('delete from %I where id = $1 returning true;', _q_table)
    using tonga_delete.id
    into _res;
    return _res;
end;
$$ language plpgsql;

create function tonga_gc()
returns void as $$
declare
    _rec record;
begin
    for _rec in 
        delete from tonga_channels
		where expires_at is not null and expires_at <= now()
        returning queue_name
    loop
        execute format('drop table %I;', _tonga_queue_table(_rec.queue_name));
    end loop;
end
$$ language plpgsql;