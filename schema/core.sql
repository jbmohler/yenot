
create schema yenotsys;

create table yenotsys.eventlog (
	id serial primary key,
	logtype varchar(30) not null,
	logtime timestamptz default current_timestamp not null,
	descr text,
	logdata json
);

-- The table key_column_usage only shows columns that the current role has
-- ownership level access.  Implement this function as a "security definer" to
-- enable admin level access for this information.
create function yenotsys.foreign_key_detail(text, text) returns table (
    column_name text,
    foreign_table_schema text,
    foreign_table_name text,
    foreign_column_name text,
    foreign_data_type text,
    foreign_character_maximum_length int,
    foreign_numeric_precision int,
    foreign_numeric_precision_radix int,
    foreign_numeric_scale int
)
as $$
select
    kcu.column_name,
    ccu.table_schema as foreign_table_schema,
    ccu.table_name as foreign_table_name,
    ccu.column_name as foreign_column_name,
    fcol.data_type as foreign_data_type,
    fcol.character_maximum_length as foreign_character_maximum_length,
    fcol.numeric_precision as foreign_numeric_precision,
    fcol.numeric_precision_radix as foreign_numeric_precision_radix,
    fcol.numeric_scale as foreign_numeric_scale
from information_schema.table_constraints as tc
join information_schema.key_column_usage kcu on
                kcu.constraint_name=tc.constraint_name and
                kcu.table_name=tc.table_name and kcu.table_schema=tc.table_schema
join information_schema.constraint_column_usage as ccu on ccu.constraint_name=tc.constraint_name and ccu.table_schema=tc.table_schema
join information_schema.columns fcol on fcol.table_name=ccu.table_name and fcol.table_schema=ccu.table_schema and fcol.column_name=ccu.column_name
where tc.constraint_type='FOREIGN KEY' and tc.table_schema=$1 and tc.table_name=$2
$$
language sql
security definer
-- Limit to trusted schema(s)
set search_path=pg_catalog, pg_temp;

-- TODO -- possibly create a view with foreign key details

-- The table key_column_usage only shows columns that the current role has
-- ownership level access.  Implement this function as a "security definer" to
-- enable admin level access for this information.
create function yenotsys.matrix_key_table(text, text) returns table (
    constraint_name text,
    foreign_table_schema text,
    foreign_table_name text,
    ordered_keys text[],
    unordered_referents text[]
)
as $$
with primkeys as (
    select array_agg(kcu.column_name order by kcu.ordinal_position) as pk_columns
    from information_schema.table_constraints tc
    join information_schema.key_column_usage kcu on kcu.constraint_name=tc.constraint_name and
                        kcu.table_schema=tc.table_schema and kcu.table_name=tc.table_name
    where tc.constraint_type='PRIMARY KEY' and tc.table_schema=$1 and tc.table_name=$2
), foreign_maps as (
        select
                tc.constraint_name,
                ftable.foreign_table_schema,
                ftable.foreign_table_name,
                kcu_agg.ordered_keys,
                ftable.unordered_referents
        from information_schema.table_constraints as tc
        join lateral (
                select
                        array_agg(kcu.column_name order by kcu.ordinal_position) as ordered_keys
                from information_schema.key_column_usage kcu
                where kcu.constraint_name=tc.constraint_name and kcu.table_schema=tc.table_schema and kcu.table_name=tc.table_name
                ) kcu_agg on true
        join lateral (
                -- column ordering in composite keys is not available in constraint_column_usage so we do not pursue it
                select
                        table_schema as foreign_table_schema, table_name as foreign_table_name,
                        array_agg(ccu.column_name) as unordered_referents
                from information_schema.constraint_column_usage ccu
                where ccu.constraint_schema=tc.table_schema and ccu.constraint_name=tc.constraint_name
                group by foreign_table_schema, foreign_table_name
                ) ftable on true
        where tc.constraint_type='FOREIGN KEY' and tc.table_schema=$1 and tc.table_name=$2
)
select fmaps.*
from foreign_maps fmaps
join primkeys on primkeys.pk_columns @> fmaps.ordered_keys
$$
language sql
security definer
-- Limit to trusted schema(s)
set search_path=pg_catalog, pg_temp;
