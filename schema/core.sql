
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
create function yenotsys.matrix_key_table(text, text) returns table (
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
with primkeys as (
    select kcu.column_name
    from information_schema.table_constraints tc
    join information_schema.key_column_usage kcu on kcu.constraint_name=tc.constraint_name and
                        kcu.table_name=tc.table_name and kcu.table_schema=tc.table_schema
    where tc.constraint_type='PRIMARY KEY' and tc.table_schema=$1 and tc.table_name=$2
)
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
join information_schema.key_column_usage as kcu on tc.constraint_name=kcu.constraint_name and tc.table_schema=kcu.table_schema                                                                                   join information_schema.constraint_column_usage as ccu on ccu.constraint_name=tc.constraint_name and ccu.table_schema=tc.table_schema
join primkeys pk on pk.column_name=kcu.column_name
join information_schema.columns fcol on fcol.table_name=ccu.table_name and fcol.table_schema=ccu.table_schema and fcol.column_name=ccu.column_name
where tc.constraint_type='FOREIGN KEY' and tc.table_schema=$1 and tc.table_name=$2
$$
language sql
security definer
-- Limit to trusted schema(s)
set search_path=pg_catalog, pg_temp;
