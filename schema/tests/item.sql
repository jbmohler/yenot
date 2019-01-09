create table item (
	id serial primary key,
	name varchar(20),
	price numeric(12, 2) not null,
	sale_class char(1)
);

create unique index item_name_idx on item(name);
