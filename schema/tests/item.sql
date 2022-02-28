create table item (
	id serial primary key,
	name varchar(20) not null,
	price numeric(12, 2) not null,
	sale_class char(1)
);

create unique index item_name_idx on item(name);

create table tag (
	id serial primary key,
	name varchar(20) not null unique
);

create table tagitem (
	tag_id integer references tag(id) not null,
	item_id integer references item(id) not null,
	primary key (tag_id, item_id)
);

