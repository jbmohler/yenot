
create schema yenotsys;

create table yenotsys.eventlog (
	id serial primary key,
	logtype varchar(30) not null,
	logtime timestamptz default current_timestamp not null,
	descr text,
	logdata json
);
