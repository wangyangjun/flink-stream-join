CREATE TABLE if not exists Advertisement(
	id varchar(255) NOT NULL,
	shown_time BIGINT NOT NULL,
	PRIMARY KEY (id)
);

CREATE TABLE if not exists click(
	id varchar(255) NOT NULL,
	click_time BIGINT NOT NULL,
	PRIMARY KEY (id)
);

CREATE TABLE if not exists advertise_shown_join(
	id varchar(255) NOT NULL,
    shown_time BIGINT NOT NULL,
	click_time BIGINT NOT NULL,
	PRIMARY KEY (id)
);

CREATE TEMPORARY TABLE IF NOT EXISTS joined as(
	select * from (
		select click.id as id, advertisement.shown_time, click.click_time
		from advertisement
		join click
		on advertisement.id = click.id
		and advertisement.shown_time<1449151387976
		and click.click_time<1449151387976
	) as joined
	where (shown_time<click_time and shown_time+20000 >= click_time)
);