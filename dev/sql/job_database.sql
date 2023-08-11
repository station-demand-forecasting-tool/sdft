
CREATE SCHEMA jobs;

-- job queue (used to create config.csv)

CREATE TABLE jobs.job_queue (
	job_id VARCHAR ( 20 ) PRIMARY KEY,
	timestamp timestamp default current_timestamp,
	username text UNIQUE NOT NULL,
	email text UNIQUE NOT NULL,
	method text CONSTRAINT check_method CHECK (method = 'isolation' OR method = 'concurrent'),
	testing text CONSTRAINT check_testing CHECK (testing IN ('true', 'false')),
	loglevel text CONSTRAINT check_loglevel CHECK (loglevel IN ('DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL')),
	cores int2,
	status int2
);

insert into jobs.job_queue(job_id, username, email, method, testing, loglevel, cores, status)
values
('job006','myoung6','test6@test.com','isolation','true','INFO',6, 0),
('job007','myoung7','test7@test.com','isolation','true','INFO',6, 0);

-- job stations

create table jobs.job_stations (
	job_id VARCHAR ( 20 ),
	id VARCHAR ( 20 ),
	name VARCHAR ( 20 ),
	region text,
	stn_east text,
	stn_north text,
	acc_east text,
	acc_north text,
	freq int2,
	freqgrp text,
	carsp int2,
	ticketm text,
	busint text,
	cctv text,
	terminal text,
	electric text,
	tcbound text,
	category VARCHAR ( 1 ) CONSTRAINT check_category CHECK (category IN ('E', 'F')),
	abstract text,
	PRIMARY KEY (job_id, id)
);

-- insert demo data

insert into jobs.job_stations(
	job_id,
	id,
	name,
	region,
	stn_east,
	stn_north,
	acc_east,
	acc_north,
	freq,
	carsp,
	ticketm,
	busint,
	cctv,
	terminal,
	electric,
	tcbound,
	category)
	values
	('job001','HELST1','HELSTON','South West','166257','028051','166257','028051',20,25,'true','false','true','true','false','false','E'
);


-- frequency groups

create table jobs.job_freqgroups (
	job_id VARCHAR ( 20 ),
	group_id VARCHAR ( 20 ),
	group_crs text,
	PRIMARY KEY (job_id, group_id)
);


-- exogenous

create table jobs.job_exogenous (
	id SERIAL PRIMARY KEY,
	job_id VARCHAR ( 20 ),
	type text CONSTRAINT check_group_crs CHECK (type IN ('population', 'houses', 'jobs')),
	number int2,
	centroid text
);

-- add example data

insert into jobs.job_exogenous (
job_id,
type,
number,
centroid
)
values
('job001','population',1000,'TR138JX'),
('job001','houses',150,'TR13AS'),
('job001','jobs',400,'E33048534');




