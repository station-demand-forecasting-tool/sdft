
CREATE SCHEMA jobs;

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