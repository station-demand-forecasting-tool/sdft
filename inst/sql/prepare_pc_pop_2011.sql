-- update pc_pop_2011 with county and la codes

alter table pc_pop_2011 add column oscty text, add COLUMN oslaua text

UPDATE pc_pop_2011 a
SET oscty = b.oscty,
oslaua = b.oslaua
FROM onspd_nov_2015 b
WHERE a.postcode = b.postcode
