SELECT 'abc' LIKE 'a%';
SELECT 'abc' NOT LIKE 'a%';
SELECT 'abc' NOT LIKE 'b%';
SELECT 'a%b' LIKE 'a!%b' ESCAPE '!';
SELECT 'a_b' LIKE 'a!_b' ESCAPE '!';
SELECT 'a!b' LIKE 'a!!b' ESCAPE '!';
SELECT NULL LIKE 'a%';
SELECT 'abc' LIKE NULL;
SELECT NULL NOT LIKE 'a%';
