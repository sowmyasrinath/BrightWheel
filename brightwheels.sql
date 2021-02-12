--How many Family Child Care Home providers are there in the dataset?
SELECT count(*)
from merged
where "Type" = 'Family Child Care Home';

-- Which Zip code has the most providers?
SELECT Zip, count(Phone) cnt
FROM merged
group by Zip
order by cnt DESC
;