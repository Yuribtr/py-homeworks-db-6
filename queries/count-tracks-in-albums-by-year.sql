select t.name, a.year
from albums as a
left join tracks as t on t.album_id = a.id
where (a.year >= {}) and (a.year <= {})