select t.name
from tracks as t
left join collections_tracks as ct on t.id = ct.track_id
where ct.track_id is null