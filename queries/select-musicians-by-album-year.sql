select distinct m.name
from musicians as m
where m.name not in (
    select distinct m.name
    from musicians as m
    left join albums_musicians as am on m.id = am.musician_id
    left join albums as a on a.id = am.album_id
    where a.year = {}
)
order by m.name