select distinct a.name
from albums as a
left join tracks as t on t.album_id = a.id
where t.album_id in (
    select album_id
    from tracks
    group by album_id
    having count(id) = (
        select count(id)
        from tracks
        group by album_id
        order by count
        limit 1
    )
)
order by a.name