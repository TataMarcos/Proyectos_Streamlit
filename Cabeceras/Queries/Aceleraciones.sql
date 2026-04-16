with art as (
select c.class_name as clase, s.sub_name as subclase, im.item, laa.artc_artc_id, laa.artc_artc_desc
from mstrdb.dwh.item_master im
join mstrdb.dwh.subclass s on im.subclase = s.subclase
join mstrdb.dwh.class c on im.clase = c.clase
join mstrdb.dwh.lu_artc_articulo laa on laa.orin = im.item
)

, p as (
select distinct c.geog_locl_id, c.geog_locl_cod, c.geog_locl_desc, a.puntera, a.seccion, a.item,
        a.fecha_inicio, a.fecha_fin, DATEDIFF('DAY', a.fecha_inicio, a.fecha_fin) + 1 DURACION_PUNTERA
from sandbox_plus.dwh.INPUT_PUNTERAS a join mstrdb.dwh.lu_geog_local c on c.geog_locl_cod=a.local
where upper(puntera) not like '%EX%BI%' and seccion<>'NF' order by puntera, geog_locl_cod)

, basal as (
Select tiem_dia_id, artc_artc_id, geog_locl_id, ROUND(avg(venta_basal), 2) avg_basal_180, ROUND(avg(venta_REAL), 2) avg_REAL_180 
from bizmetriks.dwh.ft_venta_basal
where tiem_dia_id between dateadd('month', -6, date_trunc('month', current_date())) and date_trunc('month', current_date()) - 1
group by all)

, v as (
select fv.tiem_dia_id, fv.geog_locl_id, fv.artc_artc_id, sum(fv.vnta_importe_con_iva) as venta, sum(fv.vnta_unidades) as unidades
from mstrdb.dwh.ft_ventas fv
where tiem_dia_id between dateadd('month', -6, date_trunc('month', current_date())) and date_trunc('month', current_date()) - 1 group by all)

, vp as (
select p.fecha_inicio, p.fecha_fin, v.geog_locl_id, p.geog_locl_cod, p.geog_locl_desc, p.puntera, p.seccion, p.DURACION_PUNTERA,
    art.clase, art.subclase, art.artc_artc_id, art.item, art.artc_artc_desc, round(sum(avg_basal_180)/p.DURACION_PUNTERA, 2) as avg_basal_180,
        round(sum(v.unidades)/p.DURACION_PUNTERA, 2) as unidades_promedio
from v join art on art.artc_artc_id = v.artc_artc_id
join p on p.item = art.item and p.geog_locl_id = v.geog_locl_id and v.tiem_dia_id between p.fecha_inicio and p.fecha_fin
join basal on basal.artc_artc_id = v.artc_artc_id and basal.geog_locl_id = v.geog_locl_id and v.tiem_dia_id = basal.tiem_dia_id group by all
)

, ac as (
select *, round(div0(unidades_promedio, avg_basal_180), 2) as aceleracion_art,
    round(median(aceleracion_art) over (partition by subclase), 2) as aceleracion_subclase,
    round(median(aceleracion_art) over (partition by clase), 2) as aceleracion_clase from vp)

, f as (
select geog_locl_id, geog_locl_cod as local, geog_locl_desc, clase, subclase, artc_artc_id, item, artc_artc_desc,
    round(avg(aceleracion_art), 2) as aceleracion_art, round(avg(aceleracion_subclase), 2) as aceleracion_subclase,
    round(avg(aceleracion_clase), 2) as aceleracion_clase
from ac group by all)

select distinct * from f;