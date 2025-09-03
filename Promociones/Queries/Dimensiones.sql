with basal as (
Select artc_artc_id, geog_locl_id, ROUND(avg(venta_basal), 2) avg_basal_180, ROUND(avg(venta_REAL), 2) avg_REAL_180 
from bizmetriks.dwh.ft_venta_basal where tiem_dia_id >= current_date() - 181 group by all)

select distinct local, lgl.geog_locl_desc as nombre_tienda, g.group_name as grupo, c.class_name as clase, s.sub_name as subclase,
        im.item, laa.artc_artc_desc, avg_basal_180, dim.length as profundidad, dim.width as frente, dim.height as altura,
    floor(130/dim.width) items_por_frente, case when floor(30/dim.length) = 0 then 1 else floor(30/dim.length) end as items_por_lateral,
    items_por_lateral*items_por_frente cant_max_x_estante,
        case
        when items_por_lateral > 2 then 2*items_por_frente
        else cant_max_x_estante
        end as cant_min_x_estante
from sandbox_plus.dwh.V_ITEM_SUPP_COUNTRY_DIM dim
join mstrdb.dwh.item_master im on dim.item = im.item join mstrdb.dwh.deps d on d.dept = im.dept
join mstrdb.dwh.groups g on d.group_no = g.group_no and g.group_no in (1, 3, 4, 11, 12, 14, 15, 18)
join mstrdb.dwh.class c on im.clase = c.clase join mstrdb.dwh.subclass s on im.subclase = s.subclase
join mstrdb.dwh.lu_artc_articulo laa on laa.orin = im.item
join sandbox_plus.dwh.input_punteras ip on laa.orin = ip.item
join mstrdb.dwh.lu_geog_local lgl on lgl.geog_locl_cod = ip.local
left join basal on basal.artc_artc_id = laa.artc_artc_id and basal.geog_locl_id = lgl.geog_locl_id
where dim_object='EA' and lwh_uom='CM';