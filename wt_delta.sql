drop table if exists wixraptor.editor_wix_code.firsts_data_by_site_delta;
create table if not exists wixraptor.editor_wix_code.firsts_data_by_site_delta as


with sites as (select    site_date_created
                        ,site_type
                        ,msid
                        ,uuid
                        ,transfer_date_created
                        ,transfer_origin_msid
                        ,transfer_origin_uuid
                        ,url
                        ,row_number() over (partition by msid,uuid order by coalesce(site_type,0) desc) as sort_id
                    from sunduk.tbl.html_site_ng
                where application_type='HtmlWeb'
                    and (site_type=2 or site_type is null)
                    and document_type='UGC'
                    and msid in (select new_msid as msid from wixraptor.editor_wix_code.firsts_data_by_site_raw_data_wt)
                )

, data as (
select

      b.site_type             as site_type
    , b.site_date_created     as site_date_created
    , if(b.msid is null ,1,0) as is_template
    , b.uuid                  as uuid_owner
    , a.*
    , b.transfer_date_created
    , b.transfer_origin_msid
    , b.transfer_origin_uuid
    , b.url

from
wixraptor.editor_wix_code.firsts_data_by_site_raw_data_wt a
left join sites b
on a.new_msid=b.msid and sort_id=1

)
--  select count(1) from data   --349680

, publish as (

select
    date_trunc('minute',date_created) as date_created
    , msid

from users_42

where
    evid in (110)
    and editor_version='1.4'
    and ds_origin!='onboarding'
    and  date_created >= date_add('day', -2, current_date) and date_created<= date_add('day', -1, current_date)
    and msid in (select new_msid as msid from data)
)



, firsts_publish as (

select
    new_msid
    , min(a.date_created) as first_publish_after_dev_mode_on
from
    wixraptor.editor_wix_code.firsts_data_by_site_raw_data_wt b
inner join publish a
    on  b.new_msid = a.msid
    and a.date_created >= b.first_dev_mode_on_estimate
    and if(a.date_created<= last_dev_mode_off or last_dev_mode_off is null,1,0)=1
group by 1
)



select
    b.first_publish_after_dev_mode_on
    ,a.*

from
    data a
    left join
    firsts_publish b
    on a.new_msid=b.new_msid