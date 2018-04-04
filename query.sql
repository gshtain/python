drop table if exists wixraptor.editor_wix_code.firsts_data_by_site_raw_data_wt_test;
create table if not exists wixraptor.editor_wix_code.firsts_data_by_site_raw_data_wt_test as
with
source_38_events as (
select
    msid

    -- takes a random esi
    -- , min_by(esi, date_created ) 																																			   as esi
    , min(esi)                                                                                                                                                                 as esi
    , max(date_created)                                                                                                                                                        as last_add_editor_component
    , min(date_created)                                                                                                                                                        as first_add_editor_component
    , sum(if(evid !=287,1,0)  )                                                                                                                                                as total_add_editor_component

    , min(if(evid=287 and category in ('dev_mode', 'properties_panel', 'show_hidden_components') and status=true   , date_trunc('minute',date_created)))  	 			       as first_dev_mode_on
    , min(if(evid=287 and category in ('dev_mode', 'properties_panel', 'show_hidden_components')  and status=false  , date_trunc('minute',date_created)))					   as first_dev_mode_off

	, max(if(evid=287 and category in ('dev_mode', 'properties_panel', 'show_hidden_components') and status=true   , date_trunc('minute',date_created)))  					   as last_dev_mode_on
	, max(if(evid=287 and category in ('dev_mode', 'properties_panel', 'show_hidden_components') and status=false  , date_trunc('minute',date_created)))					   as last_dev_mode_off

    , min(if(evid=123 and component_type ='platform.components.AppController' , date_trunc('minute',date_created))) 	                                                       as first_add_dataset
    , min(if(evid=123 and (component_type like 'wysiwyg.viewer.components.inputs.%' or component_type='wysiwyg.viewer.components.Group'), date_trunc('minute',date_created)))  as first_user_input_general
    , min(if(evid=123 and component_type = 'wysiwyg.viewer.components.Grid' , date_trunc('minute',date_created))) 		                                                       as first_add_grid
    , min(if(evid=123 and component_type = 'wysiwyg.viewer.components.Repeater' , date_trunc('minute',date_created))) 		                                                   as first_add_repeater


from users_38

where(
-- 	(date_created >= date '2017-07-25' and date_created<= date_add('day', -5, current_date)
      date_created >= date_add('day', {}, current_date) and date_created<= date_add('day', 1+{}, current_date)
     and uuid not in (select uuid from sunduk.tbl.wix_users)
    and uuid ='0ac3c7d5-9e7d-476d-9006-90e051b69f7b'
    and
    (
        (evid = 287 and category in ('dev_mode', 'properties_panel', 'show_hidden_components')) or
        (evid = 123 and (component_type in ('wysiwyg.viewer.components.Repeater','platform.components.AppController','wysiwyg.viewer.components.Grid', 'wysiwyg.viewer.components.Group') or component_type like 'wysiwyg.viewer.components.inputs%'))
    ))
group by
   msid
having min(esi) is not null
-- in some cases (rare, it's a bug) we don't have an esi
)

--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-- collects events from source 83: add collection and insert data to collection.
, source_83_events as (
select
    msid

    -- , min_by(esi, date_created ) 										as esi
    ,min(esi)                                                           as esi
    ,min(date_created)                                                  as first_collection_interaction
    ,max(date_created)                                                  as last_collection_interaction
    ,count(1)                                                           as total_collection_interaction

    , min(if(  evid=101 , date_trunc('minute',date_created))) 			as first_add_collection
    , min(if(  evid=167 , date_trunc('minute',date_created))) 			as first_cm_import_finished
    , min(if(  evid=169 , date_trunc('minute',date_created))) 			as first_cm_export_finished
    , min(if(  evid=133 , date_trunc('minute',date_created))) 			as first_insert_data_collection
    , sum(if(  evid=133 , 1,0))                               			as total_insert_data_collections
    , min(if(  evid=106 , date_trunc('minute',date_created))) 			as first_join_collection

from users_83

where
	(
-- 	date_created >= date '2017-07-25' and date_created <=  date_add('day', -5, current_date)
          date_created >= date_add('day', {}, current_date) and date_created<= date_add('day', 1+{}, current_date)
    and uuid not in (select uuid from sunduk.tbl.wix_users)
    and (evid in (101,133,167,169) or (evid = 106 and cast(json_extract(field_json,'$.type') as varchar) = 'reference') ))

group by
    msid
having min(esi) is not null

)

--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-- collects events from source 79: add/delete router,add/delete dynamic page, connect component/collection to data set, connect/disconnect component to submit button, connect user input to collection and connect component to dynamic page.
, source_79_events as (
select
    msid

    , min(esi)																															                    as esi

    ,min(date_created)                                                                                                                                      as first_platform_interaction
    ,max(date_created)                                                                                                                                      as last_platform_interaction
    ,count(1)                                                                                                                                               as total_platform_interaction

    , min(if((evid=65 and app_name='custom router') , date_trunc('minute',date_created))) 						        									as first_add_router


    , min(if((evid=62 and app_name='dynamic page') , date_trunc('minute',date_created))) 							    									as first_add_dynamic_page
    , min(if((evid=74 or (evid= 73 and component_type='wysiwyg.viewer.components.Grid') or evid = 78)    	, date_trunc('minute',date_created))) 				as first_data_binding
    , max(if((evid=74 or (evid= 73 and component_type='wysiwyg.viewer.components.Grid') or evid = 78)    	, date_trunc('minute',date_created))) 				as last_data_binding

	, min(if(((evid=74 and ds_type='dataset') or (evid= 73 and component_type='wysiwyg.viewer.components.Grid')), date_trunc('minute',date_created)))    	as first_connect_component_to_DS
    , min(if((evid=74 and ds_type='router_dataset') , date_trunc('minute',date_created))) 							    									as first_connect_component_to_DP
    , min(if((evid=74 and repeater_parent_id is not null and ds_type='dataset')			, date_trunc('minute',date_created))) 								as first_connect_repeater_to_DS
    , min(if((evid=74 and repeater_parent_id is not null and ds_type='router_dataset' ) , date_trunc('minute',date_created)))   							as first_connect_repeater_to_DP

    , min(if((evid=74 and component_type='wysiwyg.viewer.components.SiteButton'
        and field_name='save'
		and field_type='action'
        and app_id='dataBinding'
		and property='link'
        and  app_name='databinding') , date_trunc('minute',date_created))) 	                                            									 as first_connect_comp_to_Submit_Button

    , min(if((evid=100) , date_trunc('minute',date_created))) 						                            	    									 as first_add_IDE_file
    , sum(if((evid=100) , 1,0)) 							                                                            									 as total_add_IDE_file
	,min(if ((evid =114 and toggle_name = 'open') or (evid = 113 and item_name!=' minimize')
	            or (evid=20 and cast(substring(new_hight,1,length(new_hight)-1) as double)<=80),date_trunc('minute',date_created)))                          as first_opened_ide
	,sum(if ((evid =114 and toggle_name = 'open')
	or (evid = 113 and item_name!=' minimize') or (evid=20 and cast(substring(new_hight,1,length(new_hight)-1) as double)<=80),1,0))                         as total_opened_ide

from users_79

where
-- 	date_created >= date '2017-07-25' and date_created<= date_add('day', -5, current_date)
      date_created >= date_add('day', {}, current_date) and date_created<= date_add('day', 1+{}, current_date)
    and uuid not in (select uuid from sunduk.tbl.wix_users)

    and
    (
    (	evid=65  and app_name='custom router') -- add router
    or (evid=62  and app_name='dynamic page') -- add dynamic page
    or (evid=52  and app_name='databinding' and panel_name='CREATE_DATASET_PANEL') --connect component to DS
    or (evid in (74,78 )) --data binding
    or (evid=100 ) -- add IDE file
    or (evid= 73 and component_type='wysiwyg.viewer.components.Grid')
	or (evid in (20,114,113))

    )
group by
    msid
having min(esi) is not null

)
 --~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- collects events from source 39: use hidden/collapsed/enabled on load visible/not visible, use onViewportEnter on/off, use onViewportLeave on/off, use onMouseIn on/off, use onMouseout on/off, use onclick on/off and use onDbClick on/off.
, source_39_events as (
select
      msid

     , min(esi) 																                as esi
     ,min(date_created)                                                                         as first_properties_panel_action
     ,max(date_created)                                                                         as last_properties_panel_action
     ,count(1)                                                                                  as total_properties_panel_action
     , min(if((evid=148 and event='onViewportEnter') , date_trunc('minute',date_created))) 		as first_use_onViewportEnter_on
     , min(if((evid=148 and event='onViewportLeave') , date_trunc('minute',date_created))) 		as first_use_onViewportLeave_on
     , min(if((evid=148 and event='onMouseIn') 	     , date_trunc('minute',date_created))) 		as first_use_onMouseIn_on
     , min(if((evid=148 and event='onMouseOut')	     , date_trunc('minute',date_created)))		as first_use_onMouseOut_on
     , min(if((evid=148 and event='onClick') 		 , date_trunc('minute',date_created))) 	    as first_use_onClick_on
     , min(if((evid=148 and event='onDbClick') 	     , date_trunc('minute',date_created))) 	    as first_use_onDbClick_on


from users_39

where
-- 	date_created >= date '2017-07-25' and date_created<= date_add('day', -5, current_date)
          date_created >= date_add('day', {}, current_date) and date_created<= date_add('day', 1+{}, current_date)
    and uuid not in (select uuid from sunduk.tbl.wix_users)
    and evid in ( 148, 151)
group by
    msid
having min(esi) is not null
)


--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- full join all event tables - for each msid, uuid we take one of the esi's
-- minimum date from the firsts, maximum on the lasts and sum of the events of the total
, firsts as (
select
     coalesce(a.msid,b.msid,c.msid,d.msid) 																	as msid
    ,min(coalesce(a.esi,b.esi,c.esi,d.esi))																	as esi
    --source 38
        ,min(a.first_add_editor_component)   																as first_add_editor_component
        ,max(a.last_add_editor_component)   																as last_add_editor_component
        ,sum(a.total_add_editor_component)                                                                  as total_add_editor_component
        ,min(a.first_dev_mode_on)  																			as first_dev_mode_on
        ,min(a.first_dev_mode_off) 																			as first_dev_mode_off
        ,max(a.last_dev_mode_on)   																			as last_dev_mode_on
        ,max(a.last_dev_mode_off)  																			as last_dev_mode_off
        ,min(a.first_add_dataset)  																			as first_add_dataset
        ,min(a.first_user_input_general) 																	as first_user_input_general
        ,min(a.first_add_grid) 			 																	as first_add_grid
        ,min(a.first_add_repeater) 		 																	as first_add_repeater
       --
    --source_83
        ,min(b.first_collection_interaction)                                                                as first_collection_interaction
        ,max(b.last_collection_interaction)                                                                 as last_collection_interaction
        ,sum(b.total_collection_interaction)                                                                as total_collection_interaction
        ,min(b.first_add_collection) 	 																	as first_add_collection
        ,min(b.first_cm_import_finished) 	 																as first_cm_import_finished
        ,min(b.first_cm_export_finished) 	 																as first_cm_export_finished
        ,min(b.first_insert_data_collection)  																as first_insert_data_collection
        ,sum(b.total_insert_data_collections) 																as total_insert_data_collections
        ,min(b.first_join_collection)  																		as first_join_collection
    --
    --source_79
        ,min(c.first_platform_interaction)       														    as first_platform_interaction
        ,min(c.last_platform_interaction)       															as last_platform_interaction
        ,min(c.total_platform_interaction)       															as total_platform_interaction
        ,min(c.first_add_router)       																		as first_add_router
        ,min(c.first_add_dynamic_page) 																		as first_add_dynamic_page
		,min(c.first_data_binding)																			as first_data_binding
        ,max(c.last_data_binding)																			as last_data_binding
        ,min(c.first_connect_component_to_DS) 																as first_connect_component_to_DS
        ,min(c.first_connect_component_to_DP) 																as first_connect_component_to_DP
        ,min(c.first_connect_repeater_to_DS) 																as first_connect_repeater_to_DS
        ,min(c.first_connect_repeater_to_DP) 																as first_connect_repeater_to_DP
        ,min(c.first_connect_comp_to_Submit_Button) 	  													as first_connect_comp_to_Submit_Button
        ,min(c.first_add_IDE_file) 																			as first_add_IDE_file
        ,sum(c.total_add_IDE_file) 																			as total_add_IDE_file
		,min(c.first_opened_ide) 																			as first_opened_ide
        ,sum(c.total_opened_ide) 																			as total_opened_ide
    --
    --source_39

        ,max(d.first_properties_panel_action)   															as first_properties_panel_action
        ,max(d.last_properties_panel_action)   																as last_properties_panel_action
        ,sum(d.total_properties_panel_action)  																as total_properties_panel_action

        ,min(d.first_use_onViewportEnter_on)  																as first_use_onViewportEnter_on

        ,min(d.first_use_onViewportLeave_on)  																as first_use_onViewportLeave_on

        ,min(d.first_use_onMouseIn_on)  																	as first_use_onMouseIn_on

        ,min(d.first_use_onMouseOut_on) 																	as first_use_onMouseOut_on

        ,min(d.first_use_onClick_on)    																	as first_use_onClick_on

        ,min(d.first_use_onDbClick_on)  																	as first_use_onDbClick_on
    --
from source_38_events a

full join source_83_events b
on a.msid=b.msid

full join source_79_events c
on a.msid=c.msid

full join source_39_events d
on a.msid=d.msid

where
    coalesce(a.msid,b.msid,c.msid,d.msid)!='a573279f-ae6f-46d1-8556-7c93ae9b2c84'
-- exclude one problematic msid
group by 1
)

--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  , fix_msid as (--

select
          esi
        , min_by(msid, date_created) as first_msid
        , max_by(msid, date_created) as last_msid
from events.dbo.users_42 u42
where evid in (270, 271, 107, 108, 103, 104) -- added server events to account for client events not firing
     and  date_created >= date_add('day', {}, current_date) and date_created<= date_add('day', 1+{}, current_date)
    -- and date_created > date '2017-07-25' and date_created<= date_add('day', -5, current_date)
    --and msid in (select msid from firsts group by 1)
group by esi
having min_by(msid, date_created) != max_by(msid, date_created) -- if we only want sessions where MSID changed


)


select
     coalesce(b.last_msid,a.msid)                      														as new_msid

    ,min(if(array_min(array[
                      coalesce (first_add_editor_component						, date '3000-01-01')
					 ,coalesce (first_collection_interaction                    , date '3000-01-01')
					 ,coalesce (first_platform_interaction                      , date '3000-01-01')
					 ,coalesce (first_properties_panel_action                   , date '3000-01-01')

				]) = date '3000-01-01'
        ,null
        ,array_min(array[
                      coalesce (first_add_editor_component						, date '3000-01-01')
					 ,coalesce (first_collection_interaction                    , date '3000-01-01')
					 ,coalesce (first_platform_interaction                      , date '3000-01-01')
					 ,coalesce (first_properties_panel_action                   , date '3000-01-01')
                    ])) )																		            as first_dev_mode_on_estimate
    ,max(if(array_max(array[
                      coalesce (first_add_editor_component						, date '1900-01-01')
					 ,coalesce (first_collection_interaction                    , date '1900-01-01')
					 ,coalesce (first_platform_interaction                      , date '1900-01-01')
					 ,coalesce (first_properties_panel_action                   , date '1900-01-01')

				]) = date '3000-01-01'
        ,null
        ,array_max(array[
                      coalesce (first_add_editor_component						, date '1900-01-01')
					 ,coalesce (first_collection_interaction                    , date '1900-01-01')
					 ,coalesce (first_platform_interaction                      , date '1900-01-01')
					 ,coalesce (first_properties_panel_action                   , date '1900-01-01')
                    ])) )																		            as last_wix_code_activity
    --source 38
        ,min(a.first_add_editor_component)   																as first_add_editor_component
        ,max(a.last_add_editor_component)   																as last_add_editor_component
        ,sum(a.total_add_editor_component)                                                                  as total_add_editor_component
        ,min(a.first_dev_mode_on)  																			as first_dev_mode_on
        ,min(a.first_dev_mode_off) 																			as first_dev_mode_off
        ,max(a.last_dev_mode_on)   																			as last_dev_mode_on
        ,max(a.last_dev_mode_off)  																			as last_dev_mode_off
        ,min(a.first_add_dataset)  																			as first_add_dataset
        ,min(a.first_user_input_general) 																	as first_user_input_general
        ,min(a.first_add_grid) 			 																	as first_add_grid
        ,min(a.first_add_repeater) 		 																	as first_add_repeater
       --
    --source_83
        ,min(a.first_collection_interaction)                                                                as first_collection_interaction
        ,max(a.last_collection_interaction)                                                                 as last_collection_interaction
        ,sum(a.total_collection_interaction)                                                                as total_collection_interaction
        ,min(a.first_add_collection) 	 																	as first_add_collection
        ,min(a.first_cm_import_finished) 	 																as first_cm_import_finished
        ,min(a.first_cm_export_finished) 	 																as first_cm_export_finished
        ,min(a.first_insert_data_collection)  																as first_insert_data_collection
        ,sum(a.total_insert_data_collections) 																as total_insert_data_collections
        ,min(a.first_join_collection)  																		as first_join_collection
    --
    --source_79
        ,min(a.first_platform_interaction)       														    as first_platform_interaction
        ,min(a.last_platform_interaction)       															as last_platform_interaction
        ,min(a.total_platform_interaction)       															as total_platform_interaction
        ,min(a.first_add_router)       																		as first_add_router
        ,min(a.first_add_dynamic_page) 																		as first_add_dynamic_page
		,min(a.first_data_binding)																			as first_data_binding
        ,max(a.last_data_binding)																			as last_data_binding
        ,min(a.first_connect_component_to_DS) 																as first_connect_component_to_DS
        ,min(a.first_connect_component_to_DP) 																as first_connect_component_to_DP
        ,min(a.first_connect_repeater_to_DS) 																as first_connect_repeater_to_DS
        ,min(a.first_connect_repeater_to_DP) 																as first_connect_repeater_to_DP
        ,min(a.first_connect_comp_to_Submit_Button) 	  													as first_connect_comp_to_Submit_Button
        ,min(a.first_add_IDE_file) 																			as first_add_IDE_file
        ,sum(a.total_add_IDE_file) 																			as total_add_IDE_file
		,min(a.first_opened_ide) 																			as first_opened_ide
        ,sum(a.total_opened_ide) 																			as total_opened_ide
    --
    --source_39

        ,max(a.first_properties_panel_action)   															as first_properties_panel_action
        ,max(a.last_properties_panel_action)   																as last_properties_panel_action
        ,sum(a.total_properties_panel_action)  																as total_properties_panel_action

        ,min(a.first_use_onViewportEnter_on)  																as first_use_onViewportEnter_on

        ,min(a.first_use_onViewportLeave_on)  																as first_use_onViewportLeave_on

        ,min(a.first_use_onMouseIn_on)  																	as first_use_onMouseIn_on

        ,min(a.first_use_onMouseOut_on) 																	as first_use_onMouseOut_on

        ,min(a.first_use_onClick_on)    																	as first_use_onClick_on

        ,min(a.first_use_onDbClick_on)  																	as first_use_onDbClick_on
from
firsts a
left join fix_msid b
on a.esi = b.esi
    and a.msid = b.first_msid
group by 1