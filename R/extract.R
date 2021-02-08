# ./extract.r
# This file extracts the data needed for marketing reports
# Blake Abbenante
# 10/1/19

library(glue)
      
#load our old dataset, so there aren't any missing pieces
load("~/OneDrive - CBRE, Inc/data/raw_data/ELT_raw_data.RData")

                                
# get contact records
hs_contacts<-  dbGetQuery(ft_con,
                          "with current_contacts as
(
select contact_id
from hubspot.contact_list_member
where contact_list_id=30
and _fivetran_deleted=false
)

SELECT ct.*
FROM hubspot.contact ct
join current_contacts cc
on ct.id=cc.contact_id

")
#names(hs_contacts) <- substring(names(hs_contacts), 10)
#names(hs_contacts)[1]<-"id"
#names(hs_contacts)[2]<-"fivetran_deleted"

# get contact lifecycle changes
hs_contact_history_lifecycle<-dbGetQuery(ft_con,
                               "SELECT * from hubspot.contact_property_history where name='lifecyclestage'")
names(hs_contact_history_lifecycle)[2]<-'property_name'

# get local fill deals
hs_deals<-dbGetQuery(ft_con,
                     "SELECT d.*,
                     o.email as sales_owner
                     FROM hubspot.deal d
                     left join hubspot.owner o
                     on d.owner_id=o.owner_id;")

# get local fill deal stage changes
hs_deal_history<-dbGetQuery(ft_con,
                            "SELECT 
deal_id
, ph.name
,date_trunc('minute', ph.timestamp) as date_created
,ps.label as pipeline_stage
, ph.source_id
, ph.source                            
FROM hubspot.deal_property_history ph
join hubspot.deal_pipeline_stage ps
on ph.value=ps.stage_id
and ps.pipeline_id='default'
where ph.name='deal_pipeline_stage_id'
;")

hs_isr_history<-dbGetQuery(ft_con,
                           "select 
contact_id ,
timestamp ,
value as status
FROM hubspot.contact_property_history
where name  = 'isr_review'
order by contact_id ,
timestamp")


# get contacts associated with deals
hs_deal_contacts<-dbGetQuery(ft_con,
                             "SELECT deal_id, contact_id from hubspot.deal_contact;")


# get companies associated with deals
hs_deal_companies<-dbGetQuery(ft_con,
                             "SELECT deal_id, company_id from hubspot.deal_company;")

hs_companies<-dbGetQuery(ft_con,
                         "SELECT * from hubspot.company;")

#get conversion probabilities by stage
hs_deal_probabilities<-dbGetQuery(ft_con,"SELECT ps.pipeline_id
,pl.label as pipeline
,ps.label as pipeline_stage
,ps.stage_id
,ps.probability 
FROM hubspot.deal_pipeline_stage ps 
join hubspot.deal_pipeline pl
on ps.pipeline_id=pl.pipeline_id
where ps.active='true';")

#get all engagement counts by contact
hs_contact_engagements<-dbGetQuery(ft_con,"select ec.contact_id,
eg.type,
count(*) as engagements
from hubspot.engagement_contact ec
join hubspot.engagement eg
on ec.engagement_id=eg.id
group by ec.contact_id,
eg.type")

#get phone call details
hs_phone_call_log<-dbGetQuery(ft_con,"select  
ecc.contact_id,
ec.disposition,
e.created_at,
ec.to_number,
ec.from_number,
ec.status,
ec.external_id,
ec.duration_milliseconds,
ec.body
FROM hubspot.engagement_call ec
join hubspot.engagement_contact ecc
on ec.engagement_id=ecc.engagement_id
join hubspot.engagement e
on ec.engagement_id=e.id")

#get engagement email details
hs_email_log<-dbGetQuery(ft_con,"select 
ecc.contact_id,
--ee.thread_id,
e.type,
min(e.created_at)
from
hubspot.engagement_email ee
join hubspot.engagement_contact ecc
on ee.engagement_id=ecc.engagement_id
join hubspot.engagement e
on ee.engagement_id=e.id
group by ecc.contact_id,
--ee.thread_id,
e.type
")

#get all scheduled tours and calls
# hs_tours_and_calls<-dbGetQuery(ft_con,"select distinct ct.id as contact_id
# ,ct.property_email
# ,ct.property_in_which_service_are_you_interested_
# ,ct.property_in_which_location_are_you_interested_
# ,case when lower(title) like '%excited for your tour%' then 'Tour' else 'Call' end as meeting_type
# ,ct.property_hs_analytics_source
# ,max(em.start_time) as tour_time
# ,he.created_at as create_time
# from hubspot.engagement_meeting em
# join hubspot.engagement_contact ec
# on em.engagement_id=ec.engagement_id
# join hubspot.contact ct
# on ec.contact_id=ct.id
# join hubspot.engagement he
# on em.engagement_id=he.id
# where lower(title) similar to '%(excited for your tour|learn more about hana)%'
# group by ct.id
# ,ct.property_email
# ,ct.property_in_which_service_are_you_interested_
# ,ct.property_in_which_location_are_you_interested_
# ,case when lower(title) like '%excited for your tour%' then 'Tour' else 'Call' end 
# ,ct.property_hs_analytics_source
# ,he.created_at;")

hs_tours_and_calls<-dbGetQuery(ft_con,"select ct.id as contact_id
,ct.property_email
,ct.property_in_which_service_are_you_interested_
,ct.property_in_which_location_are_you_interested_
,case when lower(title) similar to '%(excited for your tour|tour of hana)%' then 'Tour' else 'Call' end as meeting_type
,ct.property_hs_analytics_source
,em.start_time as tour_time
,he.created_at as create_time
,he.activity_type 
from hubspot.engagement_meeting em
join hubspot.engagement_contact ec
on em.engagement_id=ec.engagement_id
join hubspot.contact ct
on ec.contact_id=ct.id
join hubspot.engagement he
on em.engagement_id=he.id
where lower(title) similar to '%(excited for your tour|learn more about hana|tour of hana)%'
order by create_time desc;")


#get list of deals that moved from broker pipeline to local fill pipeline
hs_broker_to_local_fill_deals<-dbGetQuery(ft_con,"with broker_nurture_current_pipeline_deals as
(
  SELECT distinct deal_id, count(distinct value) as num_pipelines
  FROM hubspot.deal_property_history
  where name = 'deal_pipeline_id'
  and value in ('default','517397')
  group by deal_id
  order by count(distinct value) desc
)

select deal_id,
'yes' as broker_pipeline
from broker_nurture_current_pipeline_deals
where num_pipelines=2;")


# get broker nurture deal stage changes
hs_broker_deal_history<-dbGetQuery(ft_con,
                                   "SELECT 
deal_id
, ph.name
,date_trunc('minute', ph.timestamp) as date_created
,ps.label as pipeline_stage
, ph.source_id
, ph.source                            
FROM hubspot.deal_property_history ph
join hubspot.deal_pipeline_stage ps
on ph.value=ps.stage_id
and ps.pipeline_id='517397'
where ph.name='deal_pipeline_stage_id';")

#form conversion data

form_data<-dbGetQuery(ft_con,"SELECT page_hostname,
page_page_path,
visit_start_time,
visit_id,
event_info_action,
count(*)
FROM google_analytics_360.session_hit
where event_info_category = 'lead form'
and event_info_action in ('lead form viewed','lead form submitted')
group by page_hostname,
page_page_path,
visit_start_time,
visit_id,
event_info_action")

#get ad network spend data
marketing_spend<-dbGetQuery(ft_con,"with fb_ad_conversions as (
select date, 
ad_id,
count(*) as conversions
from facebook.facebook_actions
where action_type in ('onsite_conversion.post_save','leadgen.other')
group by date,
ad_id
), 

bing_campaigns as (
SELECT distinct id,
name 
FROM bingads.campaign_history),

linkedin_campaigns as
(
select * from
(
select
id,
name,
rank() over (partition by id order by last_modified_time desc) as pos
FROM linkedin_ads.campaign_group_history 
) b
 where pos=1
)



SELECT cgh.name as campaign_name
,date(abc.day) as campaign_date
,abc.impressions
,abc.clicks
,abc.external_website_conversions as conversions
--,abc.external_website_post_view_conversions
,abc.cost_in_usd as spend,
'LinkedIn' as source
FROM linkedin_ads.ad_analytics_by_campaign abc
join
linkedin_ads.campaign_history ch
on abc.campaign_id=ch.id
join linkedin_campaigns cgh
on ch.campaign_group_id=cgh.id
where ch.account_id<>506493913

UNION

SELECT ch.name as campaign_name
,date as campaign_date
,sum(impressions) as impressions
,sum (clicks) as clicks
,sum (conversions) as conversions
,sum (spend) as spend
,'Bing' as source
FROM bingads.campaign_performance_daily_report cd
join bing_campaigns ch
on cd.campaign_id=ch.id
group by 
ch.name,
date

union

SELECT fb.campaign_name as campaign_name
,fb.date as campaign_date
,fb.impressions
,fb.inline_link_clicks as clicks
,fac.conversions
,fb.spend
,'Facebook' as source
FROM facebook.facebook fb
left join fb_ad_conversions fac
on fb.ad_id=fac.ad_id
and fb.date=fac.date
WHERE fb.account_id<>341070269934784

union

SELECT campaign_name
,date as campaign_date
,sum(impressions) as impressions
,sum(clicks) as clicks
,sum(conversions) as conversions
,sum(cost) as spend
,'Google' as source
FROM adwords.adwords_campaign_hourly
group by date,
campaign_name 

union

SELECT campaign_name
,date as campaign_date
,sum(impressions) as impressions
,sum(clicks) as clicks
,sum(conversions) as conversions
,sum(cost) as spend
,'Google' as source
FROM adwords.adwords_campaign_hourly_uk
group by date,
campaign_name")

isr_engagement_activity<-dbGetQuery(ft_con,"with isr_engagement_calls as (
select id,
created_at
FROM hubspot.engagement
where owner_id=40490781
and type = 'CALL'
),

isr_engagement_emails as (
select id
,created_at
FROM hubspot.engagement
where owner_id=40490781
and type = 'EMAIL'
),

isr_engagement_meetings as (
select id,
created_at
FROM hubspot.engagement
where owner_id=40490781
and type = 'MEETING'
)

select 
ec.contact_id,
'Call' as engagement_type,
ic.created_at,
ecc.disposition as status,
'' as subject,
ecc.body
from  hubspot.engagement_contact ec 
join isr_engagement_calls ic
on ec.engagement_id=ic.id
left join hubspot.engagement_call ecc
on ec.engagement_id=ecc.engagement_id


UNION

select 
ec.contact_id,
'Meeting' as engagement_type,
im.created_at,
'' as status,
ecm.title as subject,
ecm.body 
from  hubspot.engagement_contact ec 
join isr_engagement_meetings im
on ec.engagement_id=im.id
left join hubspot.engagement_meeting ecm
on ec.engagement_id=ecm.engagement_id


UNION

select 
ec.contact_id,
'Email' as engagement_type,
ie.created_at,
ece.status,
ece.subject,
ece.text as body 
from  hubspot.engagement_contact ec 
join isr_engagement_emails ie
on ec.engagement_id=ie.id
left join hubspot.engagement_email ece
on ec.engagement_id=ece.engagement_id")

isr_task_activity<-dbGetQuery(ft_con,"with isr_engagement_tasks as(
SELECT id,
created_at
FROM hubspot.engagement
where owner_id=40490781
and type = 'TASK'
)

select 
ec.contact_id,
et.task_type ,
ie.created_at,
TO_TIMESTAMP(et.completion_date/ 1000) as completed_date,
et.status,
et.subject,
et.body
from  hubspot.engagement_contact ec 
join isr_engagement_tasks ie
on ec.engagement_id=ie.id
left join hubspot.engagement_task et
on ec.engagement_id=et.engagement_id")

nexudus_meetings<-dbGetQuery(edp_con,"with valid_bookings as (
select distinct * from
(
SELECT id as booking_id,
coworkerid,
coworkerfullname,
resourceid,
resourcename,
resourceresourcetypename,
teamsatthetimeofbooking,
tariffatthetimeofbooking,
fromtime,
totime,
coworkerextraserviceprice,
createdon,
updatedon,
is_deleted,
rank() over (partition by id order by updatedon desc) as pos
FROM landing_nexudus.bookings
) b
 where pos=1
),

nexudus_resources as(
select * from
(SELECT id,
name,
resourcetypename,
businessid,
rank() over (partition by id order by updatedon desc) as pos
FROM landing_nexudus.resource
) rs
where 
rs.pos=1
),

meet_resources as
(
select distinct rs.id as resource_id,
b.id as business_id,
b.name as hana_location,
rs.name as room_name,
rs.resourcetypename as room_size
from
(
SELECT id,
name,
rank() over (partition by id order by updatedon desc) as pos
FROM landing_nexudus.business
) b
join nexudus_resources rs
on b.id=rs.businessid
where 
b.pos=1
),

customer_types as
(select 
id,
custom6,
custom8
 from
(
select 
id,
custom6,
custom8,
rank() over (partition by id order by updatedon desc) as pos
from landing_nexudus.coworker c
) cs
where pos=1
),

coworker_teams as
(select * from
(select distinct
c.id as coworker_id,
t.name as team_name,
rank() over (partition by c.id,t.id order by t.updatedon desc) as pos
from landing_nexudus.coworker c 
left join landing_nexudus.team t 
on c.teamid =t.id 
where t.name is not null
) ct
where ct.pos=1
),

booking_costs as
(SELECT bookingid,
coworkerid,
sum(case when price<0 then price else 0 end) as discount,
sum(case when price>0 then price else 0 end) as gross_price
from
	(select *,
	rank() over (partition by id order by updatedon desc) as pos
	FROM landing_nexudus.coworker_extraservices) b
where pos=1
group by bookingid,
coworkerid
)

select b.*,
bc.bookingid,
bc.discount,
bc.gross_price,
cw.email,
cw.companyname,
cwn.custom6 as member_product,
cwn.custom8 as member_type,
mr.hana_location,
mr.room_size,
ct.team_name
from 
valid_bookings b
join operation_datamart.coworker cw
on b.coworkerid=cw.coworker_id
left join booking_costs bc
on b.booking_id=bc.bookingid
join customer_types cwn
on cw.coworker_id=cwn.id
join  meet_resources mr
on b.resourceid=mr.resource_id
left join coworker_teams ct
on cw.coworker_id =ct.coworker_id")



# hs_deal_notes<-dbGetQuery(ft_con,"with deal_notes as (select * from (SELECT
# ed.engagement_id, ed.deal_id, eg.created_at, rank() over (partition by
# ed.deal_id order by eg.created_at desc) as pos FROM hubspot.engagement_deal ed
# join hubspot.engagement eg on ed.engagement_id=eg.id and eg.type = 'NOTE') b
# --where pos=1 )
#
# select d.deal_id, d.created_at, n.body as notes, d.pos from
# hubspot.engagement_note n join deal_notes d on
# n.engagement_id=d.engagement_id")

nexudus_spaces<-dbGetQuery(edp_con,"select
id,
name,
floorplanid ,
floorplanname ,
floorplanbusinessname ,
size,
capacity ,
coworkerid,
coworkercontractids ,
coworkercontractstartdates,
createdon,
updatedon 
FROM landing_nexudus.desk_offices
where is_deleted ='NO';")

nexudus_contracts<-dbGetQuery(edp_con,"with coworker_contract as
(
select coworkerid,
tariffid ,
tariffname,
nexttariffname ,
tariffprice ,
createdon,
startdate ,
renewaldate ,
cancellationdate ,
invoicedperiod ,
contractterm ,
active ,
cancelled ,
floorplandeskids ,
floorplandesknames ,
uniqueid 
from
(
select *,
rank() over (partition by id order by updatedon desc) as pos
from landing_nexudus.coworker_contract) cc
where pos=1
),

contract_payments as 
(
select contractguid,
count(*) as num_periods,
sum(totalamount) as total_billed,
sum(paidamount ) as total_paid from
(
SELECT *,
rank() over (partition by id order by updatedon desc) as pos
FROM landing_nexudus.coworker_invoice
) ci
where pos=1
and contractguid is not null
and contractguid != ''
group by contractguid 
),

customer_types as
(select 
id,
email ,
billingemail ,
custom6 as hana_product,
custom8 as customer_role,
teamid 
 from
(
select 
id,
email,
billingemail ,
custom6,
custom8,
teamid ,
rank() over (partition by id order by updatedon desc) as pos
from landing_nexudus.coworker c
) cs
where pos=1
),

plan_names as (
SELECT id,
businessname,
name as plan_name,
price as plan_price
from (
select *,
rank() over (partition by id order by hdp_created_ts desc) as pos
FROM landing_nexudus.tariffs) tr
where pos = 1
),

teams as (
select id as team_id,
name as team_name
from
(select *,
rank() over (partition by id order by updatedon desc) as pos
from landing_nexudus.team) t
where pos=1
)


select cc.*,
ct.*,
cp.*,
pn.businessname,
pn.plan_name,
pn.plan_price,
t.team_name
from coworker_contract cc
join customer_types ct
on cc.coworkerid=ct.id
left join contract_payments cp
on cc.uniqueid=cp.contractguid
left join plan_names pn
on cc.tariffid=pn.id
left join teams t
on ct.teamid=t.team_id 
order by coworkerid,startdate;")

nexudus_teams<-dbGetQuery(edp_con,"with nexudus_teams as(
select * from
(
select *,
rank() over (partition by id order by updatedon desc) as pos
FROM landing_nexudus.team
) t
 where pos=1
),

hana_businesses as
(
select * from
(
select *,
rank() over (partition by id order by updatedon desc) as pos
FROM landing_nexudus.business b 
) t
 where pos=1
)

select nt.id
,businessid
,nt.name as business_team_name
,payingmemberid
,sharetimepasses
,shareextraservices
,sharebookingcredit
,discountextraservices
,coworkerids
,activecontracts
,b.name as hana_location
from nexudus_teams nt
inner join hana_businesses b
on nt.businessid=b.id ")

nexudus_suite_assignments<-dbGetQuery(edp_con,
"select
id,
name,
floorplanid ,
floorplanname ,
floorplanbusinessname ,
size,
capacity ,
coworkerid,
coworkercontractids ,
coworkercontractstartdates,
createdon,
updatedon 
FROM landing_nexudus.desk_offices
where is_deleted ='NO';")


# get web data
# ga_visits_and_conversions<- dbGetQuery(ft_con,
# "with landing_page as(
# select concat(page_hostname,page_page_path) as landing_page,
# visit_id,
# visitor_id
# from google_analytics_360.session_hit sh 
# where hit_number =1
# ),
# 
# lead_forms_viewed as
# (
# select distinct visitor_id,
# visit_id,
# 1 as lead_form_viewed
# FROM google_analytics_360.session_hit
# where event_info_category='lead form'
# and event_info_action='lead form viewed'
# ),
# 
# lead_forms_submitted as
# (
# select distinct visitor_id,
# visit_id,
# 1 as lead_form_submitted
# FROM google_analytics_360.session_hit
# where event_info_category='lead form'
# and event_info_action='lead form confirmation viewed'
# )
# 
# SELECT visit_start_time::date,
# sess.visitor_id,
# sess.visit_id,
# concat(sess.visit_id,sess.visitor_id) as uniq_id,
# traffic_source_medium as medium,
# traffic_source_source as source,
# traffic_source_campaign as campaign,
# geo_network_continent as continent,
# lp.landing_page,
# coalesce(lfv.lead_form_viewed,0) as lead_form_viewed,
# coalesce(sbm.lead_form_submitted,0) as lead_form_submitted
# --count (distinct concat(sess.visit_id,sess.visitor_id)) as visits,
# --count (distinct lfv.uniq_id) as lead_forms_viewed,
# --count (distinct sbm.uniq_id) as lead_forms_submitted
# FROM google_analytics_360.ga_session sess
# inner join landing_page lp
# on sess.visit_id =lp.visit_id
# and sess.visitor_id =lp.visitor_id
# left join lead_forms_viewed lfv
# on sess.visit_id=lfv.visit_id
# and sess.visitor_id =lfv.visitor_id
# left join lead_forms_submitted sbm
# on sess.visit_id=sbm.visit_id
# and sess.visitor_id=sbm.visitor_id")
# 
# ga_top_content<-dbGetQuery(ft_con,
# "with landing_page as(
# select concat(page_hostname,page_page_path) as landing_page,
# visit_id,
# visitor_id
# from google_analytics_360.session_hit sh 
# where hit_number =1
# )
# 
# SELECT ss.visit_start_time::date,
# ss.traffic_source_medium as medium,
# ss.traffic_source_source as source,
# ss.traffic_source_campaign as campaign, 
# sh.page_page_path,
# sh.page_hostname,
# ss.visit_id,
# ss.visitor_id,
# concat(ss.visitor_id,'.',ss.visit_id) as uniq_id,
# sh.hit_number,
# lp.landing_page
# FROM google_analytics_360.session_hit sh
# join google_analytics_360.ga_session ss
# on sh.visit_id=ss.visit_id
# and sh.visitor_id =ss.visitor_id 
# left join landing_page lp
# on ss.visit_id = lp.visit_id
# and ss.visitor_id =lp.visitor_id
# where sh.type='PAGE'
# and ss.visit_start_time>'2019-07-01'::date;
# ")    
#                                        
# ga_converters<-dbGetQuery(ft_con,
# "SELECT visitor_id,
# visit_id,
# concat(visitor_id,'.',visit_id) as uniq_id,
# visit_start_time::date
# FROM google_analytics_360.session_hit
# where event_info_category='lead form'
# and event_info_action='lead form confirmation viewed'")



  
# zoom_meetings<-dbGetQuery(ft_con,"select host,
# start_time ,
# duration_hh_mm_ss_ ,
# participants,
# video ,
# screen_sharing from zoom.zoom_meetings zm
# where \"group\" != 'Hana Corp Users'
# ")


#lets only run the swipe query once a week for now

if (wday(today(),label = TRUE)=='XXX')
{
       
feenics_events<-dbGetQuery(edp_con,"with linked_objects as 
(
select * from 
(select eol.event_id,
eol.linked_object_id,
eol.common_name,
eol.relation,
rank() over (partition by eol.event_id ,eol.linked_object_id,eol.relation order by eol.hdp_created_ts desc) as pos
 from landing_feenics.events e 
 join landing_feenics.events_object_link eol 
 on e.event_id =eol.event_id) eol
 where pos=1 and relation  in ('Person','Reader')
 ),

 feenics_emails as
 (
 select * from (
select people_id,
mail_to as member_email,
rank() over (partition by a.address_id order by a.hdp_created_ts desc) as pos
from landing_feenics.address a 
) a2
where pos=1
)

select ev.event_id ,
ev.message_short,
ev.published_on ,
ev.occurred_on,
pl.common_name as member_name,
fe.member_email,
lo2.common_name as reader_name
from landing_feenics.events ev
join linked_objects lo 
on ev.event_id =lo.event_id 
and lo.relation ='Person'
join linked_objects lo2 
on ev.event_id =lo2.event_id 
and lo2.relation ='Reader'
join landing_feenics.people pl
on lo.linked_object_id =pl.people_id 
join feenics_emails fe
on pl.people_id =fe.people_id 
where ev.message_short  in ('Access Granted','Access denied')
")
}

hs_deleted_deals<-dbGetQuery(ft_con,
'SELECT object_id as deal_id
FROM webhooks.hubspot_deal_deletions
where portal_id =5071399
union 
SELECT deal_id
FROM webhooks."HubspotDeletedDealsPreWebhook"')

sfdc_opptys<-dbGetQuery(seg_con,
                        "with first_contact_create as(
SELECT acr.account_id ,
min(c.created_date ) as first_contact_create
FROM salesforce_prod.account_contact_relationships acr
join salesforce_prod.contacts c 
on acr.contact_id =c.id 
where acr.is_deleted is false
group by acr.account_id 
),

consolidated_accts as(
	select id,
	name,
	created_date
	from salesforce_prod.accounts a 
	union
	select id,
	name,
	created_date 
	from salesforce_prod.landlord_company lc 
)


SELECT o.id,
rt.name as oppty_type,
--o.account_id,
b.name as location_name,
b.building_name_c as location_id,
b.city_c ,
b.sub_region_c ,
b.region_c ,
a.name as customer_name,
a.created_date as customer_create_date,
fcc.first_contact_create,
o.date_needed_lease_expiration_c::date as anticipated_start_date,
o.stage_name ,
o.close_date as anticipated_close_date,
o.created_date ,
o.name,
--o.building_c ,
--o.sub_region_c ,
--o.city_c ,
o.enterprise_notes_c ,
o.additional_notes_text_c ,
--o.region_c ,
o.opportunity_type_c ,
o.hana_fill_product_c,
o.desks_max_c ,
o.desks_min_c,
o.term_length_max_c,
o.term_length_min_c,
o.term_length_period_c,
o.price_range_min_c,
o.price_range_max_c,
o.budget_min_c,
o.budget_max_c,
o.total_deal_amount_c,
o.enterprise_portfolio_c,
o.executive_spotlight_c,
o.executive_summary_c,
o.closed_lost_reasons_c,
concat_ws(';', o.negotiation_deal_terms_c, o.lead_quality_c,o.client_interest_c, o.product_alignment_c) as closed_lost_reason,
o.local_fill_closed_lost_reasons_c,
o.lead_source ,
o.data_source_1_c,
o.data_source_2_c ,
o.data_source_3_c ,
o.digital_tracking_code_c ,
u.name as owner_name,
u.username ,
r.name as sales_role,
u2.name as creator,
rt.name as record_type
FROM salesforce_prod.opportunities o
join salesforce_prod.record_types rt
on o.record_type_id =rt.id 
--and rt.name in ('HANA Enterprise', 'Broker-Led')
and rt.sobject_type = 'Opportunity'
left join consolidated_accts a
on o.account_id = a.id 
left join first_contact_create fcc
on o.account_id =fcc.account_id
join salesforce_prod.users u
on o.owner_id =u.id 
join salesforce_prod.role r 
on u.user_role_id =r.id
left join salesforce_prod.buildings b
on o.building_fill_c =b.id 
join salesforce_prod.users u2 
on o.created_by_id =u2.id
where o.is_deleted = false 
and  rt.name != 'HANA Find';")

sfdc_deal_history<-dbGetQuery(seg_con,"SELECT oh.opportunity_id, 
oh.created_date,
oh.stage_name 
FROM salesforce_prod.opportunity_history oh
join salesforce_prod.opportunities op
on oh.opportunity_id=op.id
join salesforce_prod.record_types rt
on op.record_type_id =rt.id 
and rt.sobject_type ='Opportunity'
and rt.name in ('Hana Local Fill','Broker-Led', 'HANA Enterprise') 
where oh.is_deleted is false 
order by oh.opportunity_id ,
oh.created_date;")

sfdc_leads<-dbGetQuery(seg_con,"SELECT l.id,
l.first_name ,
l.last_name,
l.company ,
l.contact_company_c ,
l.is_converted ,
l.created_date ,
l.converted_date ,
l.converted_contact_id ,
l.converted_account_id ,
l.converted_opportunity_id ,
l.hana_fill_product_c ,
l.location_c ,
l.lead_source ,
l.source_c ,
l.data_source_1_c ,
l.data_source_2_c ,
l.data_source_3_c ,
l.digital_tracking_code_c,
u.name as creator
FROM salesforce_prod.leads l
join salesforce_prod.record_types rt 
on l.record_type_id =rt.id 
and rt.sobject_type ='Lead'
and rt.name ='Hana Fill'
join salesforce_prod.users u 
on l.created_by_id =u.id
where l.is_deleted is false;")

sfdc_activity<-dbGetQuery(seg_con,"SELECT t.task_subtype as subtype,
t.subject ,
t.created_date ,
t.activity_date ,
t.who_id as contact_id,
t.what_id as opportunity_id,
u.alias ,
u.name as creator,
'task' as type
FROM salesforce_prod.tasks t
join salesforce_prod.users u 
on t.created_by_id =u.id
join salesforce_prod.opportunities o 
on t.what_id =o.id 
join salesforce_prod.record_types rt
on o.record_type_id =rt.id 
and  rt.name != 'HANA Find'

UNION

select e.event_subtype as subtype,
e.subject ,
e.created_date ,
e.activity_date, 
e.who_id as contact_id,
e.what_id as opportunity_id,
u.alias ,
u.name as creator,
'event' as type
from salesforce_prod.events e 
join salesforce_prod.users u 
on e.created_by_id =u.id
join salesforce_prod.opportunities o 
on e.what_id =o.id 
join salesforce_prod.record_types rt
on o.record_type_id =rt.id 
and  rt.name != 'HANA Find';")

sfdc_oppty_roles<-dbGetQuery(seg_con,"SELECT c.name ,
lc.name as account,
ocr.role,
ocr.contact_id ,
ocr.opportunity_id 
FROM salesforce_prod.opportunity_contact_role ocr
join salesforce_prod.opportunities o 
on ocr.opportunity_id =o.id 
join salesforce_prod.record_types rt
on o.record_type_id =rt.id 
and  rt.name != 'HANA Find'
join salesforce_prod.contacts c 
on ocr.contact_id =c.id
left join salesforce_prod.landlord_company lc 
on c.account_id =lc.id ")

sfdc_tours<-dbGetQuery(seg_con,"SELECT awt.id,
awt.building_c as what_id,
b.name as hana_unit,
awt.contact_c  as who_id,
c.name as contact,
awt.name as tour_type,
awt.created_date,
awt.date_toured_c as activity_date,
'Awareness Tours' as source
--awt.tour_status_c 
--awt.landlord_company_c 
FROM salesforce_prod.awareness_tours awt
left join salesforce_prod.buildings b 
on awt.building_c =b.id 
left join salesforce_prod.contacts c 
on awt.contact_c =c.id 
where awt.is_deleted is false 

union

SELECT ev.id,
ev.what_id,
b.name as hana_unit,
ev.who_id,
c.name as contact,
ev.subject as tour_type,
ev.created_date,
ev.activity_date ,
'Events' as source
FROM salesforce_prod.events ev
left join salesforce_prod.opportunities o 
on ev.what_id =o.id
left join salesforce_prod.buildings b 
on o.building_fill_c=b.id 
left join salesforce_prod.contacts c 
on ev.who_id =c.id 
where ev.subject similar to '%Tour%'
or ev.description like '%tour%'
and ev.is_deleted is false 

")

sfdc_unit_geo_heirarchy<-dbGetQuery(seg_con,"select distinct building_name_c as location_id,
name as unit_name,
neighborhood_name_c ,
submarket_temp_c ,
city_c ,
state_province_c ,
sub_region_c ,
country_c, 
region_c 
from salesforce_prod.buildings b;")


my_query<-glue_sql("SELECT opf.parent_id as opportunity_id,
opf.created_date ,
opf.created_by_id ,
opf.body 
FROM salesforce_prod.opportunity_feed opf
join salesforce_prod.opportunities o 
on opf.parent_id =o.id 
join salesforce_prod.record_types rt 
on o.record_type_id =rt.id 
and rt.sobject_type ='Opportunity'
and rt.name in ('Broker-Led','Hana Local Fill','HANA Enterprise')
where opf.type = 'TextPost'
and opf.is_deleted is false;", .con = seg_con)

sfdc_oppty_chatter<-dbGetQuery(seg_con,my_query)


my_query<-glue_sql('SELECT value as ip_address
FROM "Internal_Identifiers"."IP_addresses";'
                   , .con = ft_con)


internal_ips<-dbGetQuery(ft_con,my_query)


my_query<-glue_sql('select id,
original_timestamp ,
context_ip ,
context_campaign_medium ,
context_campaign_source ,
context_campaign_name ,
referrer,
path,
url,
user_id,
title,
anonymous_id,
context_user_agent
from yourhana_com_production.pages p 
',.con=seg_con)


segment_pageviews<-dbGetQuery(seg_con,my_query)

my_query<-glue_sql('select gcl_id ,
campaign_name as adwords_campaign_name
from adwords.click_performance cp',.con=ft_con)

adwords_autotagged_campaigns<-dbGetQuery(ft_con,my_query) 

my_query<-glue_sql('SELECT id, original_timestamp, context_page_path, context_page_url, anonymous_id, digital_tracking_code, form_name, event
FROM yourhana_com_production.lead_form_viewed lfv

UNION

SELECT id, original_timestamp, context_page_path, context_page_url, anonymous_id, digital_tracking_code, form_name, event
FROM yourhana_com_production.lead_form_submitted lfs 
',.con=seg_con)

segment_forms<-dbGetQuery(seg_con,my_query)

hs_sfdc_synced_opptys<-dbGetQuery(ft_con,'SELECT salesforce_opportunity_id, 
hubspot_opportunity_id
FROM 
(
select *,
rank() over (partition by hid.salesforce_opportunity_id order by hid."_fivetran_synced" desc) as pos
from "HS_SFDC"."HS_Imported_Deals" hid
) hid
where pos=1;')
                      
dfs<-Filter(function(x) is.data.frame(base::get(x)) , ls())

save(list=dfs,file="~/OneDrive - CBRE, Inc/data/raw_data/ELT_raw_data.RData")

