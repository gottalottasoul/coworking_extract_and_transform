# ./transform
# This file takes the raw data loads and transforms them to a base set of master dataframes
# Blake Abbenante
# 10/1/19

####functions######
Mode <- function(x) {
  ux <- unique(na.omit(x))
  ux[which.max(tabulate(match(x, ux)))]
}



#########Transform data for reporting############################
#load("~/OneDrive - CBRE, Inc/data/raw_data/ELT_raw_data.RData")


jira_ips<-segment_pageviews%>% 
  filter(grepl('cbremb.atlassian.net|applause.com|qa-space|dev-space|hana_qa|-qa|ads.google|storage.googleapi|utest.com',referrer,ignore.case = TRUE) | grepl('qa',path,ignore.case=TRUE) | grepl('localhost',url,ignore.case=TRUE))  %>% 
  select(context_ip) %>% 
  unique(.)

my_ips<-segment_pageviews %>%
  group_by(context_ip) %>%
  summarise(pv=n()) %>%
  arrange(desc(pv)) %>% 
  slice(1:100)

my_result<-rgeolocate::ip_info(my_ips$context_ip)

top100_filter<-my_ips %>% 
  cbind(my_result) %>% 
  filter(grepl('cbre|amazon',org,ignore.case = TRUE)) %>% 
  select(context_ip)

neil_ips<-as_tibble_col(c("76.93.145.39","100.17.2.226"),column_name="context_ip")


ua_list<-segment_pageviews %>% 
  select(context_user_agent) %>% 
  unique() %>% 
  as_vector() %>% 
  uaparserjs::ua_parse()


segment_pageviews_edited<-segment_pageviews %>% 
  filter(!grepl('404',title,ignore.case = TRUE)) %>% 
  anti_join(internal_ips,by=c("context_ip"="ip_address")) %>% 
  anti_join(jira_ips) %>% 
  anti_join(top100_filter) %>% 
  anti_join(neil_ips) %>% 
  left_join(ua_list,by=c("context_user_agent"="userAgent")) %>% 
  filter(!grepl('bot|crawl|spider',ua.family,ignore.case = TRUE),
         !grepl('Google-Ads-Overview',context_user_agent)) %>% 
  separate(url,sep="gclid=",into=c("url","gcl_id"),remove=FALSE) %>% 
  left_join(adwords_autotagged_campaigns) %>%
  mutate(seg_date=lubridate::as_date(original_timestamp),
         host=ifelse(!is.na(referrer),urltools::url_parse(referrer)$domain,"none"),
         campaign_medium_param=urltools::param_get(url)$utm_medium,
         campaign_source_param=urltools::param_get(url)$utm_source,
         campaign_name_param=urltools::param_get(url)$utm_campaign,
         adwords_medium=ifelse(!is.na(gcl_id),"cpc",NA_character_),
         adwords_source=ifelse(!is.na(gcl_id),"google",NA_character_),
         adwords_campaign=ifelse(!is.na(gcl_id),"not captured",NA_character_),
         campaign_name=coalesce(context_campaign_name,campaign_name_param,adwords_campaign),
         campaign_medium=coalesce(context_campaign_medium,campaign_medium_param,adwords_medium),
         campaign_source=coalesce(context_campaign_source,campaign_source_param,adwords_source)) %>% 
  arrange(original_timestamp) %>% 
  group_by(anonymous_id,seg_date) %>% 
  fill(campaign_medium,campaign_source,campaign_name,.direction="downup") %>% 
  mutate(campaign_medium=case_when(
    is.na(campaign_medium) & grepl('yahoo|bing|duckduckgo|google',host,ignore.case = TRUE)~'organic',   
    TRUE~campaign_medium)) %>% 
  fill(campaign_medium,campaign_source,campaign_name,.direction="downup") %>% 
  mutate(path=ifelse(grepl('404',title),'404',path), 
         campaign_medium=case_when(
           grepl('paid|ppc|cpc',campaign_medium,ignore.case = TRUE)~'paid',
           campaign_medium=='organic'~'organic',
           #           !is.na(campaign_medium)~campaign_medium,
           !is.na(campaign_medium)~'other',
           is.na(campaign_medium) & host=='none'~'direct',
           is.na(campaign_medium) &  referrer=="https://www.yourhana.com/"~'direct',
           is.na(campaign_medium) &  grepl('cbre',host,ignore.case = TRUE)~'referral (internal)',
           is.na(campaign_medium) &  grepl('yourhana',host,ignore.case = TRUE)~NA_character_,
           is.na(campaign_medium)~'referral',
           TRUE~'other')) %>% 
  arrange(original_timestamp) %>% 
  group_by(anonymous_id) %>% 
  fill(campaign_medium,.direction="down") %>% 
  group_by(anonymous_id,seg_date) %>% 
  fill(campaign_medium,.direction="up") %>% 
  mutate(landing_content=first(url)) %>% 
  replace_na(list(campaign_medium="unknown")) %>% 
  select(id,anonymous_id,seg_date,original_timestamp,host,path,url,campaign_medium,campaign_source,campaign_name,landing_content,ua.family,device.family,device.brand,device.model,os.family,os.major)

web_dau_all<-segment_pageviews_edited %>% 
  mutate(hana_city= case_when(
    grepl('dallas|park-district',path,ignore.case = TRUE)~'Dallas',
    grepl('irvine|park-place|orange_county',path,ignore.case = TRUE)~'Irvine',
    grepl('london|hammersmith|st-mary|st-martin',path,ignore.case = TRUE)~'London',
    grepl('manchester',path,ignore.case = TRUE)~'Manchester',    
    grepl('arlington',path,ignore.case = TRUE)~'Arlington',
    grepl('new-york',path,ignore.case = TRUE)~'New York',
    grepl('philadelphia',path,ignore.case = TRUE)~'Philadelphia',        
    TRUE~NA_character_
  ),
  location_id= case_when(
    grepl('dallas|park-district',path,ignore.case = TRUE)~'UnitedStates_Texas_Dallas_PwCToweratParkDistrict_2121NorthPearlStreet',
    grepl('irvine|park-place|orange_county',path,ignore.case = TRUE)~'UnitedStates_California_Irvine_IrvineParkPlace_3349MichelsonDrive',
    grepl('hammersmith',path,ignore.case = TRUE)~'UnitedKingdom_London_245Hammersmith_245HammersmithRoad',
    grepl('st-mary',path,ignore.case = TRUE)~'UnitedKingdom_London_70SMA_70StMarysAxe',
    grepl('st-martin',path,ignore.case = TRUE)~'UnitedKingdom_London_SMC_10PaternosterRow',
    grepl('manchester',path,ignore.case = TRUE)~'UnitedKingdom_Manchester_OneWindmillGreen_24MountStreet',    
    grepl('arlington',path,ignore.case = TRUE)~'UnitedStates_Virginia_Arlington_2451CrystalDrive_2451CrystalDrive',
    grepl('new-york',path,ignore.case = TRUE)~'UnitedStates_NewYork_NewYorkCity_3WTC_175GreenwichSt',
    grepl('philadelphia',path,ignore.case = TRUE)~'UnitedStates_Pennsylvania_Philadelphia_1818MarketSt_1818MarketSt',        
    TRUE~NA_character_
  ),
  hana_product=case_when(
    grepl('team',campaign_name,ignore.case = TRUE) | grepl('team',path,ignore.case=TRUE)~'Team',
    grepl('share',campaign_name,ignore.case = TRUE) | grepl('share',path,ignore.case = TRUE)~'Share',
    grepl('meet',campaign_name,ignore.case=TRUE) | grepl('meet',path,ignore.case = TRUE)~'Meet',
    TRUE~NA_character_
  )) %>% 
  group_by(seg_date,anonymous_id,landing_content) %>% 
  summarise(campaign_medium=Mode(campaign_medium),
            campaign_source=Mode(campaign_source),
            campaign_name=Mode(campaign_name),
#            landing_content=first(url),
            hana_city=Mode(hana_city),
            hana_unit=Mode(location_id),
            hana_product_orig=Mode(hana_product),
            true_paid=ifelse(campaign_medium=='paid',1,0))  %>% 
  mutate(dau_day=wday(seg_date,week_start = 1),
         week_start=floor_date(seg_date, unit="week",week_start = 1),
         month_start=floor_date(seg_date, unit="month"),
         quarter_start=floor_date(seg_date, unit="quarter")) %>% 
  dplyr::rename(dau_date=seg_date) %>% 
  ungroup(.)

segment_form_conversions<-web_dau_all %>% 
  select(dau_date,anonymous_id) %>% 
  inner_join(segment_forms %>% 
               mutate(dau_date=lubridate::as_date(original_timestamp)))


# web_data_all<-ga_visits_and_conversions %>% 
#   mutate(channel= case_when(
#     medium == 'cpc' & grepl('(google|bing)',source,ignore.case = TRUE) ~ 'Paid - Search',
#     medium == 'cpc' ~ 'Paid - Social',
#     medium == 'organic' ~ 'Organic',
#     medium == '(none)' ~ 'Direct',
#     TRUE ~ 'Other'),
#     landing_content=case_when(
#       grepl('^blog',landing_page,ignore.case = TRUE)~'Blog',
#       grepl('^find|/lp/',landing_page,ignore.case=TRUE)~'Landing Page',
#       grepl('^www',landing_page,ignore.case = TRUE)~'WWW',
#       grepl('member',landing_page,ignore.case = TRUE)~'Member Site',
#       TRUE~'Other'),
#     visit_day=wday(visit_start_time,week_start = 1),
#     week_start=floor_date(visit_start_time, unit="week",week_start = 1),
#     month_start=floor_date(visit_start_time, unit="month"),
#     quarter_start=floor_date(visit_start_time, unit="quarter")) %>% 
#   group_by(landing_content,channel,medium,source,campaign,continent,visit_start_time,visit_day,week_start,month_start,quarter_start) %>% 
#   summarise(visits=n(),
#             lead_forms_viewed=sum(lead_form_viewed),
#             lead_forms_submitted=sum(lead_form_submitted)) %>% 
#   ungroup(.)


# web_content_all<-ga_top_content %>% 
#   mutate(channel= case_when(
#     medium == 'cpc' & grepl('(google|bing)',source,ignore.case = TRUE) ~ 'Paid - Search',
#     medium == 'cpc' ~ 'Paid - Social',
#     medium == 'organic' ~ 'Organic',
#     medium == '(none)' ~ 'Direct',
#     TRUE ~ 'Other'),
#     landing_content=case_when(
#       grepl('^blog',landing_page,ignore.case = TRUE)~'Blog',
#       grepl('^find|/lp/',landing_page,ignore.case=TRUE)~'Landing Page',
#       grepl('^www',landing_page,ignore.case = TRUE)~'WWW',
#       grepl('member',landing_page,ignore.case = TRUE)~'Member Site',
#       TRUE~'Other'),
#   ) %>% 
#   select(channel,landing_content,visit_start_time,uniq_id,visit_id,visitor_id,page_hostname,page_page_path,hit_number) %>% 
#   rename(page=page_page_path,
#          host=page_hostname) %>% 
#   ungroup(.)



marketing_spend_edited<-marketing_spend %>% 
  mutate_if(is.numeric,~replace(.,is.na(.),0)) %>% 
  mutate(hana_product = case_when(
    grepl('Team',campaign_name,ignore.case = TRUE) ~ 'Team',
    grepl('Meet', campaign_name,ignore.case = TRUE) ~ 'Meet',
    grepl('Share', campaign_name,ignore.case = TRUE) ~ 'Share',
    TRUE ~ 'Non-Specific'),
    hana_market = case_when(
      grepl('dallas',campaign_name,ignore.case = TRUE) ~ 'Dallas',
      grepl('irvine',campaign_name,ignore.case = TRUE) ~ 'Irvine',
      grepl('uk-london',campaign_name,ignore.case = TRUE)~'London',
      TRUE ~ 'Non-Specific'),
    spend_week=floor_date(campaign_date, unit="week",week_start = 1),
    spend_month=floor_date(campaign_date, unit="month"),
    Team=case_when(
      hana_product=='Team'~spend,
      hana_product=='Meet'~0,
      hana_product=='Share'~0,
      hana_product=='Non-Specific'~spend*.4),
    Share=case_when(
      hana_product=='Team'~0,
      hana_product=='Meet'~0,
      hana_product=='Share'~spend,
      hana_product=='Non-Specific'~spend*.4),
    Meet=case_when(
      hana_product=='Team'~0,
      hana_product=='Meet'~spend,
      hana_product=='Share'~0,
      hana_product=='Non-Specific'~spend*.2)
    )  
  

scheduled_calls_by_contact_email<-hs_tours_and_calls %>% 
  filter(meeting_type=='Call') %>% 
  group_by(property_email) %>% 
  summarise(total_scheduled_calls=n(),
            most_recent_scheduled_call=as_date(max(tour_time)),
            first_created_scheduled_call=as_date(min(create_time))) 

scheduled_tours_by_contact_email<-hs_tours_and_calls %>% 
  filter(meeting_type=='Tour') %>% 
  group_by(property_email) %>% 
  summarise(total_scheduled_tours=n(),
            most_recent_scheduled_tour=as_date(max(tour_time)),
            first_created_scheduled_tour=as_date(min(create_time))) 

engagements_by_contact<-hs_contact_engagements %>% 
  rename(engagement_type=type,
         id=contact_id) %>% 
  mutate(engagements=as.numeric(engagements)) %>% 
  spread(engagement_type,engagements,fill=0)

emails_by_contact<-hs_email_log %>% 
  spread(type,min,fill=NA) %>% 
  rename(id=contact_id)

calls_by_contact<-hs_phone_call_log %>% 
  mutate(disposition=case_when(
    disposition == "9d9162e7-6cf3-4944-bf63-4dff82258764" ~ "Busy",
    disposition == "f240bbac-87c9-4f6e-bf70-924b57d47db7" ~ "Connected",
    disposition =="a4c4c377-d246-4b32-a13b-75a56a4cd0ff" ~ "Left live message",
    disposition =="b2cf5968-551e-4856-9783-52b3da59a7d0" ~ "Left voicemail",
    disposition =="73a0d17f-1163-4015-bdd5-ec830791da20" ~ "No answer",
    disposition =="17b47fee-58de-441e-a44c-c6300d46f273" ~ "Wrong number",
    TRUE~ 'Not Specified'
  )) %>% 
  select(id=contact_id,disposition,created_at) %>% 
  group_by(id) %>% 
  summarise(first_outbound_call=min(created_at),
            first_call_connected=min(created_at[disposition=='Connected']))

hs_contacts_edited<-hs_contacts %>% 
  mutate(create_date=as_date(property_createdate)) %>% 
  filter(!grepl(('TEST'),property_lastname,ignore.case = TRUE),
         !grepl(('TEST'),property_firstname,ignore.case = TRUE),
         !grepl('hana',property_company, ignore.case=TRUE),
         !property_lifecyclestage %in% c('subscriber','other')) %>% 
  left_join(hs_companies %>% select(id,property_name),by=c("property_associatedcompanyid"="id")) %>% 
  mutate(create_week=floor_date(create_date, unit="week",week_start = 1),
         create_month=floor_date(create_date, unit="month"),
         create_quarter=floor_date(create_date, unit="quarter"),
         create_wday=wday(create_date,week_start = 1),
         acquisition_channel=case_when(
           property_offline_source_drill_down_1=='3rd Party Aggregator' | !grepl('offline',property_hs_analytics_source,ignore.case = TRUE) ~ 'Marketing',
           property_hs_analytics_source_data_1=='SALESFORCE' | property_hs_analytics_source=='OFFLINE' ~ 'Sales',
           grepl('broker',property_hs_analytics_source_data_1,ignore.case = TRUE) | property_hs_all_team_ids==287864 ~ 'Broker',
           grepl('enterprise',property_hs_analytics_source_data_1,ignore.case = TRUE) ~ 'Enterprise',
           TRUE ~ 'Local'),
         digital_channel=case_when(
           property_hs_analytics_source == 'DIRECT_TRAFFIC' ~ 'Direct',
           property_hs_analytics_source == 'ORGANIC_SEARCH' ~ 'Organic',
           property_hs_analytics_source %in% c('PAID_SEARCH','PAID_SOCIAL') ~ 'Paid',
           property_offline_source_drill_down_1=='3rd Party Aggregator'~ 'Aggregator',
           property_hs_analytics_source == 'OFFLINE' ~ 'N/A',
           TRUE ~ 'Other'),
         acquisition_source=case_when(
           digital_channel=='Paid' & grepl('lead.gen', property_hs_analytics_source_data_2) ~ paste0('Off-platform(',property_hs_analytics_source_data_1,')'),
           digital_channel=='Direct' & grepl('yourhana.com/$', property_hs_analytics_source_data_1,ignore.case=TRUE)~'Direct(homepage)',
           digital_channel=='Direct' & grepl('blog',property_hs_analytics_source_data_1,ignore.case = TRUE)~'Direct(blog)',
           digital_channel=='Direct' & grepl('yourhana.com',property_hs_analytics_source_data_1,ignore.case = TRUE)~'Direct(website)',
           digital_channel=='Direct' & grepl('meetings.hubspot',property_hs_analytics_source_data_1,ignore.case = TRUE)~'Direct(meeting link)',
           digital_channel=='Direct'~'Direct(other)',
           digital_channel=='Organic'~ tolower(property_hs_analytics_source_data_2),
           digital_channel=='Other' & property_hs_analytics_source=='SOCIAL_MEDIA'~paste0('Social(',tolower(property_hs_analytics_source_data_1),')'),
           digital_channel=='Paid' & grepl('facebook',property_hs_analytics_source_data_1,ignore.case = TRUE)~'Paid Social(Facebook)',
           digital_channel=='Paid' & grepl('linkedin',property_hs_analytics_source_data_1,ignore.case = TRUE)~'Paid Social(LinkedIn)',
           digital_channel=='Paid' & grepl('auto-tagged',property_hs_analytics_source_data_1,ignore.case = TRUE)~'Paid Search(Google)',
           digital_channel=='Paid' & grepl('google|adwords',property_hs_analytics_first_referrer,ignore.case = TRUE)~'Paid Search(Google)',
           digital_channel=='Other'~tolower(property_hs_analytics_source),
           digital_channel=='N/A'~tolower(property_offline_source_drill_down_1),
           TRUE ~ 'Other'),
         first_page=str_remove(property_hs_analytics_first_url,"\\?.*$"),
         budget=coalesce(property_budget,property_amount),
         desired_move_in_date=coalesce(property_start_date,property_move_in_date),
         hana_location=coalesce(property_in_which_location_are_you_interested_,"Not Specified"),
         hana_product=coalesce(property_in_which_service_are_you_interested_,property_what_type_of_workspace_are_you_interested_in_,"Not Specified"),
         company=coalesce(property_name,property_company)) %>% 
  left_join(scheduled_calls_by_contact_email) %>%
  left_join(scheduled_tours_by_contact_email) %>% 
  left_join(emails_by_contact) %>% 
  left_join(calls_by_contact) %>% 
  select(contact_id=id,
         hana_location,
         first_name=property_firstname,
         last_name=property_lastname,
         contact_email=property_email,
         #         company=property_company,
         company,
         company_id=property_associatedcompanyid,
         hana_product,
         lifecycle_stage=property_lifecyclestage,
         create_date,
         create_week,
         create_month,
         create_quarter,
         create_wday,
         lead_date=property_hs_lifecyclestage_lead_date,
         mql_date=property_hs_lifecyclestage_marketingqualifiedlead_date,
         sql_date=property_hs_lifecyclestage_salesqualifiedlead_date,
         oppty_date=property_hs_lifecyclestage_opportunity_date,
         customer_date=property_hs_lifecyclestage_customer_date,
         disqualify_reason=property_disqualified_reason,
         budget,
         total_scheduled_calls,
         most_recent_scheduled_call,
         first_created_scheduled_call,
         total_scheduled_tours,
         most_recent_scheduled_tour,
         first_created_scheduled_tour,
         first_outbound_email=EMAIL,
         first_email_response=INCOMING_EMAIL,
         first_outbound_call,
         first_call_connected,
         desired_move_in_date,
         tracking_code=property_digital_tracking_code,
         digital_channel,
         acquisition_channel,
         acquisition_source,
         first_page,
         isr_review=property_isr_review,
         salesforce_lead_id=property_salesforceleadid,
         salesforce_contact_id=property_salesforcecontactid) %>% 
  arrange(desc(create_week)) %>% 
  mutate(scheduled_meeting=ifelse(is.na(first_created_scheduled_tour)+is.na(first_created_scheduled_call)==2,0,1),
         engaged_contact=ifelse(is.na(first_email_response)+is.na(first_call_connected)<2,1,0),
         first_outreach=coalesce(first_outbound_email,now()),
         no_outreach=ifelse(is.na(first_outbound_call)+is.na(first_outbound_email)==2,1,0),
         time_to_first_contact=round(as.numeric(difftime(first_outreach, lead_date,units="hours")),1))  


  sfdc_deal_history_edited<-hs_deal_history %>% 
    inner_join(hs_sfdc_synced_opptys,by=c("deal_id"="hubspot_opportunity_id")) %>% 
    mutate(source="HS") %>% 
    select(opportunity_id=salesforce_opportunity_id,created_date=date_created,pipeline_stage_detail=pipeline_stage,source) %>% 
  rbind(sfdc_deal_history %>% 
          rename(pipeline_stage_detail=stage_name) %>% 
          mutate(source="SFDC")) %>% 
  arrange(created_date) %>% 
  group_by(opportunity_id) %>% 
  filter(pipeline_stage_detail!= lag(pipeline_stage_detail, default="1")) %>% 
    mutate(pipeline_stage=case_when(
      pipeline_stage_detail=='Cold/Dead' ~ 'Closed Lost',
      pipeline_stage_detail=='Contract Negotiation'~'Negotiation/Proposal',
      pipeline_stage_detail=='Tour Scheduled' ~ 'Touring',
      grepl('(Qualified - Local Fill Opportunity|Touring|Negotiation/Proposal|Contract Sent|Closed Won|Closed Lost)',pipeline_stage_detail) ~ pipeline_stage_detail,
      TRUE ~ 'Upper Funnel')) %>% 
    rename(stage_date=created_date)

  hs_sfdc_deal_amounts<-hs_deals%>% 
    inner_join(hs_sfdc_synced_opptys,by=c("deal_id"="hubspot_opportunity_id")) %>% 
    select(salesforce_opportunity_id,hs_monthly_amount=property_amount,hs_close_date=property_closedate)
  
  sfdc_broker_deals<-sfdc_oppty_roles %>% 
    filter(grepl('broker',role,ignore.case = TRUE)) %>% 
    select(opportunity_id) %>% 
    mutate(broker_roles=TRUE) %>% 
    unique(.)
  
  sfdc_deals_contacts<-sfdc_oppty_roles %>% 
    mutate(account = na_if(account, "Unknown"),
      deal_contact=coalesce(account,name)) %>% 
    group_by(opportunity_id) %>% 
    mutate(associated_contacts=paste0(unique(deal_contact), collapse = "; ")) %>% 
    distinct(opportunity_id,associated_contacts)
  
sfdc_deals_edited <- sfdc_opptys %>%
  filter(record_type=='Hana Local Fill') %>% 
  left_join(sfdc_deal_history_edited %>% 
      arrange(desc(stage_date,pipeline_stage)) %>% 
      group_by(opportunity_id) %>% 
      slice(1) %>% 
      ungroup(.) %>% 
      filter(pipeline_stage %in% c('Closed Lost','Closed Won')) %>% 
        mutate(close_date=stage_date) %>% 
      select(id=opportunity_id,close_date)) %>% 
  left_join(hs_sfdc_deal_amounts,by=c("id"="salesforce_opportunity_id")) %>% 
  left_join(sfdc_broker_deals,by=c("id"="opportunity_id")) %>% 
  left_join(sfdc_deals_contacts,by=c("id"="opportunity_id")) %>% 
  mutate(create_date=as_date(created_date),
         create_week=as_date(floor_date(created_date, unit="week",week_start = 1)),
         create_month=as_date(floor_date(created_date,unit="month")),
         create_quarter=as_date(floor_date(created_date,unit="quarter")),
         create_wday=wday(created_date,week_start = 1),
         contact_create_date=as_date(customer_create_date),
         days_since_created=round(as.numeric(difftime(Sys.Date(), create_date,units="days")),1),
         contact_create_week=floor_date(contact_create_date, unit="week",week_start = 1),
#         pipeline_stage=case_when(
#             stage_name=='Cold/Dead' ~ 'Closed Lost',
#             stage_name=='Contract Negotiation'~'Negotiation/Proposal',
#             stage_name=='Tour Scheduled' ~ 'Touring',
#             grepl('(Qualified - Local Fill Opportunity|Touring|Negotiation/Proposal|Contract Sent|Closed Won|Closed Lost)',stage_name) ~ stage_name,
#             TRUE ~ 'Upper Funnel'),
         pipeline_stage=stage_name,
         hana_product=case_when(
           !is.na(hana_fill_product_c)~hana_fill_product_c,
           grepl("enterprise|broker",oppty_type,ignore.case = TRUE)~'Hana Team',
           desks_max_c>1~'Hana Team',
           TRUE~'Hana Share'),
#         notes=paste0(executive_summary_c,enterprise_notes_c,collapse="<br>")),
         close_reason=closed_lost_reasons_c,
         deal_link=paste0("<a href='https://hanasimplify.lightning.force.com/lightning/r/Opportunity/",id,"/view 'target='_blank'>",name,"</a>"),
         hana_location=location_name,
         lead_source=coalesce(lead_source,oppty_type),
         close_date=as_date(coalesce(close_date,anticipated_close_date)),
         acquisition_channel=case_when(
           grepl('enterprise',lead_source,ignore.case = TRUE)~'Enterprise',
           grepl('broker',lead_source,ignore.case = TRUE)~'Broker',
           lead_source=='Online'~'Marketing',
           lead_source=='Hana Local Fill'~'Local Sales',
           data_source_1_c=='3rd Party Aggregator'~'Marketing',
           TRUE~'Local Sales'),
          digital_channel=case_when(
            lead_source=='Online' & data_source_1_c == 'DIRECT_TRAFFIC' ~ 'Direct',
            lead_source=='Online' & data_source_1_c == 'ORGANIC_SEARCH' ~ 'Organic',
            lead_source=='Online' & data_source_1_c %in% c('PAID_SEARCH','PAID_SOCIAL') ~ 'Paid',
            lead_source=='Offline' & data_source_1_c=='3rd Party Aggregator'~ 'Aggregator',
            TRUE ~ NA_character_),
          acquisition_source=case_when(
            digital_channel=='Paid' & grepl('lead.gen', data_source_3_c) ~ paste0('Off-platform(',data_source_2_c,')'),
            digital_channel=='Direct' & grepl('yourhana.com/$', data_source_2_c,ignore.case=TRUE)~'Direct(homepage)',
            digital_channel=='Direct' & grepl('blog',data_source_2_c,ignore.case = TRUE)~'Direct(blog)',
            digital_channel=='Direct' & grepl('yourhana.com',data_source_2_c,ignore.case = TRUE)~'Direct(website)',
            digital_channel=='Direct' & grepl('meetings.hubspot',data_source_2_c,ignore.case = TRUE)~'Direct(meeting link)',
            digital_channel=='Direct'~'Direct(other)',
            digital_channel=='Organic'~ tolower(data_source_2_c),
            digital_channel=='Other' & data_source_1_c=='SOCIAL_MEDIA'~paste0('Social(',tolower(data_source_2_c),')'),
            digital_channel=='Paid' & grepl('facebook',data_source_2_c,ignore.case = TRUE)~'Paid Social(Facebook)',
            digital_channel=='Paid' & grepl('linkedin',data_source_2_c,ignore.case = TRUE)~'Paid Social(LinkedIn)',
            digital_channel=='Paid' & grepl('auto-tagged',data_source_2_c,ignore.case = TRUE)~'Paid Search(Google)',
#            digital_channel=='Paid' & grepl('google|adwords',property_hs_analytics_first_referrer,ignore.case = TRUE)~'Paid Search(Google)',
            digital_channel=='Paid' & data_source_1_c=='PAID_SEARCH'~'Paid Search(Google*)',
            digital_channel=='Other'~tolower(lead_source),
            digital_channel=='N/A'~tolower(data_source_2_c),
            TRUE ~ 'Other'),
         tracking_code=digital_tracking_code_c,
         confidential_client=NA,
#         monthly_amount=coalesce(price_range_max_c,price_range_min_c,budget_max_c,hs_monthly_amount),
         monthly_amount=coalesce(price_range_min_c,budget_min_c,price_range_max_c,budget_max_c,hs_monthly_amount),
         sales_channel=case_when(
           broker_roles==TRUE~'Broker',
           oppty_type=='Broker-Led'~'Broker',
           grepl('broker',oppty_type,ignore.case = TRUE)~'Broker',
           grepl('enterprise',oppty_type,ignore.case = TRUE)~'Enterprise',
           grepl('local',oppty_type,ignore.case = TRUE)~'Local',
           TRUE~'Unspecified'),
         notes=str_remove_all(paste(executive_summary_c,enterprise_notes_c,sep="<br>"),"NA")) %>% 
  select(deal_id=id,
         deal_name=name,
         create_date,
         create_week,
         create_month,
         create_quarter,
         create_wday,
         contact_create_date,
         seats=desks_max_c,
         hana_unit=hana_location,
         location_id,
         hana_city=city_c,
         hana_region=region_c,
         pipeline_stage,
         pipeline_stage_detail=stage_name,
         hana_product,
         monthly_amount,
         total_deal_amount=total_deal_amount_c,
         lease_term=term_length_max_c,
         anticipated_start_date,
         closed_lost_reason,
         acquisition_channel,
         acquisition_source,
         digital_channel,
         tracking_code,
         customer_name,
         associated_contacts,
         owner_name,         
         notes,
         deal_link,         
         close_date,
         contact_create_week,
         sales_channel)


hana_unit_geo_heirarchy<-sfdc_unit_geo_heirarchy %>% 
  mutate(hana_submarket=coalesce(submarket_temp_c,neighborhood_name_c)) %>% 
  select(location_id,
         hana_unit=unit_name,
         hana_submarket,
         hana_city=city_c,
         hana_state=state_province_c,
         hana_country=country_c,
         hana_sub_region=sub_region_c,
         hana_region=region_c) %>% 
  rbind(rep("Not Specified",8))
  

isr_engagement_activity_edited<-isr_engagement_activity %>% 
  mutate(status=case_when(
      status == "9d9162e7-6cf3-4944-bf63-4dff82258764" ~ "Busy",
      status == "f240bbac-87c9-4f6e-bf70-924b57d47db7" ~ "Connected",
      status =="a4c4c377-d246-4b32-a13b-75a56a4cd0ff" ~ "Left live message",
      status =="b2cf5968-551e-4856-9783-52b3da59a7d0" ~ "Left voicemail",
      status =="73a0d17f-1163-4015-bdd5-ec830791da20" ~ "No answer",
      status =="17b47fee-58de-441e-a44c-c6300d46f273" ~ "Wrong number",
      TRUE~ status)) 

isr_task_activity_edited<-isr_task_activity %>% 
  mutate(time_to_complete=round(difftime(completed_date,created_at,units="hours"),1))
      
isr_activity_summary_edited<-
  isr_task_activity_edited %>% 
  group_by(contact_id) %>% 
  summarise(num_tasks=n(),
            completed_tasks=length(task_type[status=='COMPLETED']),
            first_task_create_date=min(created_at),
            first_task_completed_date=min(completed_date),
            time_to_first_completed_task= as.numeric(round(difftime(first_task_completed_date,first_task_create_date,units="hours"),1))) %>% 
  left_join(hs_isr_history %>% 
              arrange(desc(timestamp)) %>% 
              group_by(contact_id) %>% 
              slice(1) %>% 
              mutate(isr_status_date=as_date(timestamp)) %>% 
              select(contact_id,isr_status_date)) %>% 
  left_join(isr_engagement_activity_edited) %>% 
  group_by(contact_id,num_tasks,completed_tasks,first_task_create_date,first_task_completed_date,time_to_first_completed_task,isr_status_date) %>%
  summarise(total_engagements=sum(!is.na(engagement_type)),
            email_engagements=sum(engagement_type=='Email'),
            phone_engagements=sum(engagement_type=='Call'),
            time_to_first_engagement= as.numeric(round(difftime(min(created_at),min(first_task_create_date),units="hours"),1))) %>% 
  ungroup(.) %>% 
  mutate(first_task_create_date=as_date(first_task_create_date),
         first_task_completed_date=as_date(first_task_completed_date)) %>% 
#need to change this in the future to account for deleted contacts
    inner_join(hs_contacts_edited %>% 
              select(contact_id,contact_create_date=create_date,isr_review,lifecycle_stage,hana_location,hana_product)) %>% 
     left_join(RiHana::get_hana_locations() %>% 
                 select(hs_locations,hana_city,hana_name),
                 by=c("hana_location"="hs_locations")) %>% 
  mutate_if(is.character,~replace(.,is.na(.),"Not Specified"))
  



isr_activity_detail_edited<-isr_engagement_activity_edited %>% 
  left_join(hs_contacts_edited %>% select(contact_id,hana_location,hana_product)) %>% 
  filter(engagement_type %in% c("Email","Call")) %>% 
  mutate(engagement_date=as_date(created_at)) %>% 
    group_by(engagement_date,engagement_type,hana_location,hana_product) %>% 
    summarise(engagements=n()) %>% 
  ungroup(.) %>% 
  mutate(hana_location=coalesce(hana_location,"Not Specified"),
         hana_product=coalesce(hana_product,"Not Specified")) %>% 
  left_join(RiHana::get_hana_locations() %>% 
              select(hs_locations,hana_city,hana_name),
            by=c("hana_location"="hs_locations")) %>% 
  mutate_if(is.character,~replace(.,is.na(.),"Not Specified"))


# web_converters<-ga_converters %>% 
#   #  mutate(uniq_id=paste0(visitor_id,".",visit_id)) %>% 
#   group_by(uniq_id) %>% 
#   summarise(conversion_date=min(visit_start_time))


save(hs_contacts_edited,
     sfdc_deals_edited,
     sfdc_deal_history_edited,
     hana_unit_geo_heirarchy,
     isr_activity_summary_edited,
     isr_activity_detail_edited,
     segment_pageviews_edited,
     web_dau_all,
     segment_form_conversions,
#     web_data_all,
#     web_content_all,
#     web_converters,
     marketing_spend_edited,
     file="~/OneDrive - CBRE, Inc/data/raw_data/transform_data.RData")



