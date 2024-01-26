clear;

drop table ora_to_ch_tasks_tables;
drop table ora_to_ch_tasks;
drop sequence s_ora_to_ch_tasks;

create sequence s_ora_to_ch_tasks
minvalue 1
maxvalue 9999999999999999999999999999
start with 1
increment by 1
nocache;

create sequence s_ora_to_ch_views_query_log
minvalue 1
maxvalue 9999999999999999999999999999
start with 1
increment by 1
nocache;

create table ora_to_ch_tasks(
 id              integer primary key,
 ora_sid         integer not null,
 state           varchar2(32) default 'none',-- executing, finished
 begin_datetime  date default sysdate,
 --task_json clob
 end_datetime    date
);

create table ora_to_ch_tasks_tables(
 id_task              integer not null constraint fk_ora_to_ch_tbl_tsk references ora_to_ch_tasks(id) on delete cascade,
 schema_name          varchar2(32) not null,
 table_name           varchar2(32) not null,
 begin_datetime       date,
 end_datetime         date,
 state                varchar2(32) default 'none',-- copying, finished
 copied_records_count integer default 0,
 speed_rows_sec       number,
 constraint uk_ora_to_ch_tasks_tables unique(id_task,schema_name,table_name)
);

create table ora_to_ch_views_query_log(
 id             integer constraint pk_ora_to_ch_views_query_log primary key,
 id_vq          integer not null constraint fk_orach_vq_log_vq references ora_to_ch_views_query(id) on delete cascade,
 ora_sid        integer,
 begin_calc     date,
 end_calc       date,
 begin_copy     date,
 end_copy       date,
 state          varchar2(32) default 'none'
);

create table ora_to_ch_views_query
(
  id         integer not null,
  view_name  varchar2(32),
  ch_table   varchar2(32) not null,
  ora_table  varchar2(32) not null,
  query_text clob
);

alter table ora_to_ch_views_query
  add primary key (id);

create table ora_to_ch_vq_params
(
  id_vq       integer not null,
  param_name  varchar2(32) not null,
  param_type  varchar2(32) not null,
  param_order integer not null
);

alter table ora_to_ch_vq_params
  add constraint fk_otc_params_vq foreign key (id_vq)
  references ora_to_ch_views_query (id);

insert into ORA_TO_CH_VQ_PARAMS (id_vq, param_name, param_type, param_order) values (1, 'date_cache_1', 'Decimal(38,6)', 1);
insert into ORA_TO_CH_VQ_PARAMS (id_vq, param_name, param_type, param_order) values (1, 'datecalc_cache_1', 'Decimal(38,6)', 2);
insert into ORA_TO_CH_VQ_PARAMS (id_vq, param_name, param_type, param_order) values (1, 'date_calc_ctr', 'String', 3);
insert into ORA_TO_CH_VQ_PARAMS (id_vq, param_name, param_type, param_order) values (1, 'year_bo_param', 'UInt32', 4);
insert into ORA_TO_CH_VQ_PARAMS (id_vq, param_name, param_type, param_order) values (1, 'c_year', 'UInt32', 5);
commit;

insert into ora_to_ch_views_query(id,view_name,ch_table,ora_table)
values(1,'v_cache_for_calc_6184_4626','ch_cache_for_calc_6184_4626','ora_cache_for_calc_6184_4626');
--QUERY

 select a.id_datasource,
        a.summa,
        a.id_expense,
        a.id_section,
        a.id_oiv,
        a.id_exp_kind,
        a.id_kosgu,
        a.year,
        a.doc_num,
        a.version,
        a.ra_id,
        a.id_classifier_kbk,
        a.gp_code,
        a.gsp_code,
        a.is_modern,
        a.id_meropr_group,
        a.id_contract,
        a.fp_id, ddate,
        a.is_budget_type,
        a.date_cache,
        coalesce(b.id_oiv,null,0,1) as is_restr,
        a.datecalc_cache,
        a.sign_number
 from (
  select
        1 id_datasource,
        if(dd.type_info<>5,dd.summa,d2.payment_bo) as summa,
        dd.id_expense  as id_expense,
        dd.id_section  as id_section,
        dd.id_oiv      as id_oiv,
        dd.id_exp_kind as id_exp_kind,
        dd.id_kosgu    as id_kosgu,
        dd.year,
        dd.doc_num,
        dd.version,
        coalesce(ra_id,0) ra_id,
        coalesce(id_classifier_kbk,0) as id_classifier_kbk,
        gp_code,
        gsp_code,
        is_modern,
        id_meropr_group,
        dd.id_contract,
        dd.fp_id, ddate,
        is_budget_type,
        dd.sign_number,
        dd.gp_kbk_nyear,
        dd.date_cache,
        dd.datecalc_cache
      from(
             select
                     summa,
                     type_info,
                     coalesce(d.id,0)         as id_expense,
                     coalesce(ds.id,0)        as id_section,
                     coalesce(do.id,0)        as id_oiv,
                     coalesce(dk.id,0)        as id_exp_kind,
                     coalesce(dko.id,0)       as id_kosgu,
                     toYear(t4.date_start)    as year,
                     doc_num,
                     version                   as version,
                     t4.id                     as fp_id,
                     id_contract               as id_contract,
                     coalesce(toYYYYMMDD(date_start),99990000) as ddate,
                     gp.ra_id                  as ra_id,
                     id_classifier_kbk         as id_classifier_kbk,
                     coalesce(gp.gp_code, '0') as gp_code,
                     gp.gsp_code               as gsp_code,
                     is_modern                 as is_modern,
                     gp.id_meropr_group        as id_meropr_group,
                     is_budget_type            as is_budget_type,
                     t4.sign_number            as sign_number,
                     coalesce(gp.n_year, 0)    as gp_kbk_nyear,
                     t4.date_cache             as date_cache,
                     t4.datecalc_cache         as datecalc_cache
              from
               (
                  select
                         tt.summa as summa,tt.purpose as purpose,tt.grbs as grbs,tt.functional as functional,
                 tt.expense as expense,tt.economic as economic,tt.budget_year as budget_year,tt.id as id,
                 t3.doc_num as doc_num,t3.version as version,t3.type_info as type_info,t3.sign_date as sign_date,t3.id_contract as id_contract,
                 t3_.date_start as date_start,
                 t3_.date_end as date_end,
                 t3.sign_number as sign_number,
                 t3.date_cache     as date_cache,
                 t3.datecalc_cache as datecalc_cache
           from msk_arm_v2.eaist_v_financeplan tt
               join (select *
                       from msk_analytics_caches.cache_for_calc_12904_11487
                      where date_cache     = date_cache_1 and
                            datecalc_cache = datecalc_cache_1
                    ) t3 on t3.id_contract = tt.contract_id
               join (select *
                      from msk_arm_v2.eaist_v_contract t3_
                     where toYear(t3_.date_start) < toYear(parseDateTime(date_calc_ctr,'%Y-%m-%d')) and
                             toYear(t3_.date_end) >= toYear(parseDateTime(date_calc_ctr,'%Y-%m-%d')) and
                   t3_.date_start < parseDateTime(date_calc_ctr,'%Y-%m-%d') and
                       t3_.date_end >= parseDateTime(date_calc_ctr,'%Y-%m-%d')
                    ) t3_ on t3_.id = t3.id_contract
           where (
                   t3.type_info  <> 5 or
                   (t3.type_info = 5 and
                    t3.id_contract in (select a.id_contract
                                     from msk_arm_v2.v_ref_financeplan_data_depfi_v a
                                    where a.data_bo < parseDateTime(date_calc_ctr,'%Y-%m-%d') and
                                          a.year_bo = year_bo_param and
                                          a.payment_bo > 0 and
                                          a.id_pbo_type = 1
                                      )
                   )
                 ) and
             t3.sign_date < if((toYear(tt.budget_year) < toYear(today())), parseDateTime(concat(toString(toYear(tt.budget_year)+1),'-01-01'),'%Y-%m-%d') ,today())
        ) t4
                      join msk_analytics.v_gp_kbk_un gp  on  gp.id_budget_type             = 1 and
                                                     t4.expense                    = gp.vr_code and
                                                     t4.purpose                    = gp.cr_code and
                                                     lpad(toString(t4.grbs),3,'0') = gp.gr_code and
                                                     t4.functional                 = gp.fk_code and
                                                     t4.economic                   = gp.kg_code
              left join msk_nsi.d_expense_article d  on t4.purpose    = d.s_code
              left join msk_nsi.d_oiv do             on do.grbs       = lpad(toString(t4.grbs),3,'0')
              left join msk_nsi.d_expense_section ds on t4.functional = ds.s_code
              left join msk_nsi.d_expense_kind dk    on t4.expense    = dk.s_code
              left join msk_nsi.d_expense_kosgu dko  on t4.economic   = dko.s_code
          where (dk.s_code in (select toFixedString(tt.code,1000) from msk_arm_v2.mv_spr_w_vr_contr tt))
                and (coalesce(do.id,0) <> 0)
                and (coalesce(dko.id,0) <> 0)
                and gp.n_year   = c_year
                and gp.cur_year = c_year
      ) dd
      left join (select *
                   from msk_analytics_caches.cache_for_calc_12044_10407
                  where date_cache     = date_cache_1 and
                        datecalc_cache = datecalc_cache_1
                ) d2 on dd.fp_id = d2.id_finplan and
                        d2.rn_pbo = 1 and
                        dd.type_info=5
  ) a
 left join (select id_oiv, id_expense, id_section, id_exp_kind, id_kosgu_ext, year n_year_restr, id_budget_type
              from msk_arm_v2.mv_restriction_pd
             where year = c_year) b on
                                      b.id_budget_type = 1
                                  AND b.id_oiv         = a.id_oiv
                                  AND b.id_expense     = a.id_expense
                                  AND b.id_section     = a.id_section
                                  AND b.id_exp_kind    = a.id_exp_kind
                                  AND b.id_kosgu_ext   = a.id_kosgu
   where (ddate < date_cache_1 and year = c_year)