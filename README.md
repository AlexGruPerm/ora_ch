# orach

Http service that can be used to load data from oracle into clickhouse and back.
You can make whole loading (when clickhouse table is recreated) or
partial append. You can make calculation on clickhouse side with using 2 possibilities: 
parametrized view or query with parameters.
Also, you can update columns in clickhouse table from oracle table.

<picture>
 <img alt="image" src="https://i.ibb.co/0q5yNDT/orach.png">
</picture>

How it works:
When you want to load (recreate clickhouse table) date from oracle into clickhouse you can use Json like this:

```json
{
  "servers":{
  "oracle":{
  "ip" : "1.2.3.4",
  "port": 1521,
  "tnsname" : "ora_tnsname",
  "fetch_size" : 10000,
  "user" : "login",
  "password" : "password"
  },
  "clickhouse":{
  "ip": "5.6.7.8",
  "port": 8123,
  "db" : "default",
  "batch_size" : 20000,
  "user" : "ch_login",
  "password" : "ch_password"
  },
  "config":{
  "mode":"sequentially"
  }
},
  "schemas":[
    {"schema":"schema_in_oracle",
      "tables":[
                {
                  "recreate":1,
                  "name":"table_name",
                  "where_filter":" rn <= 5000 "
                }
              ]
    }
   ]
}
```

all possible keys:

```
schema - schema name
  recreate: Int - 0,1 for guard, recreate table on clickhouse or not.
  name: String - table name
  pk_columns:"date_start,date_end,id" - if you want any p.k. coluns
  only_columns: Array[String] - if you want to load just part of columns.
  ins_select_order_by: String - order by part when select data in oracle.
  partition_by: String - PARTITION BY() for clickhouse
  notnull_columns: Array[String] - list of columns that marker as nullable (possible) in oracle, but you know that they
  not null
  where_filter : String - additional where filter when select data in oracle.
  sync_by_column_max - single column marker. If you want to just upload new data into clickhouse, 
  without this key clickhouse table recreated. 
```

you can make post request on 2 addresses:

web_service_url:8081/task for data loading/appending</br>
web_service_url:8081/calc for make calculation and loading results back into oracle. 

For work with service you to need make oracle metadata tables:

```  sql
drop table ora_to_ch_tasks_tables;
drop table ora_to_ch_tasks;

drop table ora_to_ch_vq_params;
drop table ora_to_ch_views_query_log;
drop table ora_to_ch_views_query;

drop sequence s_ora_to_ch_tasks;
drop sequence s_ora_to_ch_views_query_log;

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
 state           varchar2(32) default 'none',
 begin_datetime  date default sysdate,
 end_datetime    date
);

create table ora_to_ch_tasks_tables(
 id_task              integer not null constraint fk_ora_to_ch_tbl_tsk references ora_to_ch_tasks(id) on delete cascade,
 schema_name          varchar2(32) not null,
 table_name           varchar2(32) not null,
 begin_datetime       date,
 end_datetime         date,
 state                varchar2(32) default 'none',
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
```

Examples:




