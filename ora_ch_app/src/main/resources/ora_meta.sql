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
