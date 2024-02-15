drop table test1;
drop table test2;
drop table test3;

create table test1(
  id        integer not null constraint pk_test1 primary key,
  nyear     integer not null,
  str_value varchar2(100) not null,
  num_value number not null
);

create table test2(
  id        integer not null constraint pk_test2 primary key,
  nyear     integer not null,
  str_value varchar2(100) not null,
  num_value number not null
);

create table test3(
  id        integer not null constraint pk_test3 primary key,
  nyear     integer not null,
  str_value varchar2(100) not null,
  num_value number not null
);

insert into test1
select rownum as id,
       round(dbms_random.value(2000,2024)) as nyear,
       dbms_random.string('x',32) as str_value,
       dbms_random.value(0,1000)  as num_value
  from dual
connect by rownum <= 300000;
commit;

insert into test2
select rownum as id,
       round(dbms_random.value(2000,2024)) as nyear,
       dbms_random.string('x',32) as str_value,
       dbms_random.value(0,1000)  as num_value
  from dual
connect by rownum <= 300000;
commit;

insert into test3
select rownum as id,
       round(dbms_random.value(2000,2024)) as nyear,
       dbms_random.string('x',32) as str_value,
       dbms_random.value(0,1000)  as num_value
  from dual
connect by rownum <= 300000;
commit;
