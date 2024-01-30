create user ORACH
  identified by ORACH
  default tablespace USERS
  temporary tablespace TEMP
  profile DEFAULT
  quota unlimited on users;

grant connect to ORACH;
grant resource to ORACH;

grant create any table to ORACH;
grant select any dictionary to ORACH;
grant select any table to ORACH;
grant unlimited tablespace to ORACH;
grant update any table to ORACH;