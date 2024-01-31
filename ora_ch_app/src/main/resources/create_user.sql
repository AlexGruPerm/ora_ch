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

BEGIN
  DBMS_NETWORK_ACL_ADMIN.create_acl (
    acl          => 'ws_orach.xml',
    description  => 'ACL for ORACH user to access orach web service.',
    principal    => 'ORACH',
    is_grant     => TRUE,
    privilege    => 'connect',
    start_date   => SYSTIMESTAMP,
    end_date     => NULL);
  COMMIT;
END;

BEGIN
  DBMS_NETWORK_ACL_ADMIN.assign_acl (
    acl         => 'ws_orach.xml',
    host        => '10.206.215.9',
    lower_port  => 8081,
    upper_port  => 8081);
END;

COMMIT;