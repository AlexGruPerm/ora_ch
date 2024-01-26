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
When you want load (recreate clickhouse table) date from oracle into clickhouse you can use Json like this:

```
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
"ip": "15.6.7.8",
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
]}
]
}
```

all possible keys:

```
schema - schema name
  recreate: Int - 0,1 for guard, recreate table on clickhouse or not.
  name: String - table name
  pk_columns:"date_start,date_end,id" - if you want any p.k. coluns
  only_columns: Array[String] - if you want load just part of columns.
  ins_select_order_by: String - order by part when select data in oracle.
  partition_by: String - PRIMARY KEY() for clickhouse
  notnull_columns: Array[String] - list of columns that marker as nullable (possible) in oracle, but you know that they
  not null
  where_filter : String - additional where filter when select data in oracle.
  sync_by_column_max - sigle column marker. If you want just upload nes data into clickhouse, 
  without clickhouse table recreation. 
```

you can make post request on 2 addresses:

web_service_url:8081/task
web_service_url:8081/calc

