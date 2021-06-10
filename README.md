# CheatSheet

# HIVE

1. CREATE TABLE

```sql
drop table if exists hive_access;

create table if not exists hive_access (

uname string comment 'username',
umonth string comment 'month',
count int comment 'accesss count'
) comment 'user access table'

row format delimited
fields terminated by ','
lines terminated by '\n'
stored as TEXTFILE;
```
