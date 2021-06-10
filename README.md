# CheatSheet

# HIVE

1. CREATE TABLE

Managed Table
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

Load Data

```sql
load data inpath '/FileStore/tables/access.csv' into table hive_access;
```

Insert

```sql

drop table if exists course;

create table course (
  id int,
  sid int ,
  course string,
  score int 
);

INSERT INTO course VALUES (1, 1, 'yuwen', 43);
INSERT INTO course VALUES (2, 1, 'shuxue', 55);
```

Case When

```sql
select a.sid, max(a.yuwen) yuwen, max(a.shuxue) shuxue
from
 (select 
  sid,
  case 
  when c.course = 'yuwen' then c.score
  when c.course = 'shuxue' then '3'
  end as yuwen,

  case 
  when c.course = 'shuxue' then c.score
  when c.course = 'yuwen' then 0
  end as shuxue
  from course c) a
group by a.sid
having shuxue > yuwen
```

