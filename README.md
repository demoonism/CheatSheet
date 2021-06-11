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
load data [local] inpath '/FileStore/tables/access.csv' into table hive_access;
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
  when A is not null and A <> '' then A
  when B is not null and B <> '' then B
  else C
  end as score  
  from course c) a
group by a.sid
having shuxue > yuwen
```

COALESCE
```sql
select COALESCE(comm, 0) from emp;
-- replace null with 0.
```

With Statement

```sql
WITH t1 AS (
		SELECT *
		FROM carinfo
	), 
	t2 AS (
		SELECT *
		FROM car_blacklist
	)
SELECT *
FROM t1, t2
```

Casting

```sql
select cast('2147483648' as bigint);
select cast('2147483648' as int);
```

# Ordering
Order by: global sort
Sort by: sort within reducer
Distribute by: define how to partition to reducer, often use with sort by.
Cluster by: distributed + sort by column.

```sql
select name from students 
where marks > 75 
order by substring(name, -2) asc, id asc
```

# String Functions:
```sql
CONCAT('hadoop','-','hive') returns 'hadoop-hive'
CONCAT_WS('-','hadoop','hive') returns 'hadoop-hive'
FIND_IN_SET('ha','hao,mn,hc,ha,hef') returns 4
LENGTH('hive') returns 4
LOWER('HiVe') returns 'hive'
UPPER('HiVe') returns 'HIVE'
LPAD('hive',6,'v') returns 'vvhive'
LTRIM('   hive') returns 'hive'
REPEAT('hive',2) returns 'hivehive'
RPAD('hive',6,'v') returns 'hivevv'
REVERSE('hive') returns 'evih'
LTRIM('hive   ') returns 'hive'
SPACE(4) returns '    '
SPLIT('hive:hadoop',':') returns ["hive","hadoop"]
SUBSTR('hadoop',4,2) returns 'oo'
SUBSTR('hadoop',4) returns 'oop'
SUBSTR('hadoop',-1) returns 'p'
TRIM('   hive   ') returns 'hive'
```

3. Python

File I/O

```python
# Read
with open("welcome.txt") as file:

   data = file.read()
   
# Write
with open('output.txt', 'w') as file:  # Use file to refer to the file object

    file.write('Hi there!')
    
```

list comprehension

```python

#if else has to go before the for clause, but just if can go after
correct: [len(i) if len(i) > 70 else 0 for i in data]
correct: [len(i) for i in data if len(i) > 70]

#cannt have if else after or just if before.
wrong: [len(i) for i in data if len(i) > 70 else 0]
wrong: [len(i) if len(i) > 70 for i in data ]

```

Pandas set value by condition

```python
#1 
df['target'] = 0
df.loc[condition, 'target'] = 1

#2
df['target'] = np.where(condition, value, elseValue)
