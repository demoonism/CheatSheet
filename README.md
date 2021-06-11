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


SELECT
sid,
MAX(CASE
WHEN course = 'yuwen' THEN score
ELSE 0
END) AS yuwen,

MAX(CASE
WHEN course='shuxue' THEN score
ELSE 0
END)  as shuxue

FROM course
GROUP BY sid
HAVING shuxue > yuwen
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

Window Function

```sql
rank()
row_number()

lag(col, n, default): skip fist n row
lead(col, n, default): skip last n row


SELECT name, month, count, dense_rank() OVER(PARTITION BY name ORDER BY count desc) ranks
FROM testTable

```
Ordering
Order by: global sort
Sort by: sort within reducer
Distribute by: define how to partition to reducer, often use with sort by.
Cluster by: distributed + sort by column.

```sql
select name from students 
where marks > 75 
order by substring(name, -2) asc, id asc
```

String Functions:
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

3. Pyspark

```python
#Create test DF

datavengers = [
    ["Carol","Data Scientist","USA",70000,5],
    ["Peter","Data Scientist","USA",90000,7]
]
schema = ["Name","Job","Country","salary","seniority"]
df = spark.createDataFrame(data=datavengers, schema = schema)
df.printSchema()

option 2
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

CreatingDataFrame = [
    ("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000)
  ]
schema = StructType([ \
    StructField("employee_name",StringType(),True), \
    StructField("department",StringType(),True), \
    StructField("state",StringType(),True), \
    StructField("salary", IntegerType(), True), \
    StructField("age", StringType(), True), \
    StructField("bonus", IntegerType(), True) \
  ])
df = spark.createDataFrame(data=CreatingDataFrame,schema=schema)


# I/O
#Read
df = spark.read.load("path", format="csv", sep=";", inferSchema="true", header="true")
df = spark.read.csv("path", inferSchema = True, header = True)
df = spark.table("hive_access")
df = spark.sql("select * from hive_access")

#Write
df2.createOrReplaceTempView("test")
df2.write.saveAsTable("testTable")

#Case
df.withColumn("shuxue", when(col("course") == 'shuxue', col("score")).otherwise("0")) \
  .withColumn("yuwen", when(col("course") == 'yuwen', col("score")).otherwise("0"))  \
  .groupby("sid").agg(max("shuxue").alias("shuxue"), max("yuwen").alias("yuwen")) \
  .where(col("shuxue") > col("yuwen")) \
  .show()


#Rename
df2 = df.withColumnRenamed("_c0", 'uname').withColumnRenamed("_c1", 'umonth').withColumnRenamed("_c2", 'ucount')

df3 = df2.groupby(['uname', 'umonth']).sum().withColumnRenamed("sum(ucount)", "num")

df3 = df2.groupby(['uname', 'umonth']).avg("col1", "col2").withColumnRenamed("avg(col1)", "ac1").withColumnRenamed("avg(col2)", "ac2")

df5 = df4.groupBy(["a.uname","a.umonth", "a.num"]).agg(sum('b.num').alias("total"), max('b.num').alias("max"))

#Join

df4 = df3.alias("a").join(df3.alias("b"), df3.name == df3.name, 'inner')


```

5. Python

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

```

pandas category

```python
pd.qcut(col, 5)
pd.get_dummies(train, columns=["Pclass","Embarked","Sex"])
```

pandas check null
```python
train_df.isnull().sum()
```
