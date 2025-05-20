**实验4命令**

```
get "students","RB0000001","data:birthday"
scan "students",{COLUMNS=>"data:birthday",LIMIT=>10}
scan "students",{COLUMNS=>["data:birthday","data:name"],LIMIT=>10}
scan "students",{COLUMNS=>"data:name",LIMIT=>10}
scan "students",{COLUMNS=>"data:name:toString",LIMIT=>10}
```
**过滤器**

```
[//]: # (列簇过滤器)
scan "students",FILTER=>"FamilyFilter(=,'binary:data')",LIMIT=>1 

[//]: # (查询列名有e的数据)
scan "students",FILTER=>"QualifierFilter(=,'substring:e')",LIMIT=>1 

[//]: # (三月出生学生信息)
scan "students",COLUMNS=>"data:birthday:toString",FILTER=>"ValueFilter(=,'substring:-03-')",LIMIT=>10

[//]: # (查询大于90分的学生信息)
scan "students",COLUMNS=>"data:score",FILTER=>"ValueFilter(>,'binary:90')",LIMIT=>10

[//]: # (特殊过滤器用于结果的特定化展示，如只返回3列数据)
scan "students",FILTER=>"ColumnCountGetFilter(3)"
```
**任务一：计数，男女生总人数**

```
[//]: # (Shell实现)
count "students",{FILTER=>"ValueFilter(=,'binary:男')"}
count "students",{FILTER=>"ValueFilter(=,'binary:女')"}

```
