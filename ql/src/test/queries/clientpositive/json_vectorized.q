SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

create temporary table jsonvectorized (jsonval string);

insert into jsonvectorized values
  ('{"var" : "imastring" }'),
  ('{"var" : -3 }'),
  ('{"var" : 1987.12342 }'),
  ('{"var" : true }'),
  ('{"var" : [ 1, 2, 3] }'),
  ('{"var" : { "nested" : true } }'),
  ('{"var" : null }'),
  ('{}')
  ;

explain vectorization detail
select 1 from jsonvectorized where jsonval is json;

explain vectorization detail
select 1 from jsonvectorized where jsonval is not json;

explain vectorization detail
select json_value(jsonval, '$.var'),
       json_value(jsonval, '$.var', 'a'),
       json_value(jsonval, '$.var', 1, 'default'),
       json_value(jsonval, '$.var', 1000000.000001, 'default'),
       json_value(jsonval, '$.var', false, 'default')
    from jsonvectorized;

explain vectorization detail
select json_value(jsonval, '$.var'),
       json_value(jsonval, '$.var', 'a'),
       json_value(jsonval, '$.var', 1, 'default'),
       json_value(jsonval, '$.var', 1000000.000001, 'default'),
       json_value(jsonval, '$.var', false, 'default'),
       json_value(jsonval, '$.var', array(4, 5), 'default'),
       json_value(jsonval, '$.var', array(4, 5), 'default', 'default'),
       json_value(jsonval, '$.var', named_struct('nested', false), 'default'),
       json_value(jsonval, '$.var', named_struct('nested', false), 'default', 'default')
    from jsonvectorized;
