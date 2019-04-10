create temporary table jsonvaluecast (jsonval string);

insert into jsonvaluecast values
  ('{"var" : "imastring" }'),
  ('{"var" : -3 }'),
  ('{"var" : 1987.12342 }'),
  ('{"var" : true }'),
  ('{"var" : [ 1, 2, 3] }'),
  ('{"var" : { "nested" : true } }'),
  ('{"var" : null }'),
  ('{}')
  ;

select json_value(jsonval, '$.var'),
       json_value(jsonval, '$.var', 'a'),
       json_value(jsonval, '$.var', 1, 'default'),
       json_value(jsonval, '$.var', 1000000.000001, 'default'),
       json_value(jsonval, '$.var', false, 'default'),
       json_value(jsonval, '$.var', array(4, 5), 'default'),
       json_value(jsonval, '$.var', array(4, 5), 'default', 'default'),
       json_value(jsonval, '$.var', named_struct('nested', false), 'default'),
       json_value(jsonval, '$.var', named_struct('nested', false), 'default', 'default')
    from jsonvaluecast;
