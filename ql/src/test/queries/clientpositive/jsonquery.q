create temporary table jsonquery (
    jsonval string,
    c       char(5),
    vc      varchar(100));

insert into jsonquery values
  ('{"var" : "imastring" }', 'abc', 'def'),
  ('{"var" : -3 }', 'ghi', 'jkl'),
  ('{"var" : 1987.12342 }', 'mno', 'pqr'),
  ('{"var" : true }', 'stu', 'vwx'),
  ('{"var" : [ 1, 2, 3] }', 'yzA', 'BCD'),
  ('{"var" : { "nested" : [ 1, { "key" : "value", "anotherkey" : 23 } ] } }', 'EFG', 'HIJ'),
  ('{"var" : null }', 'KLM', 'NOP'),
  ('{}', 'QRS', 'TUV')
  ;

select json_query(jsonval, '$.var'),
       json_query(jsonval, '$.var', 'a'),
       json_query(jsonval, '$.var', c, 'default'),
       json_query(jsonval, '$.var', vc, 'default')
    from jsonquery;
