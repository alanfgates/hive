create temporary table jsonvalue (jsonval string, defaultval string);

insert into jsonvalue values
  ('{"name" : "harry", "age" : 17, "gpa" : 3.03, "honors" : false, "classes" : [ "math", "history" ] }', 'ron'),
  ('{"name" : "hermione", "age" : 18, "gpa" : 4.00, "honors" : true, "classes" : [ "science", "french" ]}', 'ginny'),
  ('{"name" : null, "age" : null, "gpa" : null, "honors" : null, "classes" : null}', 'no name'),
  ('{}', 'empty');

select json_value(jsonval, '$.name'),
       json_value(jsonval, '$.age', 1L),
       json_value(jsonval, '$.age', 1),
--     json_value(jsonval, '$.gpa', 1.0),
       json_value(jsonval, '$.honors', true)
    from jsonvalue;

select json_value(jsonval, '$.name', 'fred', 'DEFAULT'),
       json_value(jsonval, '$.age', 1L, 'DEFAULT'),
       json_value(jsonval, '$.age', 1, 'DEFAULT'),
--     json_value(jsonval, '$.gpa', 1.0, 'DEFAULT'),
       json_value(jsonval, '$.honors', true, 'DEFAULT')
    from jsonvalue;

select json_value(jsonval, '$.name', defaultval, 'DEFAULT')
    from jsonvalue;

select 
--     json_value(jsonval, '$.classes'),
       json_value(jsonval, '$.classes[$index]', 'a', 'NULL', 'NULL', 'index', 0)
    from jsonvalue;

  
