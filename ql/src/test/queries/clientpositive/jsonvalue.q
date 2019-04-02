create temporary table jsonvalue (jsonval string);

insert into jsonvalue values
  ('{"name" : "harry", "age" : 17, "gpa" : 3.03, "honors" : false, "classes" : [ "math", "history" ] }'),
  ('{"name" : "hermione", "age" : 18, "gpa" : 4.00, "honors" : true, "classes" : [ "science", "french" ]}'),
  ('{"name" : null, "age" : null, "gpa" : null, "honors" : null, "classes" : null}'),
  ('{}');

select json_value(jsonval, '$.name'), json_value(jsonval, '$.age', 'bigint'),
       json_value(jsonval, '$.gpa', 'double'), json_value(jsonval, '$.honors', 'boolean')
       from jsonvalue;
  
