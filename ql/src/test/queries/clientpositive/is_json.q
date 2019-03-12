--! qt:dataset:src_json
--! qt:dataset:src

DESCRIBE FUNCTION isjson;
DESCRIBE FUNCTION EXTENDED isjson;
DESCRIBE FUNCTION isnotjson;
DESCRIBE FUNCTION EXTENDED isnotjson;

select 1 from src_json where json is json;

select 1 from src_json where json is not json;

select 1 from src where key is json;

select 1 from src where key is not json limit 1;

