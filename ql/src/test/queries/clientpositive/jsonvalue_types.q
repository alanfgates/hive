create temporary table jsonalltypes(
    strjson    string,
    charjson   char(200),
    vcharjson  varchar(255),
    chardef    char(5),
    vchardef   varchar(100),
    int1       tinyint,
    int2       smallint,
    int4       int,
    int8       bigint,
    dec4       float,
    dec8       double,
    decbig     decimal(12, 4),
    bbool      boolean,
    llist      array<int>,
    sstruct    struct<key : int, name : string>
    );

insert into jsonalltypes values
  ('{ "skey" : "sval", "ikey" : 3, "dkey" : 3.141592654, "bkey" : true , "lkey" : [ 100, 200 ], "stkey" : { "key" : -1, "name" : "structname" }}',
   '{ "skey" : "xxxx", "ikey" : 4, "dkey" : 2.718281828, "bkey" : false, "lkey" : [ 101, 201 ], "stkey" : { "key" : 18, "name" : "yyyyyyyyyy" }}',
   '{ "skey" : "zzzz", "ikey" : 5, "dkey" : 1.618033988, "bkey" : false, "lkey" : [ 102, 202 ], "stkey" : { "key" : 19, "name" : "aaaaaaaaaa" }}',
   'zz', 'yy', 1, 2, 3, 4, 1.1, 2.2, 10328.23, true, array(5, 6), named_struct('key', 3, 'name', 'n')),
  ('{ "skey" : "bbbb", "ikey" : 6, "dkey" : 1.414213562, "bkey" : true , "lkey" : [ 103, 204 ], "stkey" : { "key" : -8, "name" : "eeeeeeeeee" }}',
   '{ "skey" : "cccc", "ikey" : 7, "dkey" : 1.732050807, "bkey" : false, "lkey" : [ 104, 204 ], "stkey" : { "key" : 18, "name" : "ffffffffff" }}',
   '{ "skey" : "dddd", "ikey" : 8, "dkey" : 2.000000000, "bkey" : false, "lkey" : [ 105, 205 ], "stkey" : { "key" : 19, "name" : "gggggggggg" }}',
   'aa', 'bb', 5, 6, 7, 8, 3.1, 3.2, 30328.23, false, array(1, 2), named_struct('key', 2, 'name', 'b')),
  ('{}', NULL, NULL, 'cc', 'dd', 15, 16, 17, 18, 13.1, 13.2, 130328.23, false, array(11, 12), named_struct('key', 12, 'name', 'c'))
  ;

select json_value(strjson, "$.skey", chardef, 'DEFAULT'),
       json_value(strjson, "$.skey", vchardef, 'DEFAULT'),
       json_value(charjson, "$.skey", vchardef, 'DEFAULT'),
       json_value(vcharjson, "$.skey", chardef, 'DEFAULT')
  from jsonalltypes;

select json_value(strjson, "$.ikey", int1, 'DEFAULT'),
       json_value(strjson, "$.ikey", int2, 'DEFAULT'),
       json_value(strjson, "$.ikey", int4, 'DEFAULT'),
       json_value(strjson, "$.ikey", int8, 'DEFAULT')
  from jsonalltypes;

select json_value(strjson, "$.dkey", dec4, 'DEFAULT'),
       json_value(strjson, "$.dkey", dec8, 'DEFAULT'),
       json_value(strjson, "$.dkey", decbig, 'default'),
       json_value(strjson, "$.bkey", bbool, 'DEFAULT')
  from jsonalltypes;

select json_value(strjson, "$.lkey[0]", 1),
       json_value(strjson, "$.stkey.key", 1),
       json_value(strjson, "$.lkey", llist, 'DEFAULT'),
       json_value(strjson, "$.stkey", sstruct, 'DEFAULT')
  from jsonalltypes;

