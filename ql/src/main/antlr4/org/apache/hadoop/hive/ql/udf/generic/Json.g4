/**
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

grammar Json;

object:
       T_OPENBRACE T_CLOSEBRACE
    |  T_OPENBRACE element_list T_CLOSEBRACE
    ;

element_list:
      element
    | element_list T_COMMA element
    ;

element:
    T_DOUBLEQUOTE_STR T_COLON value
    ;

value:
      object
    | array
    | null_literal
    | boolean_literal
    | int_literal
    | decimal_literal
    | string_literal
    ;

array:
      T_OPENBRACKET T_CLOSEBRACKET
    | T_OPENBRACKET array_list T_CLOSEBRACKET
    ;

array_list:
      array_element
    | array_list T_COMMA array_element
    ;

array_element:
    value
    ;

null_literal:
    T_NULL
    ;

boolean_literal:
      T_TRUE
    | T_FALSE
    ;

int_literal:
    T_INT
    ;

decimal_literal:
    T_DECIMAL
    ;

string_literal:
    T_DOUBLEQUOTE_STR
    ;


T_COLON        : ':'  ;
T_COMMA        : ','  ;
T_OPENBRACE    : '{'  ;
T_CLOSEBRACE   : '}'  ;
T_OPENBRACKET  : '['  ;
T_CLOSEBRACKET : ']'  ;

T_FALSE        : 'false' ;
T_NULL         : 'null' ;
T_TRUE         : 'true' ;

T_INT          : [0-9]+ ('e'|'E' ('+'|'-')? [0-9]+)? ;
T_DECIMAL      : [0-9]+ '.' [0-9]* ('e'|'E' ('+'|'-')? [0-9]+)?
               | '.' [0-9]+ ('e'|'E' ('+'|'-')? [0-9]+)? ;

T_DOUBLEQUOTE_STR : '"'  (DOUBLEQUOTE_ESC_CHAR | ~('"'  | '\\'))* '"' ;

DOUBLEQUOTE_ESC_CHAR: '\\"' | ESC_CHAR ;

ESC_CHAR       : '\\\\' | '\\r' | '\\n' | '\\t' | '\\u' [0-9a-f][0-9a-f][0-9a-f][0-9a-f] ;

WS             :   [ \t\r\n]+ -> skip ;
