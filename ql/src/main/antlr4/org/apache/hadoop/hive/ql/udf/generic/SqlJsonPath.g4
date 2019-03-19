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

grammar SqlJsonPath;

// I've copied this more or less directly from the SQL 2016 spec, sections 9.38 and 9.39
// Thus some of the rules are silly or trivial, but it makes it easier to follow
// when looking at the spec.  In some places where it didn't seem to matter or was
// just too silly to put up with I've reduced it.

path_expression:
      path_wff // If I read the spec correctly, STRICT/LAX is required, but that seems pendantic.  Better to default to lax.
    | path_mode path_wff
    ;

path_mode:
      path_mode_strict
    | T_LAX
    ;

path_mode_strict:
    T_STRICT
    ;

path_wff:   // What does wff stand for?
    additive_expression
    ;

additive_expression:
      multiplicative_expression
    | additive_expression T_PLUS multiplicative_expression
    | additive_expression T_MINUS multiplicative_expression
    ;

multiplicative_expression:
      unary_expression
    | multiplicative_expression T_STAR unary_expression
    | multiplicative_expression T_SLASH unary_expression
    | multiplicative_expression T_PERCENT unary_expression
    ;

unary_expression:
      accessor_expression
    | T_PLUS unary_expression
    | T_MINUS unary_expression
    ;

accessor_expression:
      path_primary
    | accessor_expression accessor_op
    ;

path_primary:
      path_literal
    | path_variable
    | T_OPENPAREND path_wff T_CLOSEPAREND
    ;

path_variable:
      path_context_variable
    | path_named_variable
    | path_at_variable
    ;

path_context_variable:
    T_DOLLAR
    ;

path_named_variable:
    T_DOLLAR T_IDENTIFIER
    ;

path_at_variable:
    T_AT
    ;

accessor_op:
      member_accessor
    | wildcard_member_accessor
    | array_accessor
    | wildcard_array_accessor
    | filter_expression
    | item_method
    ;

member_accessor:
      member_accessor_id
    | member_accessor_string
    ;

member_accessor_id:
    T_DOT T_IDENTIFIER
    ;

member_accessor_string:
    T_DOT path_string_literal
    ;

wildcard_member_accessor:
    T_DOT T_STAR
    ;

array_accessor:
    T_OPENBRACKET subscript_list T_CLOSEBRACKET
    ;

subscript_list:
      subscript
    | subscript_list T_COMMA subscript
    ;

subscript:
      subscript_simple
    | subscript_to
    ;

subscript_simple:
    subscript_item
    ;

subscript_to:
    path_wff T_TO subscript_item
    ;

subscript_item:
      path_wff
    | subscript_last
    ;

subscript_last:
    T_LAST
    ;

wildcard_array_accessor:
    T_OPENBRACKET T_STAR T_CLOSEBRACKET
    ;

filter_expression:
    T_QUESTION T_OPENPAREND path_predicate T_CLOSEPAREND
    ;

item_method:
    T_DOT method
    ;

method:
      method_type
    | method_size
    | method_double
    | method_int     // Added in, not part of the spec.  But it seems goofy to have double() and not int()
                     // since ceiling and floor don't take strings.  You can get around it with string.double().floor()
                     // but that seems silly.
    | method_ceiling
    | method_floor
    | method_abs
//  | T_DATETIME T_OPENPAREND path_string_literal? T_CLOSEPAREND
//  | T_KEYVALUE T_OPENPAREND T_CLOSEPAREND
    ;

// Note, we do not currently support the DATETIME or KEYVALUE methods.  There are methods in Hive to do casts from
// String to Datetime, there's no reason to embed the same functionality in here.
// As far as I can tell KEYVALUE is a way to remove duplicate keys, which my implementation doesn't support anyway.

method_type:
    T_TYPE T_OPENPAREND T_CLOSEPAREND
    ;

method_size:
    T_SIZE T_OPENPAREND T_CLOSEPAREND
    ;

method_int:
    T_INTFUNC T_OPENPAREND T_CLOSEPAREND
    ;

method_double:
    T_DOUBLE T_OPENPAREND T_CLOSEPAREND
    ;

method_ceiling:
    T_CEILING T_OPENPAREND T_CLOSEPAREND
    ;

method_floor:
    T_FLOOR T_OPENPAREND T_CLOSEPAREND
    ;

method_abs:
    T_ABS T_OPENPAREND T_CLOSEPAREND
    ;


// predicates
path_predicate:
    boolean_disjunction
    ;

boolean_disjunction:
      boolean_conjunction
    | boolean_disjunction T_OR boolean_conjunction
    ;

boolean_conjunction:
      boolean_negation
    | boolean_conjunction T_AND boolean_negation
    ;

boolean_negation:
      predicate_primary
    | T_BANG delimited_predicate
    ;

predicate_primary:
      delimited_predicate
    | nondelimited_predicate
    ;

delimited_predicate:
      exists_path_predicate
    | T_OPENPAREND path_predicate T_CLOSEPAREND
    ;

nondelimited_predicate:
      comparison_predicate
    | like_regex_predicate
    | starts_with_predicate
    | unknown_predicate
    ;

exists_path_predicate:
    T_EXISTS T_OPENPAREND path_wff T_CLOSEPAREND
    ;

comparison_predicate:
      comparison_predicate_equals
    | comparison_predicate_not_equals
    | comparison_predicate_greater_than
    | comparison_predicate_greater_than_equals
    | comparison_predicate_less_than
    | comparison_predicate_less_than_equals
    ;

comparison_predicate_equals:
    path_wff T_EQUALS path_wff
    ;

comparison_predicate_not_equals:
    path_wff T_NE path_wff
    ;

comparison_predicate_greater_than:
    path_wff T_GT path_wff
    ;

comparison_predicate_greater_than_equals:
    path_wff T_GE path_wff
    ;

comparison_predicate_less_than:
    path_wff T_LT path_wff
    ;

comparison_predicate_less_than_equals:
    path_wff T_LE path_wff
    ;

like_regex_predicate:
    path_wff T_LIKEREGEX path_string_literal       // NOTE - we don't support flags
    ;

starts_with_predicate:
    path_wff T_STARTS T_WITH starts_with_initial
    ;

starts_with_initial:
      path_string_literal
    | path_named_variable
    ;

unknown_predicate:
    T_OPENPAREND path_predicate T_CLOSEPAREND T_IS T_UNKNOWN
    ;

path_literal:
      path_null_literal
    | path_boolean_literal
    | path_numeric_literal
    | path_string_literal
    ;

path_null_literal:
    T_NULL
    ;

path_boolean_literal:
      T_TRUE
    | T_FALSE
    ;

path_numeric_literal:
      path_integer_literal
    | path_decimal_literal
    ;

path_integer_literal:
    T_INT
    ;

path_decimal_literal:
    T_DECIMAL
    ;

path_string_literal:
      T_SINGLEQUOTE_STR
    | T_DOUBLEQUOTE_STR
    ;


// Lexical tokens
T_STAR         : '*'  ;
T_AT           : '@'  ;
T_COMMA        : ','  ;
T_DOT          : '.'  ;
T_DOLLAR       : '$'  ;
T_QUESTION     : '?'  ;
T_AND          : '&&' ;
T_OR           : '||' ;
T_BANG         : '!'  ;
T_GT           : '>'  ;
T_GE           : '>=' ;
T_LT           : '<'  ;
T_LE           : '<=' ;
T_EQUALS       : '==' ;
T_NE           : '<>'
               | '!=' ;  // NOTE - != is not in the spec, but given that Hive supports it I added it
T_OPENBRACKET  : '['  ;
T_OPENPAREND   : '('  ;
T_CLOSEBRACKET : ']'  ;
T_CLOSEPAREND  : ')'  ;
T_PLUS         : '+'  ;
T_MINUS        : '-'  ;
T_SLASH        : '/'  ;
T_PERCENT      : '%'  ;

// keywords
T_ABS          : 'abs' ;
T_CEILING      : 'ceiling' ;
T_DATETIME     : 'datetime' ;
T_DOUBLE       : 'double' ;
T_EXISTS       : 'exists' ;
T_FALSE        : 'false' ;
T_FLOOR        : 'floor' ;
T_IS           : 'is' ;
T_INTFUNC      : 'integer' ; // Added in, not part of the spec
T_KEYVALUE     : 'keyvalue' ;
T_LAST         : 'last' ;
T_LAX          : 'lax' ;
T_LIKEREGEX    : 'like_regex' ;
T_NULL         : 'null' ;
T_SIZE         : 'size' ;
T_STARTS       : 'starts' ;
T_STRICT       : 'strict' ;
T_TO           : 'to' ;
T_TRUE         : 'true' ;
T_TYPE         : 'type' ;
T_UNKNOWN      : 'unknown' ;
T_WITH         : 'with' ;

T_INT          : [0-9]+ ('e'|'E' ('+'|'-')? [0-9]+)? ;
T_DECIMAL      : [0-9]+ '.' [0-9]* ('e'|'E' ('+'|'-')? [0-9]+)?
               | '.' [0-9]+ ('e'|'E' ('+'|'-')? [0-9]+)? ;

// NOTE - this does not match the spec at all.  For now I have it set to just ASCII
// letters and numbers.  It's supposed to support any unicode characters and numbers
// plus any unicode connecting character (rather than just '_').  Antlr does not
// have support for unicode character classes.  Ideally we need to figure out a way
// to support that, at least more common characters.  But an argument can also be made
// for matching Hive's identifier support.
T_IDENTIFIER   : ([0-9]|[a-z]|[A-Z]) ([0-9]|[a-z]|[A-Z]|'_')* ;

// NOTE, this does not exactly match the SQL/JSON Path spec, as that would allow strings
// like 'this is a \s\i\\l\l\y string' because it only specifies a few escape characters
// and says everything else should be ok.  This instead says you can't have any \ in
// your string unless it is itself escaped \\, is a \n, \t, or \r, unicode escape, or
// quote escape \' or \"  This is both much easier to write a rule for and in my (Alan's)
// opinion much more reasonable.
T_SINGLEQUOTE_STR : '\'' (SINGLEQUOTE_ESC_CHAR | ~('\'' | '\\'))* '\'' ;
T_DOUBLEQUOTE_STR : '"'  (DOUBLEQUOTE_ESC_CHAR | ~('"'  | '\\'))* '"' ;

SINGLEQUOTE_ESC_CHAR: '\\\'' | ESC_CHAR ;
DOUBLEQUOTE_ESC_CHAR: '\\"' | ESC_CHAR ;

ESC_CHAR       : '\\\\' | '\\r' | '\\n' | '\\t' | '\\u' [0-9a-f][0-9a-f][0-9a-f][0-9a-f] ;

WS             :   [ \t\r\n]+ -> skip ;

