grammar PgArray;

/**
The grammar is used to parse PG array text
representations. E.g.:

  numeric PG arrays:
    {10, NULL, NULL, 20, 30}
    {"10", NULL, NULL, "20", "30"}

  multi-dimentsional PG arrays:
    {{"10", "20"}, {"30", NULL, "40"}}

  json PG arrays:
    {"{\"x\": 10}", "{\"y\": 20}"}
    {\"{\\\"x\\\": 10}\", \"{\\\"y\\\": 20}\"}
*/

array
   : '{' item (',' item)* '}'
   | '{' '}'
   ;

item
   : NULL           #null
   | string         #value
   | NUMBER         #value
   | bool           #value
   | array          #noop
   ;

bool
    : 'true'
    | 'false'
    ;


string
    : STRING        #quotedString
    | CHAR+         #unquotedString
    ;

STRING
    : '"' (ESC | ~["\\])* '"'
    ;

CHAR
    : [!-z]
    ;


NUMBER
    : '-'? DIGIT '.' DIGIT EXP?
    | '-'? DIGIT EXP
    | '-'? DIGIT
    ;

NULL: 'NULL';

fragment ESC
    : '\\' (["\\/bfnrt] | UNICODE)
    ;

fragment UNICODE
    : 'u' HEX HEX HEX HEX
    ;

fragment HEX
    : [0-9a-fA-F]
    ;

fragment DIGIT
    : '0' | [1-9] [0-9]*
    ;

fragment EXP
    : [Ee] [+\-]? DIGIT
    ;

WS
   : [ \t\n\r] + -> skip
   ;
