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
   : string         #value
   | NUMBER         #value
   | array          #noop
   | 'true'         #value
   | 'false'        #value
   | 'NULL'         #null
   ;


string
    : STRING        #quotedString
    | QSTRING       #unquotedString
    ;

STRING
    : '"' (ESC | ~["\\])* '"'
    ;


QSTRING
    : DIGIT* CHAR+
    ;

CHAR
    : [a-zA-Z]
    | [!-+]
    | '-'
    | '_'
    | DIGIT
    ;

NUMBER
    : '-'? DIGIT ('.' [0-9] +)? EXP?
    ;


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
