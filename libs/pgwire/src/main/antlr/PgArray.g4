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
    : string
    | array
    ;

string
    : QUOTED_STRING     #quotedString
    | NULL              #null
    | UNQUOTED_STRING   #unquotedString
    ;


NULL
    : [nN] [uU] [lL] [lL]
    ;


QUOTED_STRING
    : '"' (ESC | ~["\\])* '"'
    ;

UNQUOTED_STRING
    : CHAR+ (' ')* CHAR+
    | CHAR+
    ;

fragment CHAR
    : ~[,"\\{} \t\n\r]
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

WS: [ \t\n\r]+ -> skip;
