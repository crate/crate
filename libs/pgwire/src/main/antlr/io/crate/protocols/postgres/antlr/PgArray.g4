/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

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
    : CHAR+ ((' ')+ CHAR+)*
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
