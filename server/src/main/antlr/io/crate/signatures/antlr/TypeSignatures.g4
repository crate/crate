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

grammar TypeSignatures;

type
    : 'double precision'            #doublePrecision
    | 'timestamp without time zone' #timeStampWithoutTimeZone
    | 'timestamp with time zone'    #timeStampWithTimeZone
    | 'time with time zone'         #timeWithTimeZone
    | 'array' '(' type ')'          #array
    | 'record' parameters?          #row
    | 'object' parameters?          #object
    | identifier parameters?        #generic
    ;

parameters
    :  ('(' parameter (',' parameter)* ')')
    ;

parameter
    : type
    | identifier type
    | INTEGER_VALUE
    ;

identifier
    : UNQUOTED_INDENTIFIER
    | QUOTED_INDENTIFIER
    ;

UNQUOTED_INDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

QUOTED_INDENTIFIER
    : '"' (ESCAPED | SAFE )* '"'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

fragment ESCAPED
    : '\\' (["\\/bfnrt])
    ;

fragment SAFE
    : ~ ["\\\u0000-\u001F]
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Za-z]
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;
