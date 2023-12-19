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

lexer grammar SqlBaseLexer;

options {
   superClass = AbstractSqlBaseLexer;
}

/*
 * Antlr does not generate an import statement for SqlBaseLexer to be able to extend AbstractSqlBaseLexer.
 * seems like a known issue? https://github.com/antlr/antlr4/issues/3124
 * The workaround is found from: https://github.com/tunnelvisionlabs/antlr4ts/issues/483
 */
@lexer::header {
import io.crate.sql.AbstractSqlBaseLexer;
}

AUTHORIZATION: 'AUTHORIZATION';
SELECT: 'SELECT';
FROM: 'FROM';
TO: 'TO';
AS: 'AS';
AT: 'AT';
ALL: 'ALL';
ANY: 'ANY';
SOME: 'SOME';
DEALLOCATE: 'DEALLOCATE';
DIRECTORY: 'DIRECTORY';
DISTINCT: 'DISTINCT';
WHERE: 'WHERE';
GROUP: 'GROUP';
BY: 'BY';
ORDER: 'ORDER';
HAVING: 'HAVING';
LIMIT: 'LIMIT';
OFFSET: 'OFFSET';
OR: 'OR';
AND: 'AND';
IN: 'IN';
NOT: 'NOT';
EXISTS: 'EXISTS';
BETWEEN: 'BETWEEN';
LIKE: 'LIKE';
ILIKE: 'ILIKE';
IS: 'IS';
NULL: 'NULL';
TRUE: 'TRUE';
FALSE: 'FALSE';
IGNORE: 'IGNORE';
RESPECT: 'RESPECT';
NULLS: 'NULLS';
FETCH: 'FETCH';
FIRST: 'FIRST';
LAST: 'LAST';
NEXT: 'NEXT';
ESCAPE: 'ESCAPE';
ASC: 'ASC';
DESC: 'DESC';
SUBSTRING: 'SUBSTRING';
TRIM: 'TRIM';
LEADING: 'LEADING';
TRAILING: 'TRAILING';
BOTH: 'BOTH';
FOR: 'FOR';
TIME: 'TIME';
ZONE: 'ZONE';
YEAR: 'YEAR';
MONTH: 'MONTH';
DAY: 'DAY';
HOUR: 'HOUR';
MINUTE: 'MINUTE';
SECOND: 'SECOND';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
CURRENT_SCHEMA: 'CURRENT_SCHEMA';
CURRENT_USER: 'CURRENT_USER';
SESSION_USER: 'SESSION_USER';
EXTRACT: 'EXTRACT';
CASE: 'CASE';
WHEN: 'WHEN';
THEN: 'THEN';
ELSE: 'ELSE';
END: 'END';
IF: 'IF';
INTERVAL: 'INTERVAL';
JOIN: 'JOIN';
CROSS: 'CROSS';
OUTER: 'OUTER';
INNER: 'INNER';
LEFT: 'LEFT';
RIGHT: 'RIGHT';
FULL: 'FULL';
NATURAL: 'NATURAL';
USING: 'USING';
ON: 'ON';
OVER: 'OVER';
WINDOW: 'WINDOW';
PARTITION: 'PARTITION';
PROMOTE: 'PROMOTE';
RANGE: 'RANGE';
ROWS: 'ROWS';
UNBOUNDED: 'UNBOUNDED';
PRECEDING: 'PRECEDING';
FOLLOWING: 'FOLLOWING';
CURRENT: 'CURRENT';
ROW: 'ROW';
WITH: 'WITH';
WITHOUT: 'WITHOUT';
RECURSIVE: 'RECURSIVE';
CREATE: 'CREATE';
BLOB: 'BLOB';
TABLE: 'TABLE';
SWAP: 'SWAP';
GC: 'GC';
DANGLING: 'DANGLING';
ARTIFACTS: 'ARTIFACTS';
DECOMMISSION: 'DECOMMISSION';
CLUSTER: 'CLUSTER';
REPOSITORY: 'REPOSITORY';
SNAPSHOT: 'SNAPSHOT';
ALTER: 'ALTER';
KILL: 'KILL';
ONLY: 'ONLY';

ADD: 'ADD';
COLUMN: 'COLUMN';

OPEN: 'OPEN';
CLOSE: 'CLOSE';

RENAME: 'RENAME';

REROUTE: 'REROUTE';
MOVE: 'MOVE';
SHARD: 'SHARD';
ALLOCATE: 'ALLOCATE';
REPLICA: 'REPLICA';
CANCEL: 'CANCEL';
RETRY: 'RETRY';
FAILED: 'FAILED';

BOOLEAN: 'BOOLEAN';
BYTE: 'BYTE';
SHORT: 'SHORT';
INTEGER: 'INTEGER';
INT: 'INT';
LONG: 'LONG';
FLOAT: 'FLOAT';
DOUBLE: 'DOUBLE';
PRECISION: 'PRECISION';
TIMESTAMP: 'TIMESTAMP';
IP: 'IP';
CHARACTER: 'CHARACTER';
CHAR_SPECIAL: '"CHAR"';
VARYING: 'VARYING';
OBJECT: 'OBJECT';
STRING_TYPE: 'STRING';
GEO_POINT: 'GEO_POINT';
GEO_SHAPE: 'GEO_SHAPE';
GLOBAL : 'GLOBAL';
SESSION : 'SESSION';
LOCAL : 'LOCAL';

BEGIN: 'BEGIN';
START: 'START';
COMMIT: 'COMMIT';
WORK: 'WORK';
TRANSACTION: 'TRANSACTION';
TRANSACTION_ISOLATION: 'TRANSACTION_ISOLATION';
CHARACTERISTICS: 'CHARACTERISTICS';
ISOLATION: 'ISOLATION';
LEVEL: 'LEVEL';
SERIALIZABLE: 'SERIALIZABLE';
REPEATABLE: 'REPEATABLE';
COMMITTED: 'COMMITTED';
UNCOMMITTED: 'UNCOMMITTED';
READ: 'READ';
WRITE: 'WRITE';
DEFERRABLE: 'DEFERRABLE';

RETURNS: 'RETURNS';
CALLED: 'CALLED';
REPLACE: 'REPLACE';
FUNCTION: 'FUNCTION';
LANGUAGE: 'LANGUAGE';
INPUT: 'INPUT';

ANALYZE: 'ANALYZE';
COSTS: 'COSTS';
VERBOSE: 'VERBOSE';
DISCARD: 'DISCARD';
PLANS: 'PLANS';
SEQUENCES: 'SEQUENCES';
TEMPORARY: 'TEMPORARY';
TEMP: 'TEMP';
CONSTRAINT: 'CONSTRAINT';
CHECK: 'CHECK';
DESCRIBE: 'DESCRIBE';
EXPLAIN: 'EXPLAIN';
FORMAT: 'FORMAT';
TYPE: 'TYPE';
TEXT: 'TEXT';
GRAPHVIZ: 'GRAPHVIZ';
LOGICAL: 'LOGICAL';
DISTRIBUTED: 'DISTRIBUTED';
CAST: 'CAST';
TRY_CAST: 'TRY_CAST';
SHOW: 'SHOW';
TABLES: 'TABLES';
SCHEMAS: 'SCHEMAS';
CATALOGS: 'CATALOGS';
COLUMNS: 'COLUMNS';
PARTITIONS: 'PARTITIONS';
FUNCTIONS: 'FUNCTIONS';
MATERIALIZED: 'MATERIALIZED';
VIEW: 'VIEW';
OPTIMIZE: 'OPTIMIZE';
REFRESH: 'REFRESH';
RESTORE: 'RESTORE';
DROP: 'DROP';
ALIAS: 'ALIAS';
UNION: 'UNION';
EXCEPT: 'EXCEPT';
INTERSECT: 'INTERSECT';
SYSTEM: 'SYSTEM';
BERNOULLI: 'BERNOULLI';
TABLESAMPLE: 'TABLESAMPLE';
STRATIFY: 'STRATIFY';
INSERT: 'INSERT';
INTO: 'INTO';
VALUES: 'VALUES';
DELETE: 'DELETE';
UPDATE: 'UPDATE';
KEY: 'KEY';
DUPLICATE: 'DUPLICATE';
CONFLICT: 'CONFLICT';
DO: 'DO';
NOTHING: 'NOTHING';
SET: 'SET';
RESET: 'RESET';
DEFAULT: 'DEFAULT';
COPY: 'COPY';
CLUSTERED: 'CLUSTERED';
SHARDS: 'SHARDS';
PRIMARY_KEY: 'PRIMARY KEY';
OFF: 'OFF';
FULLTEXT: 'FULLTEXT';
FILTER: 'FILTER';
PLAIN: 'PLAIN';
INDEX: 'INDEX';
STORAGE: 'STORAGE';
RETURNING: 'RETURNING';

DYNAMIC: 'DYNAMIC';
STRICT: 'STRICT';
IGNORED: 'IGNORED';

ARRAY: 'ARRAY';

ANALYZER: 'ANALYZER';
EXTENDS: 'EXTENDS';
TOKENIZER: 'TOKENIZER';
TOKEN_FILTERS: 'TOKEN_FILTERS';
CHAR_FILTERS: 'CHAR_FILTERS';

PARTITIONED: 'PARTITIONED';
PREPARE: 'PREPARE';

TRANSIENT: 'TRANSIENT';
PERSISTENT: 'PERSISTENT';

MATCH: 'MATCH';

GENERATED: 'GENERATED';
ALWAYS: 'ALWAYS';

USER: 'USER';
ROLE: 'ROLE';
GRANT: 'GRANT';
DENY: 'DENY';
REVOKE: 'REVOKE';
PRIVILEGES: 'PRIVILEGES';
SCHEMA: 'SCHEMA';

RETURN: 'RETURN';
SUMMARY: 'SUMMARY';

METADATA: 'METADATA';

PUBLICATION: 'PUBLICATION';
SUBSCRIPTION: 'SUBSCRIPTION';
CONNECTION: 'CONNECTION';
ENABLE: 'ENABLE';
DISABLE: 'DISABLE';

DECLARE: 'DECLARE';
CURSOR: 'CURSOR';
ASENSITIVE: 'ASENSITIVE';
INSENSITIVE: 'INSENSITIVE';
BINARY: 'BINARY';
NO: 'NO';
SCROLL: 'SCROLL';
HOLD: 'HOLD';
ABSOLUTE: 'ABSOLUTE';
FORWARD: 'FORWARD';
BACKWARD: 'BACKWARD';
RELATIVE: 'RELATIVE';
PRIOR: 'PRIOR';

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';
LLT  : '<<';
REGEX_MATCH: '~';
REGEX_NO_MATCH: '!~';
REGEX_MATCH_CI: '~*';
REGEX_NO_MATCH_CI: '!~*';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';
CAST_OPERATOR: '::';
SEMICOLON: ';';
COLON: ':';
COMMA: ',';
DOT: '.';
OPEN_ROUND_BRACKET: '(';
CLOSE_ROUND_BRACKET: ')';
OPEN_CURLY_BRACKET: '{';
CLOSE_CURLY_BRACKET: '}';
OPEN_SQUARE_BRACKET: '[';
CLOSE_SQUARE_BRACKET: ']';
EMPTY_SQUARE_BRACKET: '[]';
QUESTION: '?';
DOLLAR: '$';
BITWISE_AND: '&';
BITWISE_OR: '|';
BITWISE_XOR: '#';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

// Description of rules, in the order of the appearence in |:
// 1. Restrict single matches of \ or ' inside E'' (to be able to match specific combinations)
// 2. Allow matching double single quote '' since quote can be escaped by  a quote
// 3. Allow matching one or many backslashes followed by a non-slash character
ESCAPED_STRING
    : 'E' '\'' ( ~('\'' | '\\') | '\'\'' | ('\\'+  ~'\\') )* '\''
    ;

BIT_STRING
    : 'B' '\'' ([0-1])* '\''
    ;


INTEGER_VALUE
    : DIGIT+
    | DIGIT (DIGIT | '_')+ DIGIT
    ;

DECIMAL_VALUE
    : INTEGER_VALUE '.' INTEGER_VALUE?
    | '.' DIGIT+
    | INTEGER_VALUE ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@')*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_' | '@')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

BEGIN_DOLLAR_QUOTED_STRING
   : '$' TAG? '$'
   {pushTag();} -> pushMode (DollarQuotedStringMode)
   ;

fragment TAG
    : IDENTIFIER
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Za-z]
    ;

COMMENT
    : ('--' ~[\r\n]* '\r'? '\n'? | '/*' .*? '*/') -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

UNRECOGNIZED
    : .
    ;

mode DollarQuotedStringMode;
DOLLAR_QUOTED_STRING_BODY
   : ~ '$'+
   // | '$'([0-9])+
   // this alternative improves the efficiency of handling $ characters within a dollar-quoted string which are
   // not part of the ending tag.
   | '$' ~ '$'*
   ;

END_DOLLAR_QUOTED_STRING
   : ('$' TAG? '$')
   {isTag()}?
   {popTag();} -> popMode
   ;
