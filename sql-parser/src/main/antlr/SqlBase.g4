/*
 * Licensed to Crate.io Inc. or its affiliates ("Crate.io") under one or
 * more contributor license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Crate.io licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * However, if you have executed another commercial license agreement with
 * Crate.io these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

grammar SqlBase;

singleStatement
    : statement SEMICOLON? EOF
    ;

singleExpression
    : expr EOF
    ;

statement
    : query                                                                          #default
    | BEGIN                                                                          #begin
    | EXPLAIN statement                                                              #explain
    | OPTIMIZE TABLE tableWithPartitions withProperties?                             #optimize
    | REFRESH TABLE tableWithPartitions                                              #refreshTable
    | UPDATE aliasedRelation SET assignment (',' assignment)* where?                 #update
    | DELETE FROM aliasedRelation where?                                             #delete
    | SHOW TRANSACTION ISOLATION LEVEL                                               #showTransaction
    | SHOW CREATE TABLE table                                                        #showCreateTable
    | SHOW TABLES ((FROM | IN) qname)? (LIKE pattern=stringLiteral | where)?         #showTables
    | SHOW SCHEMAS (LIKE pattern=stringLiteral | where)?                             #showSchemas
    | SHOW COLUMNS (FROM | IN) tableName=qname ((FROM | IN) schema=qname)?
        (LIKE pattern=stringLiteral | where)?                                        #showColumns
    | ALTER TABLE alterTableDefinition ADD COLUMN? addColumnDefinition               #addColumn
    | ALTER TABLE alterTableDefinition
        (SET '(' genericProperties ')' | RESET ('(' ident (',' ident)* ')')?)        #alterTableProperties
    | ALTER BLOB TABLE alterTableDefinition
        (SET '(' genericProperties ')' | RESET ('(' ident (',' ident)* ')')?)        #alterBlobTableProperties
    | ALTER TABLE alterTableDefinition (OPEN | CLOSE)                                #alterTableOpenClose
    | ALTER TABLE alterTableDefinition RENAME TO qname                               #alterTableRename
    | ALTER TABLE alterTableDefinition REROUTE rerouteOption                         #alterTableReroute
    | ALTER CLUSTER REROUTE RETRY FAILED                                             #alterClusterRerouteRetryFailed
    | ALTER USER name=ident SET '(' genericProperties ')'                            #alterUser
    | RESET GLOBAL primaryExpression (',' primaryExpression)*                        #resetGlobal
    | SET SESSION CHARACTERISTICS AS TRANSACTION setExpr (setExpr)*                  #setSessionTransactionMode
    | SET (SESSION | LOCAL)? qname
        (EQ | TO) (DEFAULT | setExpr (',' setExpr)*)                                 #set
    | SET GLOBAL (PERSISTENT | TRANSIENT)?
        setGlobalAssignment (',' setGlobalAssignment)*                               #setGlobal
    | KILL (ALL | jobId=parameterOrString)                                           #kill
    | INSERT INTO table ('(' ident (',' ident)* ')')? insertSource
        (ON DUPLICATE KEY UPDATE assignment (',' assignment)*)?                      #insert
    | RESTORE SNAPSHOT qname (ALL | TABLE tableWithPartitions) withProperties?       #restore
    | COPY tableWithPartition FROM path=expr withProperties?                         #copyFrom
    | COPY tableWithPartition columns? where?
        TO DIRECTORY? path=expr withProperties?                                      #copyTo
    | DROP BLOB TABLE (IF EXISTS)? table                                             #dropBlobTable
    | DROP TABLE (IF EXISTS)? table                                                  #dropTable
    | DROP ALIAS qname                                                               #dropAlias
    | DROP REPOSITORY ident                                                          #dropRepository
    | DROP SNAPSHOT qname                                                            #dropSnapshot
    | DROP FUNCTION (IF EXISTS)? name=qname
        '(' (functionArgument (',' functionArgument)*)? ')'                          #dropFunction
    | DROP USER (IF EXISTS)? name=ident                                              #dropUser
    | DROP INGEST RULE (IF EXISTS)? rule_name=ident                                  #dropIngestRule
    | GRANT ( privilegeTypes | ALL (PRIVILEGES)? )
      (ON clazz  ( qname (',' qname)* ))?
      TO userNames                                                                   #grantPrivilege
    | DENY ( privilegeTypes | ALL (PRIVILEGES)? )
      (ON clazz  ( qname (',' qname)* ))?
      TO userNames                                                                   #denyPrivilege
    | REVOKE ( privilegeTypes | ALL (PRIVILEGES)? )
      (ON clazz   ( qname (',' qname)* ))?
      FROM userNames                                                                 #revokePrivilege
    | createStmt                                                                     #create
    ;

query:
      queryTerm
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=parameterOrInteger)?
      (OFFSET offset=parameterOrInteger)?
    ;

queryTerm
    : querySpec                                                                      #queryTermDefault
    | first=querySpec operator=(INTERSECT | EXCEPT) second=querySpec                 #setOperation
    | left=queryTerm operator=UNION setQuant? right=queryTerm                        #setOperation
    ;

setQuant
    : DISTINCT
    | ALL
    ;

sortItem
    : expr ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpec
    : SELECT setQuant? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      where?
      (GROUP BY expr (',' expr)*)?
      (HAVING having=booleanExpression)?
    ;

selectItem
    : expr (AS? ident)?                                                              #selectSingle
    | qname '.' ASTERISK                                                             #selectAll
    | ASTERISK                                                                       #selectAll
    ;

where
    : WHERE condition=booleanExpression
    ;

relation
    : left=relation
      ( CROSS JOIN right=aliasedRelation
      | joinType JOIN rightRelation=relation joinCriteria
      | NATURAL joinType JOIN right=aliasedRelation
      )                                                                              #joinRelation
    | aliasedRelation                                                                #relationDefault
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    | USING '(' ident (',' ident)* ')'
    ;

aliasedRelation
    : relationPrimary (AS? ident aliasedColumns?)?
    ;

relationPrimary
    : table                                                                          #tableName
    | '(' query ')'                                                                  #subqueryRelation
    | '(' relation ')'                                                               #parenthesizedRelation
    ;

tableWithPartition
    : qname ( PARTITION '(' assignment ( ',' assignment )* ')')?
    ;

table
    : qname
    | ident '(' valueExpression? (',' valueExpression)* ')'
    ;

aliasedColumns
    : '(' ident (',' ident)* ')'
    ;

expr
    : booleanExpression
    ;

privilegeTypes
    : ident (',' ident)*
    ;

booleanExpression
    : predicated                                                                     #booleanDefault
    | NOT booleanExpression                                                          #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression                    #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression                     #logicalBinary
    | MATCH '(' matchPredicateIdents ',' term=primaryExpression ')'
        (USING matchType=ident withProperties?)?                                     #match
    ;

predicated
    : valueExpression predicate[$valueExpression.ctx]?
    ;

predicate[ParserRuleContext value]
    : cmpOp right=valueExpression                                                    #comparison
    | cmpOp setCmpQuantifier parenthesizedPrimaryExpressionOrSubquery                #quantifiedComparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression                   #between
    | NOT? IN '(' expr (',' expr)* ')'                                               #inList
    | NOT? IN subqueryExpression                                                     #inSubquery
    | NOT? LIKE pattern=valueExpression (ESCAPE escape=valueExpression)?             #like
    | NOT? LIKE quant=setCmpQuantifier '(' v=valueExpression')'
        (ESCAPE escape=valueExpression)?                                             #arrayLike
    | IS NOT? NULL                                                                   #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                                    #distinctFrom
    ;

valueExpression
    : primaryExpression                                                              #valueExpressionDefault
    | operator=(MINUS | PLUS) valueExpression                                        #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT)
        right=valueExpression                                                        #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression             #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression                              #concatenation
    | valueExpression CAST_OPERATOR dataType                                         #doubleColonCast
    ;

primaryExpression
    : parameterOrLiteral                                                             #defaultParamOrLiteral
    | qname '(' ASTERISK ')'                                                         #functionCall
    | ident                                                                          #columnReference
    | qname '(' (setQuant? expr (',' expr)*)? ')'                                    #functionCall
    | subqueryExpression                                                             #subqueryExpressionDefault
    // This case handles a simple parenthesized expression.
    | '(' expr ')'                                                                   #nestedExpression
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS '(' query ')'                                                           #exists
    | value=primaryExpression '[' index=valueExpression ']'                          #subscript
    | ident ('.' ident)*                                                             #dereference
    | name=CURRENT_DATE                                                              #specialDateTimeFunction
    | name=CURRENT_TIME ('(' precision=integerLiteral')')?                           #specialDateTimeFunction
    | name=CURRENT_TIMESTAMP ('(' precision=integerLiteral')')?                      #specialDateTimeFunction
    | CURRENT_SCHEMA ('(' ')')?                                                      #currentSchema
    | (CURRENT_USER | USER)                                                          #currentUser
    | SESSION_USER                                                                   #sessionUser
    | SUBSTRING '(' expr FROM expr (FOR expr)? ')'                                   #substring
    | EXTRACT '(' stringLiteralOrIdentifier FROM expr ')'                            #extract
    | CAST '(' expr AS dataType ')'                                                  #cast
    | TRY_CAST '(' expr AS dataType ')'                                              #cast
    | CASE valueExpression whenClause+ (ELSE elseExpr=expr)? END                     #simpleCase
    | CASE whenClause+ (ELSE elseExpr=expr)? END                                     #searchedCase
    | IF '('condition=expr ',' trueValue=expr (',' falseValue=expr)? ')'             #ifCase
    ;

parenthesizedPrimaryExpressionOrSubquery
    : parenthesizedPrimaryExpression
    | subqueryExpression
    ;

subqueryExpression
    : '(' query ')'
    ;

parenthesizedPrimaryExpression
    : '(' primaryExpression ')'
    ;

identExpr
    : parameterOrSimpleLiteral
    | ident
    ;

parameterOrLiteral
    : parameterOrSimpleLiteral
    | datetimeLiteral
    | arrayLiteral
    | objectLiteral
    ;

parameterOrSimpleLiteral
    : nullLiteral
    | stringLiteral
    | numericLiteral
    | booleanLiteral
    | parameterExpr
    ;

parameterOrInteger
    : parameterExpr
    | integerLiteral
    ;

parameterOrIdent
    : parameterExpr
    | ident
    ;

parameterOrString
    : parameterExpr
    | stringLiteral
    ;

parameterExpr
    : '$' integerLiteral                                                             #positionalParameter
    | '?'                                                                            #parameterPlaceholder
    ;

nullLiteral
    : NULL
    ;

stringLiteral
    : STRING
    ;

subscriptSafe
    : value=subscriptSafe '[' index=valueExpression']'
    | qname
    ;

cmpOp
    : EQ | NEQ | LT | LTE | GT | GTE | REGEX_MATCH | REGEX_NO_MATCH | REGEX_MATCH_CI | REGEX_NO_MATCH_CI
    ;

setCmpQuantifier
    : ANY | SOME | ALL
    ;

datetimeLiteral
    : DATE STRING                                                                    #dateLiteral
    | TIME STRING                                                                    #timeLiteral
    | TIMESTAMP STRING                                                               #timestampLiteral
    ;

whenClause
    : WHEN condition=expr THEN result=expr
    ;

qname
    : ident ('.' ident)*
    ;

ident
    : IDENTIFIER                                                                     #unquotedIdentifier
    | quotedIdentifier                                                               #quotedIdentifierAlternative
    | nonReserved                                                                    #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER                                                          #backQuotedIdentifier
    | DIGIT_IDENTIFIER                                                               #digitIdentifier
    | COLON_IDENT                                                                    #colonIdentifier
    ;

quotedIdentifier
    : QUOTED_IDENTIFIER
    ;

stringLiteralOrIdentifier
    : ident
    | stringLiteral
    ;

numericLiteral
    : decimalLiteral
    | integerLiteral
    ;

booleanLiteral
    : TRUE
    | FALSE
    ;

decimalLiteral
    : DECIMAL_VALUE
    ;

integerLiteral
    : INTEGER_VALUE
    ;

arrayLiteral
    : ARRAY? '[' (expr (',' expr)*)? ']'
    ;

objectLiteral
    : '{' (objectKeyValue (',' objectKeyValue)*)? '}'
    ;

objectKeyValue
    : key=ident EQ value=expr
    ;

insertSource
   : VALUES  values (',' values)*
   | query
   | '(' query ')'
   ;

values
    : '(' expr (',' expr)* ')'
    ;

columns
    : '(' primaryExpression (',' primaryExpression)* ')'
    ;

assignment
    : primaryExpression EQ expr
    ;

createStmt
    : CREATE TABLE (IF NOT EXISTS)? table
        '(' tableElement (',' tableElement)* ')'
         crateTableOption* withProperties?                                           #createTable
    | CREATE BLOB TABLE table numShards=clusteredInto? withProperties?               #createBlobTable
    | CREATE REPOSITORY name=ident TYPE type=ident withProperties?                   #createRepository
    | CREATE SNAPSHOT qname (ALL | TABLE tableWithPartitions) withProperties?        #createSnapshot
    | CREATE ANALYZER name=ident (EXTENDS extendedName=ident)?
        WITH? '(' analyzerElement ( ',' analyzerElement )* ')'                       #createAnalyzer
    | CREATE (OR REPLACE)? FUNCTION name=qname
        '(' (functionArgument (',' functionArgument)*)? ')'
        RETURNS returnType=dataType
        LANGUAGE language=parameterOrIdent
        AS body=parameterOrString                                                    #createFunction
    | CREATE USER name=ident withProperties?                                         #createUser
    | CREATE ( OR REPLACE )? VIEW name=qname AS query                                #createView
    | CREATE INGEST RULE rule_name=ident
        ON source_ident=ident
        (where)?
        INTO table_ident=qname                                                       #createIngestRule
    ;

functionArgument
    : (name=ident)? type=dataType
    ;

alterTableDefinition
    : ONLY qname                                                                     #tableOnly
    | tableWithPartition                                                             #tableWithPartitionDefault
    ;

crateTableOption
    : PARTITIONED BY columns                                                         #partitionedBy
    | CLUSTERED (BY '(' routing=primaryExpression ')')?
        (INTO numShards=parameterOrInteger SHARDS)?                                  #clusteredBy
    ;

clusteredInto
    : CLUSTERED INTO numShards=parameterOrSimpleLiteral SHARDS
    ;

tableElement
    : columnDefinition                                                               #columndDefinitionDefault
    | PRIMARY_KEY columns                                                            #primaryKeyConstraint
    | INDEX name=ident USING method=ident columns withProperties?                    #indexDefinition
    ;

columnDefinition
    : generatedColumnDefinition
    | ident dataType columnConstraint*
    ;

generatedColumnDefinition
    : ident GENERATED ALWAYS AS generatedExpr=expr columnConstraint*
    | ident (dataType GENERATED ALWAYS)? AS generatedExpr=expr columnConstraint*
    ;

addColumnDefinition
    : addGeneratedColumnDefinition
    | subscriptSafe dataType columnConstraint*
    ;

addGeneratedColumnDefinition
    : subscriptSafe GENERATED ALWAYS AS generatedExpr=expr columnConstraint*
    | subscriptSafe (dataType GENERATED ALWAYS)? AS generatedExpr=expr columnConstraint*
    ;

rerouteOption
    : MOVE SHARD shardId=parameterOrInteger FROM fromNodeId=parameterOrString TO toNodeId=parameterOrString #rerouteMoveShard
    | ALLOCATE REPLICA SHARD shardId=parameterOrInteger ON nodeId=parameterOrString                         #rerouteAllocateReplicaShard
    | CANCEL SHARD shardId=parameterOrInteger ON nodeId=parameterOrString withProperties?                   #rerouteCancelShard
    ;

dataType
    : STRING_TYPE
    | BOOLEAN
    | BYTE
    | SHORT
    | INT
    | INTEGER
    | LONG
    | FLOAT
    | DOUBLE
    | TIMESTAMP
    | IP
    | GEO_POINT
    | GEO_SHAPE
    | objectTypeDefinition
    | arrayTypeDefinition
    | setTypeDefinition
    ;

objectTypeDefinition
    : OBJECT ('(' type=(DYNAMIC | STRICT | IGNORED) ')')?
        (AS '(' columnDefinition ( ',' columnDefinition )* ')')?
    ;

arrayTypeDefinition
    : ARRAY '(' dataType ')'
    ;

setTypeDefinition
    : SET '(' dataType ')'
    ;

columnConstraint
    : PRIMARY_KEY                                                                    #columnConstraintPrimaryKey
    | NOT NULL                                                                       #columnConstraintNotNull
    | INDEX USING method=ident withProperties?                                       #columnIndexConstraint
    | INDEX OFF                                                                      #columnIndexOff
    | STORAGE withProperties                                                         #columnStorageDefinition
    ;

withProperties
    : WITH '(' genericProperties ')'                                                 #withGenericProperties
    ;

genericProperties
    : genericProperty (',' genericProperty)*
    ;

genericProperty
    : ident EQ expr
    ;

matchPredicateIdents
    : matchPred=matchPredicateIdent
    | '(' matchPredicateIdent (',' matchPredicateIdent)* ')'
    ;

matchPredicateIdent
    : subscriptSafe boost=parameterOrSimpleLiteral?
    ;

analyzerElement
    : tokenizer
    | tokenFilters
    | charFilters
    | genericProperty
    ;

tokenizer
    : TOKENIZER namedProperties
    ;

tokenFilters
    : TOKEN_FILTERS '(' namedProperties (',' namedProperties )* ')'
    ;

charFilters
    : CHAR_FILTERS '(' namedProperties (',' namedProperties )* ')'
    ;

namedProperties
    : ident withProperties?
    ;

tableWithPartitions
    : tableWithPartition (',' tableWithPartition)*
    ;

setGlobalAssignment
    : name=primaryExpression (EQ | TO) value=expr
    ;

setExpr
    : stringLiteral
    | booleanLiteral
    | numericLiteral
    | ident
    | on
    ;

on
    : ON
    ;

userNames
    : ( ident (',' ident)* )
    ;

clazz
    : SCHEMA
    | TABLE
    ;

nonReserved
    : ALIAS | ANALYZER | BERNOULLI | BLOB | CATALOGS | CHAR_FILTERS | CLUSTERED
    | COLUMNS | COPY | CURRENT | DATE | DAY | DISTRIBUTED | DUPLICATE | DYNAMIC | EXPLAIN
    | EXTENDS | FOLLOWING | FORMAT | FULLTEXT | FUNCTIONS | GEO_POINT | GEO_SHAPE | GLOBAL
    | GRAPHVIZ | HOUR | IGNORED | KEY | KILL | LOGICAL | LOCAL | MATERIALIZED | MINUTE
    | MONTH | OFF | ONLY | OVER | OPTIMIZE | PARTITION | PARTITIONED | PARTITIONS | PLAIN
    | PRECEDING | RANGE | REFRESH | ROW | ROWS | SCHEMAS | SECOND | SESSION
    | SHARDS | SHOW | STORAGE | STRICT | SYSTEM | TABLES | TABLESAMPLE | TEXT | TIME
    | TIMESTAMP | TO | TOKENIZER | TOKEN_FILTERS | TYPE | VALUES | VIEW | YEAR
    | REPOSITORY | SNAPSHOT | RESTORE | GENERATED | ALWAYS | BEGIN
    | ISOLATION | TRANSACTION | CHARACTERISTICS | LEVEL | LANGUAGE | OPEN | CLOSE | RENAME
    | PRIVILEGES | SCHEMA | INGEST | RULE
    | REROUTE | MOVE | SHARD | ALLOCATE | REPLICA | CANCEL | CLUSTER | RETRY | FAILED
    ;

SELECT: 'SELECT';
FROM: 'FROM';
TO: 'TO';
AS: 'AS';
ALL: 'ALL';
ANY: 'ANY';
SOME: 'SOME';
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
IS: 'IS';
NULL: 'NULL';
TRUE: 'TRUE';
FALSE: 'FALSE';
NULLS: 'NULLS';
FIRST: 'FIRST';
LAST: 'LAST';
ESCAPE: 'ESCAPE';
ASC: 'ASC';
DESC: 'DESC';
SUBSTRING: 'SUBSTRING';
FOR: 'FOR';
DATE: 'DATE';
TIME: 'TIME';
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
PARTITION: 'PARTITION';
RANGE: 'RANGE';
ROWS: 'ROWS';
UNBOUNDED: 'UNBOUNDED';
PRECEDING: 'PRECEDING';
FOLLOWING: 'FOLLOWING';
CURRENT: 'CURRENT';
ROW: 'ROW';
WITH: 'WITH';
RECURSIVE: 'RECURSIVE';
CREATE: 'CREATE';
BLOB: 'BLOB';
TABLE: 'TABLE';
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
TIMESTAMP: 'TIMESTAMP';
IP: 'IP';
OBJECT: 'OBJECT';
STRING_TYPE: 'STRING';
GEO_POINT: 'GEO_POINT';
GEO_SHAPE: 'GEO_SHAPE';
GLOBAL : 'GLOBAL';
SESSION : 'SESSION';
LOCAL : 'LOCAL';
BEGIN: 'BEGIN';

RETURNS: 'RETURNS';
CALLED: 'CALLED';
REPLACE: 'REPLACE';
FUNCTION: 'FUNCTION';
LANGUAGE: 'LANGUAGE';
INPUT: 'INPUT';

CONSTRAINT: 'CONSTRAINT';
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
TRANSACTION: 'TRANSACTION';
CHARACTERISTICS: 'CHARACTERISTICS';
ISOLATION: 'ISOLATION';
LEVEL: 'LEVEL';
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
SET: 'SET';
RESET: 'RESET';
DEFAULT: 'DEFAULT';
COPY: 'COPY';
CLUSTERED: 'CLUSTERED';
SHARDS: 'SHARDS';
PRIMARY_KEY: 'PRIMARY KEY';
OFF: 'OFF';
FULLTEXT: 'FULLTEXT';
PLAIN: 'PLAIN';
INDEX: 'INDEX';
STORAGE: 'STORAGE';

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

TRANSIENT: 'TRANSIENT';
PERSISTENT: 'PERSISTENT';

MATCH: 'MATCH';

GENERATED: 'GENERATED';
ALWAYS: 'ALWAYS';

USER: 'USER';
GRANT: 'GRANT';
DENY: 'DENY';
REVOKE: 'REVOKE';
PRIVILEGES: 'PRIVILEGES';
SCHEMA: 'SCHEMA';

INGEST: 'INGEST';
RULE: 'RULE';

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';
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

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    | DIGIT+ ('.' DIGIT*)? EXPONENT
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

COLON_IDENT
    : (LETTER | DIGIT | '_' )+ ':' (LETTER | DIGIT | '_' )+
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
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

UNRECOGNIZED
    : .
    ;
