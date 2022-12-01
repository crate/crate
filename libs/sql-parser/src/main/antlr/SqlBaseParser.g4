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

parser grammar SqlBaseParser;

options { tokenVocab=SqlBaseLexer; } // use tokens from SqlBaseLexer.g4

statements
    : statement (SEMICOLON statement)* SEMICOLON? EOF
    ;

singleStatement
    : statement SEMICOLON? EOF
    ;

singleExpression
    : expr EOF
    ;

statement
    : query                                                                          #default
    | BEGIN (WORK | TRANSACTION)? (transactionMode (COMMA? transactionMode)*)?       #begin
    | START TRANSACTION (transactionMode (COMMA? transactionMode)*)?                 #startTransaction
    | COMMIT (WORK | TRANSACTION)?                                                   #commit
    | END (WORK | TRANSACTION)?                                                      #commit
    | EXPLAIN (ANALYZE)? statement                                                   #explain
    | OPTIMIZE TABLE tableWithPartitions withProperties?                             #optimize
    | REFRESH TABLE tableWithPartitions                                              #refreshTable
    | UPDATE aliasedRelation
        SET assignment (COMMA assignment)*
        where?
        returning?                                                                   #update
    | DELETE FROM aliasedRelation where?                                             #delete
    | SHOW (TRANSACTION ISOLATION LEVEL | TRANSACTION_ISOLATION)                     #showTransaction
    | SHOW CREATE TABLE table                                                        #showCreateTable
    | SHOW TABLES ((FROM | IN) qname)? (LIKE pattern=stringLiteral | where)?         #showTables
    | SHOW SCHEMAS (LIKE pattern=stringLiteral | where)?                             #showSchemas
    | SHOW COLUMNS (FROM | IN) tableName=qname ((FROM | IN) schema=qname)?
        (LIKE pattern=stringLiteral | where)?                                        #showColumns
    | SHOW (qname | ALL)                                                             #showSessionParameter
    | alterStmt                                                                      #alter
    | RESET GLOBAL primaryExpression (COMMA primaryExpression)*                      #resetGlobal
    | SET (SESSION CHARACTERISTICS AS)? TRANSACTION
        transactionMode (COMMA transactionMode)*                                     #setTransaction
    | SET (SESSION | LOCAL)? SESSION AUTHORIZATION
        (DEFAULT | username=stringLiteralOrIdentifier)                               #setSessionAuthorization
    | RESET SESSION AUTHORIZATION                                                    #resetSessionAuthorization
    | SET (SESSION | LOCAL)? qname
        (EQ | TO) (DEFAULT | setExpr (COMMA setExpr)*)                               #set
    | SET GLOBAL (PERSISTENT | TRANSIENT)?
        setGlobalAssignment (COMMA setGlobalAssignment)*                             #setGlobal
    | SET LICENSE stringLiteral                                                      #setLicense
    | SET TIME ZONE (LOCAL | DEFAULT | stringLiteral)                                #setTimeZone
    | KILL (ALL | jobId=parameterOrString)                                           #kill
    | INSERT INTO table
        (OPEN_ROUND_BRACKET ident (COMMA ident)* CLOSE_ROUND_BRACKET)?
        insertSource onConflict? returning?                                          #insert
    | RESTORE SNAPSHOT qname
        (ALL | METADATA | TABLE tableWithPartitions | metatypes=idents)
        withProperties?                                                              #restore
    | COPY tableWithPartition
        (OPEN_ROUND_BRACKET ident (COMMA ident)* CLOSE_ROUND_BRACKET)?
        FROM path=expr withProperties? (RETURN SUMMARY)?                             #copyFrom
    | COPY tableWithPartition columns? where?
        TO DIRECTORY? path=expr withProperties?                                      #copyTo
    | dropStmt                                                                       #drop
    | GRANT (priviliges=idents | ALL PRIVILEGES?)
        (ON clazz qnames)? TO users=idents                                           #grantPrivilege
    | DENY (priviliges=idents | ALL PRIVILEGES?)
        (ON clazz qnames)? TO users=idents                                           #denyPrivilege
    | REVOKE (privileges=idents | ALL PRIVILEGES?)
        (ON clazz qnames)? FROM users=idents                                         #revokePrivilege
    | createStmt                                                                     #create
    | DEALLOCATE (PREPARE)? (ALL | prepStmt=stringLiteralOrIdentifierOrQname)        #deallocate
    | ANALYZE                                                                        #analyze
    | DISCARD (ALL | PLANS | SEQUENCES | TEMPORARY | TEMP)                           #discard
    | DECLARE ident declareCursorParams
        CURSOR ((WITH | WITHOUT) HOLD)? FOR queryNoWith                              #declare
    | FETCH (direction)? (IN | FROM)? ident                                          #fetch
    | CLOSE (ident | ALL)                                                            #close
    ;

dropStmt
    : DROP BLOB TABLE (IF EXISTS)? table                                             #dropBlobTable
    | DROP TABLE (IF EXISTS)? table                                                  #dropTable
    | DROP ALIAS qname                                                               #dropAlias
    | DROP REPOSITORY ident                                                          #dropRepository
    | DROP SNAPSHOT qname                                                            #dropSnapshot
    | DROP FUNCTION (IF EXISTS)? name=qname
        OPEN_ROUND_BRACKET (functionArgument (COMMA functionArgument)*)?
        CLOSE_ROUND_BRACKET                                                          #dropFunction
    | DROP USER (IF EXISTS)? name=ident                                              #dropUser
    | DROP VIEW (IF EXISTS)? names=qnames                                            #dropView
    | DROP ANALYZER name=ident                                                       #dropAnalyzer
    | DROP PUBLICATION (IF EXISTS)? name=ident                                       #dropPublication
    | DROP SUBSCRIPTION (IF EXISTS)? name=ident                                      #dropSubscription
    ;

alterStmt
    : ALTER TABLE alterTableDefinition ADD COLUMN? addColumnDefinition               #addColumn
    | ALTER TABLE alterTableDefinition DROP CONSTRAINT ident                         #dropCheckConstraint
    | ALTER TABLE alterTableDefinition
        (SET OPEN_ROUND_BRACKET genericProperties CLOSE_ROUND_BRACKET
        | RESET (OPEN_ROUND_BRACKET ident (COMMA ident)* CLOSE_ROUND_BRACKET)?)      #alterTableProperties
    | ALTER BLOB TABLE alterTableDefinition
        (SET OPEN_ROUND_BRACKET genericProperties CLOSE_ROUND_BRACKET
        | RESET (OPEN_ROUND_BRACKET ident (COMMA ident)* CLOSE_ROUND_BRACKET)?)      #alterBlobTableProperties
    | ALTER (BLOB)? TABLE alterTableDefinition (OPEN | CLOSE)                        #alterTableOpenClose
    | ALTER (BLOB)? TABLE alterTableDefinition RENAME TO qname                       #alterTableRename
    | ALTER (BLOB)? TABLE alterTableDefinition REROUTE rerouteOption                 #alterTableReroute
    | ALTER CLUSTER REROUTE RETRY FAILED                                             #alterClusterRerouteRetryFailed
    | ALTER CLUSTER SWAP TABLE source=qname TO target=qname withProperties?          #alterClusterSwapTable
    | ALTER CLUSTER DECOMMISSION node=expr                                           #alterClusterDecommissionNode
    | ALTER CLUSTER GC DANGLING ARTIFACTS                                            #alterClusterGCDanglingArtifacts
    | ALTER USER name=ident
        SET OPEN_ROUND_BRACKET genericProperties CLOSE_ROUND_BRACKET                 #alterUser
    | ALTER PUBLICATION name=ident
        ((ADD | SET | DROP) TABLE qname '*'?  (COMMA qname '*'? )*)                  #alterPublication
    | ALTER SUBSCRIPTION name=ident alterSubscriptionMode                            #alterSubscription
    ;


queryOptParens
    : '(' query ')'
    | query
    | '(' queryOptParens ')'
    ;

query
    : with? queryNoWith
    ;

queryNoWith
    : queryTerm
      (ORDER BY sortItem (COMMA sortItem)*)?
      (limitClause? offsetClause? | offsetClause? limitClause?)
    ;

limitClause
    : LIMIT (limit=parameterOrInteger | ALL)
    | FETCH (FIRST | NEXT) (limit=parameterOrInteger) (ROW | ROWS) ONLY
    ;

offsetClause
    : OFFSET offset=parameterOrInteger (ROW | ROWS)?
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
    : SELECT setQuant? selectItem (COMMA selectItem)*
      (FROM relation (COMMA relation)*)?
      where?
      (GROUP BY expr (COMMA expr)*)?
      (HAVING having=booleanExpression)?
      (WINDOW windows+=namedWindow (COMMA windows+=namedWindow)*)?                   #defaultQuerySpec
    | VALUES values (COMMA values)*                                                  #valuesRelation
    ;

selectItem
    : expr (AS? ident)?                                                              #selectSingle
    | qname DOT ASTERISK                                                             #selectAll
    | ASTERISK                                                                       #selectAll
    ;

where
    : WHERE condition=booleanExpression
    ;

returning
    : RETURNING selectItem (COMMA selectItem)*
    ;

filter
    : FILTER OPEN_ROUND_BRACKET where CLOSE_ROUND_BRACKET
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
    | USING OPEN_ROUND_BRACKET ident (COMMA ident)* CLOSE_ROUND_BRACKET
    ;

aliasedRelation
    : relationPrimary (AS? ident aliasedColumns?)?
    ;

relationPrimary
    : table                                                                          #tableRelation
    | OPEN_ROUND_BRACKET query CLOSE_ROUND_BRACKET                                   #subqueryRelation
    | OPEN_ROUND_BRACKET relation CLOSE_ROUND_BRACKET                                #parenthesizedRelation
    ;

tableWithPartition
    : qname ( PARTITION OPEN_ROUND_BRACKET assignment ( COMMA assignment )* CLOSE_ROUND_BRACKET)?
    ;

table
    : qname                                                                          #tableName
    | qname OPEN_ROUND_BRACKET
        valueExpression? (COMMA valueExpression)* CLOSE_ROUND_BRACKET                #tableFunction
    ;

aliasedColumns
    : OPEN_ROUND_BRACKET ident (COMMA ident)* CLOSE_ROUND_BRACKET
    ;

with
    : WITH namedQuery (COMMA namedQuery)*
    ;

namedQuery
    : name=ident (aliasedColumns)? AS OPEN_ROUND_BRACKET query CLOSE_ROUND_BRACKET
    ;

expr
    : booleanExpression
    ;

booleanExpression
    : predicated                                                                     #booleanDefault
    | NOT booleanExpression                                                          #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression                    #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression                     #logicalBinary
    | MATCH OPEN_ROUND_BRACKET matchPredicateIdents
        COMMA term=primaryExpression CLOSE_ROUND_BRACKET
        (USING matchType=ident withProperties?)?                                     #match
    ;

predicated
    : valueExpression predicate[$valueExpression.ctx]?
    ;

predicate[ParserRuleContext value]
    : cmpOp right=valueExpression                                                    #comparison
    | cmpOp setCmpQuantifier primaryExpression                                       #quantifiedComparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression                   #between
    | NOT? IN OPEN_ROUND_BRACKET expr (COMMA expr)* CLOSE_ROUND_BRACKET              #inList
    | NOT? IN subqueryExpression                                                     #inSubquery
    | NOT? (LIKE | ILIKE) pattern=valueExpression (ESCAPE escape=valueExpression)?   #like
    | NOT? (LIKE | ILIKE) quant=setCmpQuantifier
        OPEN_ROUND_BRACKET v=valueExpression CLOSE_ROUND_BRACKET
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
    | left=valueExpression operator=(BITWISE_AND | BITWISE_OR | BITWISE_XOR)
        right=valueExpression                                                        #bitwiseBinary
    | left=valueExpression CONCAT right=valueExpression                              #concatenation
    | dataType stringLiteral                                                         #fromStringLiteralCast
    ;

primaryExpression
    : parameterOrLiteral                                                             #defaultParamOrLiteral
    | explicitFunction                                                               #explicitFunctionDefault
    | qname OPEN_ROUND_BRACKET ASTERISK CLOSE_ROUND_BRACKET filter? over?            #functionCall
    | ident                                                                          #columnReference
    | qname OPEN_ROUND_BRACKET (setQuant? expr (COMMA expr)*)? CLOSE_ROUND_BRACKET filter?
        ((IGNORE|RESPECT) NULLS)? over?                                              #functionCall
    | subqueryExpression                                                             #subqueryExpressionDefault
    | OPEN_ROUND_BRACKET base=primaryExpression CLOSE_ROUND_BRACKET
        DOT fieldName=ident                                                          #recordSubscript
    | OPEN_ROUND_BRACKET expr CLOSE_ROUND_BRACKET                                    #nestedExpression
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS OPEN_ROUND_BRACKET query CLOSE_ROUND_BRACKET                            #exists
    | value=primaryExpression
        OPEN_SQUARE_BRACKET index=valueExpression CLOSE_SQUARE_BRACKET               #subscript
    | base=primaryExpression
        OPEN_SQUARE_BRACKET (from=valueExpression)? COLON
        (to=valueExpression)? CLOSE_SQUARE_BRACKET                                   #arraySlice
    | ident (DOT ident)*                                                             #dereference
    | primaryExpression CAST_OPERATOR dataType                                       #doubleColonCast
    | timestamp=primaryExpression AT TIME ZONE zone=primaryExpression                #atTimezone
    | 'ARRAY'? EMPTY_SQUARE_BRACKET                                                  #emptyArray
    ;

explicitFunction
    : name=CURRENT_DATE                                                              #specialDateTimeFunction
    | name=CURRENT_TIME
        (OPEN_ROUND_BRACKET precision=integerLiteral CLOSE_ROUND_BRACKET)?           #specialDateTimeFunction
    | name=CURRENT_TIMESTAMP
        (OPEN_ROUND_BRACKET precision=integerLiteral CLOSE_ROUND_BRACKET)?           #specialDateTimeFunction
    | CURRENT_SCHEMA                                                                 #currentSchema
    | (CURRENT_USER | USER)                                                          #currentUser
    | SESSION_USER                                                                   #sessionUser
    | LEFT OPEN_ROUND_BRACKET strOrColName=expr COMMA len=expr CLOSE_ROUND_BRACKET   #left
    | RIGHT OPEN_ROUND_BRACKET strOrColName=expr COMMA len=expr CLOSE_ROUND_BRACKET  #right
    | SUBSTRING OPEN_ROUND_BRACKET expr FROM expr (FOR expr)? CLOSE_ROUND_BRACKET    #substring
    | TRIM OPEN_ROUND_BRACKET ((trimMode=(LEADING | TRAILING | BOTH))?
                (charsToTrim=expr)? FROM)? target=expr CLOSE_ROUND_BRACKET           #trim
    | EXTRACT OPEN_ROUND_BRACKET stringLiteralOrIdentifier FROM
        expr CLOSE_ROUND_BRACKET                                                     #extract
    | CAST OPEN_ROUND_BRACKET expr AS dataType CLOSE_ROUND_BRACKET                   #cast
    | TRY_CAST OPEN_ROUND_BRACKET expr AS dataType CLOSE_ROUND_BRACKET               #cast
    | CASE operand=expr whenClause+ (ELSE elseExpr=expr)? END                        #simpleCase
    | CASE whenClause+ (ELSE elseExpr=expr)? END                                     #searchedCase
    | IF OPEN_ROUND_BRACKET condition=expr COMMA trueValue=expr
        (COMMA falseValue=expr)? CLOSE_ROUND_BRACKET                                 #ifCase
    | ARRAY subqueryExpression                                                       #arraySubquery
    ;

subqueryExpression
    : OPEN_ROUND_BRACKET query CLOSE_ROUND_BRACKET
    ;

parameterOrLiteral
    : parameterOrSimpleLiteral                                                       #simpleLiteral
    | ARRAY? OPEN_SQUARE_BRACKET (expr (COMMA expr)*)?
        CLOSE_SQUARE_BRACKET                                                         #arrayLiteral
    | OPEN_CURLY_BRACKET (objectKeyValue (COMMA objectKeyValue)*)?
        CLOSE_CURLY_BRACKET                                                          #objectLiteral
    ;

parameterOrSimpleLiteral
    : nullLiteral
    | intervalLiteral
    | escapedCharsStringLiteral
    | stringLiteral
    | numericLiteral
    | booleanLiteral
    | bitString
    | parameterExpr
    ;

parameterOrInteger
    : parameterExpr
    | integerLiteral
    | nullLiteral
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
    : DOLLAR integerLiteral                                                          #positionalParameter
    | QUESTION                                                                       #parameterPlaceholder
    ;

nullLiteral
    : NULL
    ;

escapedCharsStringLiteral
    : ESCAPED_STRING
    ;

stringLiteral
    : STRING
    ;

bitString
    : BIT_STRING
    ;

subscriptSafe
    : value=subscriptSafe OPEN_SQUARE_BRACKET index=valueExpression CLOSE_SQUARE_BRACKET
    | qname
    ;

cmpOp
    : EQ | NEQ | LT | LTE | GT | GTE | LLT | REGEX_MATCH | REGEX_NO_MATCH | REGEX_MATCH_CI | REGEX_NO_MATCH_CI
    ;

setCmpQuantifier
    : ANY | SOME | ALL
    ;

whenClause
    : WHEN condition=expr THEN result=expr
    ;

namedWindow
    : name=ident AS windowDefinition
    ;

over
    : OVER windowDefinition
    ;

windowDefinition
    : windowRef=ident
    | OPEN_ROUND_BRACKET
        (windowRef=ident)?
        (PARTITION BY partition+=expr (COMMA partition+=expr)*)?
        (ORDER BY sortItem (COMMA sortItem)*)?
        windowFrame?
      CLOSE_ROUND_BRACKET
    ;

windowFrame
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=PRECEDING                 #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING                 #unboundedFrame
    | CURRENT ROW                                   #currentRowBound
    | expr boundType=(PRECEDING | FOLLOWING)        #boundedFrame
    ;

qnames
    : qname (COMMA qname)*
    ;

qname
    : ident (DOT ident)*
    ;

idents
    : ident (COMMA ident)*
    ;

ident
    : unquotedIdent
    | quotedIdent
    ;

unquotedIdent
    : IDENTIFIER                        #unquotedIdentifier
    | nonReserved                       #unquotedIdentifier
    | DIGIT_IDENTIFIER                  #digitIdentifier        // not supported
    ;

quotedIdent
    : QUOTED_IDENTIFIER                 #quotedIdentifier
    | BACKQUOTED_IDENTIFIER             #backQuotedIdentifier   // not supported
    ;

stringLiteralOrIdentifier
    : ident
    | stringLiteral
    ;

stringLiteralOrIdentifierOrQname
    : ident
    | qname
    | stringLiteral
    ;

numericLiteral
    : decimalLiteral
    | integerLiteral
    ;

intervalLiteral
    : INTERVAL sign=(PLUS | MINUS)? stringLiteral from=intervalField (TO to=intervalField)?
    ;

intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
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

objectKeyValue
    : key=ident EQ value=expr
    ;

insertSource
   : query
   | OPEN_ROUND_BRACKET query CLOSE_ROUND_BRACKET
   ;

onConflict
   : ON CONFLICT conflictTarget? DO NOTHING
   | ON CONFLICT conflictTarget DO UPDATE SET assignment (COMMA assignment)*
   ;

conflictTarget
   : OPEN_ROUND_BRACKET subscriptSafe (COMMA subscriptSafe)* CLOSE_ROUND_BRACKET
   ;

values
    : OPEN_ROUND_BRACKET expr (COMMA expr)* CLOSE_ROUND_BRACKET
    ;

columns
    : OPEN_ROUND_BRACKET primaryExpression (COMMA primaryExpression)* CLOSE_ROUND_BRACKET
    ;

assignment
    : primaryExpression EQ expr
    ;

createStmt
    : CREATE TABLE (IF NOT EXISTS)? table
        OPEN_ROUND_BRACKET tableElement (COMMA tableElement)* CLOSE_ROUND_BRACKET
         partitionedByOrClusteredInto withProperties?                                #createTable
    | CREATE TABLE table AS insertSource                                             #createTableAs
    | CREATE BLOB TABLE table numShards=blobClusteredInto? withProperties?           #createBlobTable
    | CREATE REPOSITORY name=ident TYPE type=ident withProperties?                   #createRepository
    | CREATE SNAPSHOT qname (ALL | TABLE tableWithPartitions) withProperties?        #createSnapshot
    | CREATE ANALYZER name=ident (EXTENDS extendedName=ident)?
        WITH? OPEN_ROUND_BRACKET analyzerElement
        ( COMMA analyzerElement )* CLOSE_ROUND_BRACKET                               #createAnalyzer
    | CREATE (OR REPLACE)? FUNCTION name=qname
        OPEN_ROUND_BRACKET (functionArgument (COMMA functionArgument)*)?
        CLOSE_ROUND_BRACKET
        RETURNS returnType=dataType
        LANGUAGE language=parameterOrIdent
        AS body=parameterOrString                                                    #createFunction
    | CREATE USER name=ident withProperties?                                         #createUser
    | CREATE ( OR REPLACE )? VIEW name=qname AS queryOptParens                       #createView
    | CREATE PUBLICATION name=ident
        (FOR ALL TABLES | FOR TABLE qname '*'?  (COMMA qname '*'? )*)?               #createPublication
    | CREATE SUBSCRIPTION name=ident CONNECTION conninfo=expr
          PUBLICATION publications=idents
          withProperties?                                                            #createSubscription
    ;


functionArgument
    : (name=ident)? type=dataType
    ;

alterTableDefinition
    : ONLY qname                                                                     #tableOnly
    | tableWithPartition                                                             #tableWithPartitionDefault
    ;

alterSubscriptionMode
    : ENABLE
    | DISABLE
    ;

partitionedByOrClusteredInto
    : partitionedBy? clusteredBy?
    | clusteredBy? partitionedBy?
    ;

partitionedBy
    : PARTITIONED BY columns
    ;

clusteredBy
    : CLUSTERED (BY OPEN_ROUND_BRACKET routing=primaryExpression CLOSE_ROUND_BRACKET)?
        (INTO numShards=parameterOrInteger SHARDS)?
    ;

blobClusteredInto
    : CLUSTERED INTO numShards=parameterOrInteger SHARDS
    ;

tableElement
    : columnDefinition                                                               #columnDefinitionDefault
    | PRIMARY_KEY columns                                                            #primaryKeyConstraint
    | INDEX name=ident USING method=ident columns withProperties?                    #indexDefinition
    | checkConstraint                                                                #tableCheckConstraint
    ;

columnDefinition
    : ident dataType? (DEFAULT defaultExpr=expr)? ((GENERATED ALWAYS)? AS generatedExpr=expr)? columnConstraint*
    ;

addColumnDefinition
    : subscriptSafe dataType? ((GENERATED ALWAYS)? AS expr)? columnConstraint*
    ;

rerouteOption
    : MOVE SHARD shardId=parameterOrInteger FROM fromNodeId=parameterOrString TO toNodeId=parameterOrString #rerouteMoveShard
    | ALLOCATE REPLICA SHARD shardId=parameterOrInteger ON nodeId=parameterOrString                         #rerouteAllocateReplicaShard
    | PROMOTE REPLICA SHARD shardId=parameterOrInteger ON nodeId=parameterOrString withProperties?          #reroutePromoteReplica
    | CANCEL SHARD shardId=parameterOrInteger ON nodeId=parameterOrString withProperties?                   #rerouteCancelShard
    ;

dataType
    : baseDataType
        (OPEN_ROUND_BRACKET integerLiteral (COMMA integerLiteral )* CLOSE_ROUND_BRACKET)?    #maybeParametrizedDataType
    | objectTypeDefinition                                  #objectDataType
    | ARRAY OPEN_ROUND_BRACKET dataType CLOSE_ROUND_BRACKET                                #arrayDataType
    | dataType '[]'                                         #arrayDataType
    ;

baseDataType
    : definedDataType   #definedDataTypeDefault
    | ident             #identDataType
    ;

definedDataType
    : DOUBLE PRECISION
    | TIMESTAMP WITHOUT TIME ZONE
    | TIMESTAMP WITH TIME ZONE
    | TIME WITH TIME ZONE
    | CHARACTER VARYING
    | CHAR_SPECIAL
    ;

objectTypeDefinition
    : OBJECT (OPEN_ROUND_BRACKET type=(DYNAMIC | STRICT | IGNORED) CLOSE_ROUND_BRACKET)?
        (AS OPEN_ROUND_BRACKET columnDefinition ( COMMA columnDefinition )* CLOSE_ROUND_BRACKET)?
    ;

columnConstraint
    : PRIMARY_KEY                                                                    #columnConstraintPrimaryKey
    | NOT NULL                                                                       #columnConstraintNotNull
    | INDEX USING method=ident withProperties?                                       #columnIndexConstraint
    | INDEX OFF                                                                      #columnIndexOff
    | STORAGE withProperties                                                         #columnStorageDefinition
    | checkConstraint                                                                #columnCheckConstraint
    ;

checkConstraint
    : (CONSTRAINT name=ident)? CHECK
        OPEN_ROUND_BRACKET expression=booleanExpression CLOSE_ROUND_BRACKET
    ;

withProperties
    : WITH OPEN_ROUND_BRACKET genericProperties CLOSE_ROUND_BRACKET                  #withGenericProperties
    ;

genericProperties
    : genericProperty (COMMA genericProperty)*
    ;

genericProperty
    : ident EQ expr
    ;

matchPredicateIdents
    : matchPred=matchPredicateIdent
    | OPEN_ROUND_BRACKET matchPredicateIdent (COMMA matchPredicateIdent)*
        CLOSE_ROUND_BRACKET
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
    : TOKEN_FILTERS OPEN_ROUND_BRACKET namedProperties (COMMA namedProperties )*
        CLOSE_ROUND_BRACKET
    ;

charFilters
    : CHAR_FILTERS OPEN_ROUND_BRACKET namedProperties (COMMA namedProperties )*
        CLOSE_ROUND_BRACKET
    ;

namedProperties
    : ident withProperties?
    ;

tableWithPartitions
    : tableWithPartition (COMMA tableWithPartition)*
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

clazz
    : SCHEMA
    | TABLE
    | VIEW
    ;

transactionMode
    : ISOLATION LEVEL isolationLevel
    | (READ WRITE | READ ONLY)
    | (NOT)? DEFERRABLE
    ;

isolationLevel
    : SERIALIZABLE
    | REPEATABLE READ
    | READ COMMITTED
    | READ UNCOMMITTED
    ;

direction
    : NEXT
    | PRIOR
    | FIRST
    | LAST
    | ABSOLUTE integerLiteral
    | RELATIVE integerLiteral
    | integerLiteral
    | ALL
    | FORWARD
    | FORWARD integerLiteral
    | FORWARD ALL
    | BACKWARD
    | BACKWARD integerLiteral
    | BACKWARD ALL
    ;

// https://www.postgresql.org/docs/current/sql-declare.html
// The key words ASENSITIVE, BINARY, INSENSITIVE, and SCROLL can appear in any order.
declareCursorParams
    : (ASENSITIVE | BINARY | INSENSITIVE | (NO)? SCROLL)*
    ;

nonReserved
    : ALIAS | ANALYZE | ANALYZER | AT | AUTHORIZATION | BERNOULLI | BLOB | CATALOGS | CHAR_FILTERS | CHECK | CLUSTERED
    | COLUMNS | COPY | CURRENT |  DAY | DEALLOCATE | DISTRIBUTED | DUPLICATE | DYNAMIC | EXPLAIN
    | EXTENDS | FETCH | FOLLOWING | FORMAT | FULLTEXT | FUNCTIONS | GEO_POINT | GEO_SHAPE | GLOBAL
    | GRAPHVIZ | HOUR | IGNORED | IGNORE | RESPECT | ILIKE | INTERVAL | KEY | KILL | LICENSE | LOGICAL | LOCAL
    | MATERIALIZED | MINUTE | MONTH | NEXT | OFF | ONLY | OVER | OPTIMIZE | PARTITION | PARTITIONED
    | PARTITIONS | PLAIN | PRECEDING | RANGE | REFRESH | ROW | ROWS | SCHEMAS | SECOND | SESSION
    | SHARDS | SHOW | STORAGE | STRICT | SYSTEM | TABLES | TABLESAMPLE | TEXT | TIME | ZONE | WITHOUT
    | TIMESTAMP | TO | TOKENIZER | TOKEN_FILTERS | TYPE | VALUES | VIEW | YEAR
    | REPOSITORY | SNAPSHOT | RESTORE | GENERATED | ALWAYS | BEGIN | START | COMMIT
    | ISOLATION | TRANSACTION | CHARACTERISTICS | LEVEL | LANGUAGE | OPEN | CLOSE | RENAME
    | PRIVILEGES | SCHEMA | PREPARE
    | REROUTE | MOVE | SHARD | ALLOCATE | REPLICA | CANCEL | CLUSTER | RETRY | FAILED | FILTER
    | DO | NOTHING | CONFLICT | TRANSACTION_ISOLATION | RETURN | SUMMARY
    | WORK | SERIALIZABLE | REPEATABLE | COMMITTED | UNCOMMITTED | READ | WRITE | WINDOW | DEFERRABLE
    | STRING_TYPE | IP | DOUBLE | FLOAT | TIMESTAMP | LONG | INT | INTEGER | SHORT | BYTE | BOOLEAN | PRECISION
    | REPLACE | RETURNING | SWAP | GC | DANGLING | ARTIFACTS | DECOMMISSION | LEADING | TRAILING | BOTH | TRIM
    | CURRENT_SCHEMA | PROMOTE | CHARACTER | VARYING | SUBSTRING
    | DISCARD | PLANS | SEQUENCES | TEMPORARY | TEMP | METADATA
    | PUBLICATION | SUBSCRIPTION | ENABLE | DISABLE | CONNECTION | DECLARE | CURSOR | HOLD | FORWARD | BACKWARD
    | RELATIVE | PRIOR | ASENSITIVE | INSENSITIVE | BINARY | NO | SCROLL | ABSOLUTE
    ;
