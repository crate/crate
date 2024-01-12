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
    | EXPLAIN (ANALYZE | VERBOSE | explainOptions*) statement                        #explain
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
        (ON securable qnames)? TO users=idents                                       #grantPrivilege
    | DENY (priviliges=idents | ALL PRIVILEGES?)
        (ON securable qnames)? TO users=idents                                       #denyPrivilege
    | REVOKE (privileges=idents | ALL PRIVILEGES?)
        (ON securable qnames)? FROM users=idents                                     #revokePrivilege
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
    | DROP (USER | ROLE) (IF EXISTS)? name=ident                                     #dropRole
    | DROP VIEW (IF EXISTS)? names=qnames                                            #dropView
    | DROP ANALYZER name=ident                                                       #dropAnalyzer
    | DROP PUBLICATION (IF EXISTS)? name=ident                                       #dropPublication
    | DROP SUBSCRIPTION (IF EXISTS)? name=ident                                      #dropSubscription
    ;

alterStmt
    : ALTER TABLE alterTableDefinition addColumnDefinition
      (COMMA addColumnDefinition)*                                                   #addColumn
    | ALTER TABLE alterTableDefinition dropColumnDefinition
      (COMMA dropColumnDefinition)*                                                  #dropColumn
    | ALTER TABLE alterTableDefinition DROP CONSTRAINT ident                         #dropCheckConstraint
    | ALTER TABLE alterTableDefinition
        (SET OPEN_ROUND_BRACKET genericProperties CLOSE_ROUND_BRACKET
        | RESET (OPEN_ROUND_BRACKET ident (COMMA ident)* CLOSE_ROUND_BRACKET)?)      #alterTableProperties
    | ALTER BLOB TABLE alterTableDefinition
        (SET OPEN_ROUND_BRACKET genericProperties CLOSE_ROUND_BRACKET
        | RESET (OPEN_ROUND_BRACKET ident (COMMA ident)* CLOSE_ROUND_BRACKET)?)      #alterBlobTableProperties
    | ALTER (BLOB)? TABLE alterTableDefinition (OPEN | CLOSE)                        #alterTableOpenClose
    | ALTER (BLOB)? TABLE alterTableDefinition RENAME TO qname                       #alterTableRenameTable
    | ALTER (BLOB)? TABLE alterTableDefinition
        RENAME COLUMN? source=subscriptSafe TO target=subscriptSafe                  #alterTableRenameColumn
    | ALTER (BLOB)? TABLE alterTableDefinition REROUTE rerouteOption                 #alterTableReroute
    | ALTER CLUSTER REROUTE RETRY FAILED                                             #alterClusterRerouteRetryFailed
    | ALTER CLUSTER SWAP TABLE source=qname TO target=qname withProperties?          #alterClusterSwapTable
    | ALTER CLUSTER DECOMMISSION node=expr                                           #alterClusterDecommissionNode
    | ALTER CLUSTER GC DANGLING ARTIFACTS                                            #alterClusterGCDanglingArtifacts
    | ALTER (USER | ROLE) name=ident
        SET OPEN_ROUND_BRACKET genericProperties CLOSE_ROUND_BRACKET                 #alterRole
    | ALTER PUBLICATION name=ident
        ((ADD | SET | DROP) TABLE qname ASTERISK?  (COMMA qname ASTERISK? )*)        #alterPublication
    | ALTER SUBSCRIPTION name=ident alterSubscriptionMode                            #alterSubscription
    ;


queryOptParens
    : OPEN_ROUND_BRACKET query CLOSE_ROUND_BRACKET
    | query
    | OPEN_ROUND_BRACKET queryOptParens CLOSE_ROUND_BRACKET
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
    | NOT? (LIKE | ILIKE) pattern=valueExpression
        (ESCAPE escape=parameterOrLiteral)?                                          #like
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
    | ARRAY? EMPTY_SQUARE_BRACKET                                                    #emptyArray
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
    : parameterExpr                                                                  #parameterExpression
    | integerLiteral                                                                 #intAsLiteral
    | nullLiteral                                                                    #nullAsLiteral
    | parameterOrLiteral CAST_OPERATOR dataType                                      #integerParamOrLiteralDoubleColonCast
    | CAST OPEN_ROUND_BRACKET expr AS dataType CLOSE_ROUND_BRACKET                   #integerParamOrLiteralCast
    | TRY_CAST OPEN_ROUND_BRACKET expr AS dataType CLOSE_ROUND_BRACKET               #integerParamOrLiteralCast
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

dollarQuotedStringLiteral
    : BEGIN_DOLLAR_QUOTED_STRING DOLLAR_QUOTED_STRING_BODY* END_DOLLAR_QUOTED_STRING
    ;

stringLiteral
    : STRING
    | dollarQuotedStringLiteral
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

spaceSeparatedIdents
    : identWithOrWithoutValue (identWithOrWithoutValue)*
    ;

identWithOrWithoutValue
    : ident (parameterOrSimpleLiteral)?
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
    | CREATE (USER | ROLE) name=ident ((withProperties | WITH?
        OPEN_ROUND_BRACKET? options=spaceSeparatedIdents CLOSE_ROUND_BRACKET?))?     #createRole
    | CREATE ( OR REPLACE )? VIEW name=qname AS queryOptParens                       #createView
    | CREATE PUBLICATION name=ident
        (FOR ALL TABLES | FOR TABLE qname ASTERISK?  (COMMA qname ASTERISK? )*)?     #createPublication
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
    | primaryKeyContraint columns                                                    #primaryKeyConstraintTableLevel
    | INDEX name=ident USING method=ident columns withProperties?                    #indexDefinition
    | checkConstraint                                                                #tableCheckConstraint
    ;

columnDefinition
    : ident dataType? columnConstraint*
    ;

addColumnDefinition
    : ADD COLUMN? subscriptSafe dataType? columnConstraint*
    ;

dropColumnDefinition
    : DROP COLUMN? (IF EXISTS)? subscriptSafe
    ;

rerouteOption
    : MOVE SHARD shardId=parameterOrInteger FROM fromNodeId=parameterOrString TO toNodeId=parameterOrString #rerouteMoveShard
    | ALLOCATE REPLICA SHARD shardId=parameterOrInteger ON nodeId=parameterOrString                         #rerouteAllocateReplicaShard
    | PROMOTE REPLICA SHARD shardId=parameterOrInteger ON nodeId=parameterOrString withProperties?          #reroutePromoteReplica
    | CANCEL SHARD shardId=parameterOrInteger ON nodeId=parameterOrString withProperties?                   #rerouteCancelShard
    ;

dataType
    : baseDataType
        (OPEN_ROUND_BRACKET integerLiteral (COMMA integerLiteral )* CLOSE_ROUND_BRACKET)?  #maybeParametrizedDataType
    | objectTypeDefinition                                                                 #objectDataType
    | ARRAY OPEN_ROUND_BRACKET dataType CLOSE_ROUND_BRACKET                                #arrayDataType
    | dataType EMPTY_SQUARE_BRACKET                                                        #arrayDataType
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
    : primaryKeyContraint                                                            #columnConstraintPrimaryKey
    | NOT NULL                                                                       #columnConstraintNotNull
    | NULL																			 #columnConstraintNull
    | INDEX USING method=ident withProperties?                                       #columnIndexConstraint
    | INDEX OFF                                                                      #columnIndexOff
    | STORAGE withProperties                                                         #columnStorageDefinition
    | (CONSTRAINT name=ident)? DEFAULT defaultExpr=expr                              #columnDefaultConstraint
    | (CONSTRAINT name=ident)? (GENERATED ALWAYS)? AS generatedExpr=expr             #columnGeneratedConstraint
    | checkConstraint                                                                #columnCheckConstraint
    ;

primaryKeyContraint
    : (CONSTRAINT name=ident)? PRIMARY_KEY
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

explainOptions
   : OPEN_ROUND_BRACKET (explainOption) (COMMA explainOption)* CLOSE_ROUND_BRACKET
   ;

explainOption
   : (ANALYZE | COSTS | VERBOSE) booleanLiteral?
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

securable
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
    | RELATIVE (MINUS)? integerLiteral
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
    : ABSOLUTE
    | ALIAS
    | ALLOCATE
    | ALWAYS
    | ANALYZE
    | ANALYZER
    | ARTIFACTS
    | ASENSITIVE
    | AT
    | AUTHORIZATION
    | BACKWARD
    | BEGIN
    | BERNOULLI
    | BINARY
    | BLOB
    | BOOLEAN
    | BOTH
    | BYTE
    | CANCEL
    | CATALOGS
    | CHARACTER
    | CHARACTERISTICS
    | CHAR_FILTERS
    | CHECK
    | CLOSE
    | CLUSTER
    | CLUSTERED
    | COLUMNS
    | COMMIT
    | COMMITTED
    | CONFLICT
    | CONNECTION
    | COPY
    | CURRENT
    | CURRENT_DATE
    | CURRENT_SCHEMA
    | CURRENT_TIME
    | CURRENT_TIMESTAMP
    | CURSOR
    | DANGLING
    | DAY
    | DEALLOCATE
    | DECLARE
    | DECOMMISSION
    | DEFERRABLE
    | DISABLE
    | DISCARD
    | DISTRIBUTED
    | DO
    | DOUBLE
    | DUPLICATE
    | DYNAMIC
    | ENABLE
    | EXPLAIN
    | EXTENDS
    | FAILED
    | FETCH
    | FILTER
    | FLOAT
    | FOLLOWING
    | FORMAT
    | FORWARD
    | FULLTEXT
    | FUNCTIONS
    | GC
    | GENERATED
    | GEO_POINT
    | GEO_SHAPE
    | GLOBAL
    | GRAPHVIZ
    | HOLD
    | HOUR
    | IGNORE
    | IGNORED
    | ILIKE
    | INSENSITIVE
    | INT
    | INTEGER
    | INTERVAL
    | IP
    | ISOLATION
    | KEY
    | KILL
    | LANGUAGE
    | LEADING
    | LEVEL
    | LOCAL
    | LOGICAL
    | LONG
    | MATERIALIZED
    | METADATA
    | MINUTE
    | MONTH
    | MOVE
    | NEXT
    | NO
    | NOTHING
    | OFF
    | ONLY
    | OPEN
    | OPTIMIZE
    | OVER
    | PARTITION
    | PARTITIONED
    | PARTITIONS
    | PLAIN
    | PLANS
    | PRECEDING
    | PRECISION
    | PREPARE
    | PRIOR
    | PRIVILEGES
    | PROMOTE
    | PUBLICATION
    | RANGE
    | READ
    | REFRESH
    | RELATIVE
    | RENAME
    | REPEATABLE
    | REPLACE
    | REPLICA
    | REPOSITORY
    | REROUTE
    | RESPECT
    | RESTORE
    | RETRY
    | RETURN
    | RETURNING
    | ROLE
    | ROW
    | ROWS
    | SCHEMA
    | SCHEMAS
    | SCROLL
    | SECOND
    | SEQUENCES
    | SERIALIZABLE
    | SESSION
    | SHARD
    | SHARDS
    | SHORT
    | SHOW
    | SNAPSHOT
    | START
    | STORAGE
    | STRICT
    | STRING_TYPE
    | SUBSCRIPTION
    | SUBSTRING
    | SUMMARY
    | SWAP
    | SYSTEM
    | TABLES
    | TABLESAMPLE
    | TEMP
    | TEMPORARY
    | TEXT
    | TIME
    | TIMESTAMP
    | TIMESTAMP
    | TO
    | TOKEN_FILTERS
    | TOKENIZER
    | TRAILING
    | TRANSACTION
    | TRANSACTION_ISOLATION
    | TRIM
    | TYPE
    | UNCOMMITTED
    | VALUES
    | VARYING
    | VERBOSE
    | VIEW
    | WINDOW
    | WITHOUT
    | WORK
    | WRITE
    | YEAR
    | ZONE
    ;
