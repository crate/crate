/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

grammar Statement;

options {
    output = AST;
    ASTLabelType = CommonTree;
    memoize=true;
}

tokens {
    LEXER_ERROR;
    TERMINATOR;
    STATEMENT_LIST;
    GROUP_BY;
    ORDER_BY;
    SORT_ITEM;
    QUERY;
    WITH_LIST;
    WITH_QUERY;
    ALL_COLUMNS;
    SELECT_LIST;
    SELECT_ITEM;
    ALIASED_COLUMNS;
    TABLE_SUBQUERY;
    EXPLAIN_OPTIONS;
    EXPLAIN_FORMAT;
    EXPLAIN_TYPE;
    TABLE;
    JOINED_TABLE;
    QUALIFIED_JOIN;
    CROSS_JOIN;
    INNER_JOIN;
    LEFT_JOIN;
    RIGHT_JOIN;
    FULL_JOIN;
    COMPARE;
    IS_NULL;
    IS_NOT_NULL;
    IS_DISTINCT_FROM;
    IN_LIST;
    SIMPLE_CASE;
    SEARCHED_CASE;
    FUNCTION_CALL;
    WINDOW;
    PARTITION_BY;
    UNBOUNDED_PRECEDING;
    UNBOUNDED_FOLLOWING;
    CURRENT_ROW;
    NEGATIVE;
    QNAME;
    SHOW_TABLES;
    SHOW_SCHEMAS;
    SHOW_CATALOGS;
    SHOW_COLUMNS;
    SHOW_PARTITIONS;
    SHOW_FUNCTIONS;
    ALTER_TABLE;
    ALTER_BLOB_TABLE;
    CREATE_TABLE;
    CREATE_BLOB_TABLE;
    CREATE_MATERIALIZED_VIEW;
    REFRESH_MATERIALIZED_VIEW;
    VIEW_REFRESH;
    CREATE_ALIAS;
    DROP_ALIAS;
    DROP_TABLE;
    DROP_BLOB_TABLE;
    TABLE_ELEMENT_LIST;
    COLUMN_DEF;
    NOT_NULL;
    ALIASED_RELATION;
    SAMPLED_RELATION;
    QUERY_SPEC;
    STRATIFY_ON;
    IDENT_LIST;
    COLUMN_LIST;
    INSERT_VALUES;
    VALUES_LIST;
    ASSIGNMENT;
    ASSIGNMENT_LIST;
    COPY;
    INDEX_COLUMNS;
    GENERIC_PROPERTIES;
    GENERIC_PROPERTY;
    LITERAL_LIST;
    OBJECT_COLUMNS;
    INDEX_OFF;
    ANALYZER_ELEMENTS;
    NAMED_PROPERTIES;
}

@header {
    package io.crate.sql.parser;
}

@lexer::header {
    package io.crate.sql.parser;
}

@members {
    @Override
    protected Object recoverFromMismatchedToken(IntStream input, int tokenType, BitSet follow)
            throws RecognitionException
    {
        throw new MismatchedTokenException(tokenType, input);
    }

    @Override
    public Object recoverFromMismatchedSet(IntStream input, RecognitionException e, BitSet follow)
            throws RecognitionException
    {
        throw e;
    }

    @Override
    public String getErrorMessage(RecognitionException e, String[] tokenNames)
    {
        if (e.token.getType() == BACKQUOTED_IDENT) {
            return "backquoted identifiers are not supported; use double quotes to quote identifiers";
        }
        if (e.token.getType() == DIGIT_IDENT) {
            return "identifiers must not start with a digit; surround the identifier with double quotes";
        }
        if (e.token.getType() == COLON_IDENT) {
            return "identifiers must not contain a colon; use '@' instead of ':' for table links";
        }
        return super.getErrorMessage(e, tokenNames);
    }
}

@lexer::members {
    @Override
    public void reportError(RecognitionException e)
    {
        throw new ParsingException(getErrorMessage(e, getTokenNames()), e);
    }
}

@rulecatch {
    catch (RecognitionException re) {
        throw new ParsingException(getErrorMessage(re, getTokenNames()), re);
    }
}


singleStatement
    : statement EOF -> statement
    ;

singleExpression
    : expr EOF -> expr
    ;

statement
    : query
    | explainStmt
    | showTablesStmt
    | showSchemasStmt
    | showCatalogsStmt
    | showColumnsStmt
    | showPartitionsStmt
    | showFunctionsStmt
    | createTableStmt
    | alterBlobTableStmt
    | alterTableStmt
    | createBlobTableStmt
    | dropTableStmt
    | dropBlobTableStmt
    | createMaterializedViewStmt
    | refreshMaterializedViewStmt
    | createAliasStmt
    | dropAliasStmt
    | insertStmt
    | deleteStmt
    | updateStmt
    | copyStmt
    | createAnalyzerStmt
    | refreshStmt
    ;

query
    : queryExpr -> ^(QUERY queryExpr)
    ;

queryExpr
    : withClause?
      ( (orderOrLimitOrOffsetQuerySpec) => orderOrLimitOrOffsetQuerySpec
      | queryExprBody orderClause? limitClause? offsetClause?
      )
    ;

orderOrLimitOrOffsetQuerySpec
    : simpleQuery (orderClause limitClause? offsetClause? | limitClause offsetClause? | offsetClause) -> ^(QUERY_SPEC simpleQuery orderClause? limitClause? offsetClause?)
    ;

queryExprBody
    : ( queryTerm -> queryTerm )
      ( UNION setQuant? queryTerm       -> ^(UNION $queryExprBody queryTerm setQuant?)
      | EXCEPT setQuant? queryTerm      -> ^(EXCEPT $queryExprBody queryTerm setQuant?)
      )*
    ;

queryTerm
    : ( queryPrimary -> queryPrimary )
      ( INTERSECT setQuant? queryPrimary -> ^(INTERSECT $queryTerm queryPrimary setQuant?) )*
    ;

queryPrimary
    : simpleQuery -> ^(QUERY_SPEC simpleQuery)
    | tableSubquery
    | explicitTable
    ;

explicitTable
    : TABLE table -> table
    ;

simpleQuery
    : selectClause
      fromClause?
      whereClause?
      groupClause?
      havingClause?
    ;

restrictedSelectStmt
    : selectClause
      fromClause
    ;

withClause
    : WITH r=RECURSIVE? withList -> ^(WITH $r? withList)
    ;

selectClause
    : SELECT selectExpr -> ^(SELECT selectExpr)
    ;

fromClause
    : FROM tableRef (',' tableRef)* -> ^(FROM tableRef+)
    ;

whereClause
    : WHERE expr -> ^(WHERE expr)
    ;

groupClause
    : GROUP BY expr (',' expr)* -> ^(GROUP_BY expr+)
    ;

havingClause
    : HAVING expr -> ^(HAVING expr)
    ;

orderClause
    : ORDER BY sortItem (',' sortItem)* -> ^(ORDER_BY sortItem+)
    ;

limitClause
    : LIMIT integer -> ^(LIMIT integer)
    | LIMIT parameterExpr -> ^(LIMIT parameterExpr)
    ;

offsetClause
    : OFFSET integer -> ^(OFFSET integer)
    | OFFSET parameterExpr -> ^(OFFSET parameterExpr)
    ;

withList
    : withQuery (',' withQuery)* -> ^(WITH_LIST withQuery+)
    ;

withQuery
    : ident aliasedColumns? AS subquery -> ^(WITH_QUERY ident subquery aliasedColumns?)
    ;

selectExpr
    : setQuant? selectList
    ;

setQuant
    : DISTINCT
    | ALL
    ;

selectList
    : selectSublist (',' selectSublist)* -> ^(SELECT_LIST selectSublist+)
    ;

selectSublist
    : expr (AS? ident)? -> ^(SELECT_ITEM expr ident?)
    | qname '.' '*'     -> ^(ALL_COLUMNS qname)
    | '*'               -> ALL_COLUMNS
    ;

tableRef
    : ( tableFactor -> tableFactor )
      ( CROSS JOIN tableFactor                 -> ^(CROSS_JOIN $tableRef tableFactor)
      | joinType JOIN tableFactor joinCriteria -> ^(QUALIFIED_JOIN joinType joinCriteria $tableRef tableFactor)
      | NATURAL joinType JOIN tableFactor      -> ^(QUALIFIED_JOIN joinType NATURAL $tableRef tableFactor)
      )*
    ;

sampleType
    : BERNOULLI
    | SYSTEM
    ;

stratifyOn
    : STRATIFY ON '(' expr (',' expr)* ')' -> ^(STRATIFY_ON expr+)
    ;

tableFactor
    : ( tablePrimary -> tablePrimary )
      ( TABLESAMPLE sampleType '(' expr ')' stratifyOn? -> ^(SAMPLED_RELATION $tableFactor sampleType expr stratifyOn?) )?
    ;

tablePrimary
    : ( relation -> relation )
      ( AS? ident aliasedColumns? -> ^(ALIASED_RELATION $tablePrimary ident aliasedColumns?) )?
    ;

relation
    : table
    | ('(' tableRef ')') => joinedTable
    | tableSubquery
    ;

table
    : qname -> ^(TABLE qname)
    ;

tableSubquery
    : '(' query ')' -> ^(TABLE_SUBQUERY query)
    ;

joinedTable
    : '(' tableRef ')' -> ^(JOINED_TABLE tableRef)
    ;

joinType
    : INNER?       -> INNER_JOIN
    | LEFT OUTER?  -> LEFT_JOIN
    | RIGHT OUTER? -> RIGHT_JOIN
    | FULL OUTER?  -> FULL_JOIN
    ;

joinCriteria
    : ON expr                          -> ^(ON expr)
    | USING '(' ident (',' ident)* ')' -> ^(USING ident+)
    ;

aliasedColumns
    : '(' ident (',' ident)* ')' -> ^(ALIASED_COLUMNS ident+)
    ;

expr
    : orExpression
    ;

orExpression
    : andExpression (OR^ andExpression)*
    ;

andExpression
    : notExpression (AND^ notExpression)*
    ;

notExpression
    : (NOT^)* booleanTest
    ;

booleanTest
    : booleanPrimary
    ;

booleanPrimary
    : predicate
    | EXISTS subquery -> ^(EXISTS subquery)
    ;

predicate
    : (predicatePrimary -> predicatePrimary)
      ( cmpOp e=predicatePrimary                                  -> ^(cmpOp $predicate $e)
      | IS DISTINCT FROM e=predicatePrimary                       -> ^(IS_DISTINCT_FROM $predicate $e)
      | IS NOT DISTINCT FROM e=predicatePrimary                   -> ^(NOT ^(IS_DISTINCT_FROM $predicate $e))
      | BETWEEN min=predicatePrimary AND max=predicatePrimary     -> ^(BETWEEN $predicate $min $max)
      | NOT BETWEEN min=predicatePrimary AND max=predicatePrimary -> ^(NOT ^(BETWEEN $predicate $min $max))
      | LIKE e=predicatePrimary (ESCAPE x=predicatePrimary)?      -> ^(LIKE $predicate $e $x?)
      | NOT LIKE e=predicatePrimary (ESCAPE x=predicatePrimary)?  -> ^(NOT ^(LIKE $predicate $e $x?))
      | IS NULL                                                   -> ^(IS_NULL $predicate)
      | IS NOT NULL                                               -> ^(IS_NOT_NULL $predicate)
      | IN inList                                                 -> ^(IN $predicate inList)
      | NOT IN inList                                             -> ^(NOT ^(IN $predicate inList))
      )*
    ;

predicatePrimary
    : (subscript -> subscript)
      ( '||' e=subscript -> ^(FUNCTION_CALL ^(QNAME IDENT["concat"]) $predicatePrimary $e) )*
    ;

subscript
    : numericExpr ('['^ numericExpr ']'!)*
    ;

numericExpr
    : numericTerm (('+' | '-')^ numericTerm)*
    ;

numericTerm
    : numericFactor (('*' | '/' | '%')^ numericFactor)*
    ;

numericFactor
    : '+'? exprPrimary -> exprPrimary
    | '-' exprPrimary  -> ^(NEGATIVE exprPrimary)
    ;

exprPrimary
    : NULL
    | (dateValue) => dateValue
    | (intervalValue) => intervalValue
    | qnameOrFunction
    | specialFunction
    | number
    | parameterExpr
    | bool
    | STRING
    | caseExpression
    | ('(' expr ')') => ('(' expr ')' -> expr)
    | subquery
    ;

qnameOrFunction
    : (qname -> qname)
      ( ('(' '*' ')' over?                          -> ^(FUNCTION_CALL $qnameOrFunction over?))
      | ('(' setQuant? expr? (',' expr)* ')' over?  -> ^(FUNCTION_CALL $qnameOrFunction over? setQuant? expr*))
      )?
    ;

numericLiteral
    : '+'? number -> number
    | '-' number  -> ^(NEGATIVE number)
    ;

parameterExpr
    : '$' integer
    | '?'
    ;

literalOrParameter
    : NULL
    | numericLiteral
    | parameterExpr
    | bool
    | STRING
    ;


inList
    : ('(' expr) => ('(' expr (',' expr)* ')' -> ^(IN_LIST expr+))
    | subquery
    ;

sortItem
    : expr ordering nullOrdering? -> ^(SORT_ITEM expr ordering nullOrdering?)
    ;

ordering
    : -> ASC
    | ASC
    | DESC
    ;

nullOrdering
    : NULLS FIRST -> FIRST
    | NULLS LAST  -> LAST
    ;

cmpOp
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

subquery
    : '(' query ')' -> query
    ;

dateValue
    : DATE STRING      -> ^(DATE STRING)
    | TIME STRING      -> ^(TIME STRING)
    | TIMESTAMP STRING -> ^(TIMESTAMP STRING)
    ;

intervalValue
    : INTERVAL intervalSign? STRING intervalQualifier -> ^(INTERVAL STRING intervalQualifier intervalSign?)
    ;

intervalSign
    : '+' ->
    | '-' -> NEGATIVE
    ;

intervalQualifier
    : nonSecond ('(' integer ')')?                 -> ^(nonSecond integer?)
    | SECOND ('(' p=integer (',' s=integer)? ')')? -> ^(SECOND $p? $s?)
    ;

nonSecond
    : YEAR | MONTH | DAY | HOUR | MINUTE
    ;

specialFunction
    : CURRENT_DATE
    | CURRENT_TIME ('(' integer ')')?              -> ^(CURRENT_TIME integer?)
    | CURRENT_TIMESTAMP ('(' integer ')')?         -> ^(CURRENT_TIMESTAMP integer?)
    | SUBSTRING '(' expr FROM expr (FOR expr)? ')' -> ^(FUNCTION_CALL ^(QNAME IDENT["substr"]) expr expr expr?)
    | EXTRACT '(' ident FROM expr ')'              -> ^(EXTRACT ident expr)
    | CAST '(' expr AS ident ')'                    -> ^(CAST expr ident)
    ;


caseExpression
    : NULLIF '(' expr ',' expr ')'          -> ^(NULLIF expr expr)
    | COALESCE '(' expr (',' expr)* ')'     -> ^(COALESCE expr+)
    | CASE expr whenClause+ elseClause? END -> ^(SIMPLE_CASE expr whenClause+ elseClause?)
    | CASE whenClause+ elseClause? END      -> ^(SEARCHED_CASE whenClause+ elseClause?)
    | IF '(' expr ',' expr (',' expr)? ')'  -> ^(IF expr expr expr?)
    ;

whenClause
    : WHEN expr THEN expr -> ^(WHEN expr expr)
    ;

elseClause
    : ELSE expr -> expr
    ;

over
    : OVER '(' window ')' -> window
    ;

window
    : p=windowPartition? o=orderClause? f=windowFrame? -> ^(WINDOW $p? $o ?$f?)
    ;

windowPartition
    : PARTITION BY expr (',' expr)* -> ^(PARTITION_BY expr+)
    ;

windowFrame
    : RANGE frameBound                        -> ^(RANGE frameBound)
    | ROWS frameBound                         -> ^(ROWS frameBound)
    | RANGE BETWEEN frameBound AND frameBound -> ^(RANGE frameBound frameBound)
    | ROWS BETWEEN frameBound AND frameBound  -> ^(ROWS frameBound frameBound)
    ;

frameBound
    : UNBOUNDED PRECEDING -> UNBOUNDED_PRECEDING
    | UNBOUNDED FOLLOWING -> UNBOUNDED_FOLLOWING
    | CURRENT ROW         -> CURRENT_ROW
    | expr
      ( PRECEDING -> ^(PRECEDING expr)
      | FOLLOWING -> ^(FOLLOWING expr)
      )
    ;

explainStmt
    : EXPLAIN explainOptions? statement -> ^(EXPLAIN explainOptions? statement)
    ;

explainOptions
    : '(' explainOption (',' explainOption)* ')' -> ^(EXPLAIN_OPTIONS explainOption+)
    ;

explainOption
    : FORMAT TEXT      -> ^(EXPLAIN_FORMAT TEXT)
    | FORMAT GRAPHVIZ  -> ^(EXPLAIN_FORMAT GRAPHVIZ)
    | TYPE LOGICAL     -> ^(EXPLAIN_TYPE LOGICAL)
    | TYPE DISTRIBUTED -> ^(EXPLAIN_TYPE DISTRIBUTED)
    ;

showTablesStmt
    : SHOW TABLES from=showTablesFrom? like=showTablesLike? -> ^(SHOW_TABLES $from? $like?)
    ;

showTablesFrom
    : (FROM | IN) qname -> ^(FROM qname)
    ;

showTablesLike
    : LIKE s=STRING -> ^(LIKE $s)
    ;

showSchemasStmt
    : SHOW SCHEMAS from=showSchemasFrom? -> ^(SHOW_SCHEMAS $from?)
    ;

showSchemasFrom
    : (FROM | IN) ident -> ^(FROM ident)
    ;

showCatalogsStmt
    : SHOW CATALOGS -> SHOW_CATALOGS
    ;

showColumnsStmt
    : SHOW COLUMNS (FROM | IN) qname -> ^(SHOW_COLUMNS qname)
    | DESCRIBE qname                 -> ^(SHOW_COLUMNS qname)
    | DESC qname                     -> ^(SHOW_COLUMNS qname)
    ;

showPartitionsStmt
    : SHOW PARTITIONS (FROM | IN) qname w=whereClause? o=orderClause? l=limitClause? oc=offsetClause? -> ^(SHOW_PARTITIONS qname $w? $o? $l? $oc?)
    ;

showFunctionsStmt
    : SHOW FUNCTIONS -> SHOW_FUNCTIONS
    ;

dropBlobTableStmt
    : DROP BLOB TABLE table -> ^(DROP_BLOB_TABLE table)
    ;

dropTableStmt
    : DROP TABLE table -> ^(DROP_TABLE table)
    ;

createMaterializedViewStmt
    : CREATE MATERIALIZED VIEW qname r=viewRefresh? AS s=restrictedSelectStmt -> ^(CREATE_MATERIALIZED_VIEW qname $r? $s)
    ;

refreshMaterializedViewStmt
    : REFRESH MATERIALIZED VIEW qname -> ^(REFRESH_MATERIALIZED_VIEW qname)
    ;

viewRefresh
    : REFRESH r=integer -> ^(REFRESH $r)
    ;

createAliasStmt
    : CREATE ALIAS qname forRemote -> ^(CREATE_ALIAS qname forRemote)
    ;

dropAliasStmt
    : DROP ALIAS qname -> ^(DROP_ALIAS qname)
    ;

forRemote
    : FOR qname -> ^(FOR qname)
    ;

tableContentsSource
    : AS query -> query
    ;

qname
    : ident ('.' ident)* -> ^(QNAME ident+)
    ;

ident
    : IDENT
    | QUOTED_IDENT
    | nonReserved  -> IDENT[$nonReserved.text]
    ;

number
    : DECIMAL_VALUE
    | INTEGER_VALUE
    ;

bool
    : TRUE
    | FALSE
    ;

integer
    : INTEGER_VALUE
    ;


insertStmt
    : INSERT INTO table (columns=identList)? VALUES values=insertValues -> ^(INSERT table $values $columns?)
    ;

identList
    : '(' ident ( ',' ident )* ')' -> ^(IDENT_LIST ident+)
    ;

columnList
    : '(' subscript ( ',' subscript )* ')' -> ^(COLUMN_LIST subscript+)
    ;

insertValues
    : valuesList ( ',' valuesList )* -> ^(INSERT_VALUES valuesList+)
    ;

valuesList
    : '(' expr (',' expr)* ')' -> ^(VALUES_LIST expr+)
    ;


deleteStmt
    : DELETE FROM table whereClause? -> ^(DELETE table whereClause?)
    ;


updateStmt
    : UPDATE table SET assignmentList whereClause? -> ^(UPDATE table assignmentList whereClause?)
    ;

assignmentList
    : assignment ( ',' assignment )* -> ^(ASSIGNMENT_LIST assignment+)
    ;

assignment
    : subscript EQ expr -> ^(ASSIGNMENT subscript expr)
    ;


copyStmt
    : COPY table columnList? toFrom DIRECTORY? expr
      ( WITH '(' genericProperties ')' )? -> ^(COPY toFrom DIRECTORY? table columnList? expr genericProperties?)
    ;

toFrom
    : FROM
    | TO
    ;

alterBlobTableStmt
    : ALTER BLOB TABLE table
      SET '(' genericProperties ')' -> ^(ALTER_BLOB_TABLE table genericProperties)
    | ALTER BLOB TABLE table
      RESET identList -> ^(ALTER_BLOB_TABLE table identList)
    ;

alterTableStmt
    : ALTER TABLE table
      SET '(' genericProperties ')' -> ^(ALTER_TABLE table genericProperties)
    | ALTER TABLE table
      RESET identList -> ^(ALTER_TABLE table identList)
    ;

createBlobTableStmt
    : CREATE BLOB TABLE table clusteredInto?
      (WITH '(' genericProperties ')' )? -> ^(CREATE_BLOB_TABLE table clusteredInto? genericProperties?)
    ;

createTableStmt
    : CREATE TABLE table
      tableElementList
      crateTableOption*
      (WITH '(' genericProperties ')' )? -> ^(CREATE_TABLE table tableElementList crateTableOption* genericProperties?)
    ;

crateTableOption
    : clusteredBy
    | partitionedBy
    ;

tableElementList
    : '(' tableElement (',' tableElement)* ')' -> ^(TABLE_ELEMENT_LIST tableElement+)
    ;


tableElement
    :   columnDefinition
    |   indexDefinition
    |   primaryKeyConstraint
    ;

columnDefinition
    : ident dataType columnConstDef* -> ^(COLUMN_DEF ident dataType columnConstDef*)
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
    | objectTypeDefinition
    | arrayTypeDefinition
    | setTypeDefinition
    ;

objectTypeDefinition
    : OBJECT ( '(' objectType ')' )? objectColumns? -> ^(OBJECT objectType? objectColumns? )
    ;

arrayTypeDefinition
    : ARRAY '(' dataType ')' -> ^(ARRAY dataType)
    ;

setTypeDefinition
    : SET '(' dataType ')' -> ^(SET dataType)
    ;


objectType
    : DYNAMIC
    | STRICT
    | IGNORED
    ;

objectColumns
    : AS '(' columnDefinition ( ',' columnDefinition )* ')' -> ^(OBJECT_COLUMNS columnDefinition+)
    ;

columnConstDef
    : columnConst -> ^(CONSTRAINT columnConst)
    ;

columnConst
    : PRIMARY_KEY
    | columnIndexConstraint
    ;

columnIndexConstraint
    : INDEX USING indexMethod=ident (WITH '(' genericProperties ')' )? -> ^(INDEX $indexMethod genericProperties?)
    | INDEX OFF                                                        -> INDEX_OFF
    ;

indexDefinition
    : INDEX ident USING indexMethod=ident columnList (WITH '(' genericProperties ')' )? -> ^(INDEX ident $indexMethod columnList genericProperties?)
    ;

genericProperties
    :  genericProperty ( ',' genericProperty )* -> ^(GENERIC_PROPERTIES genericProperty+)
    ;

genericProperty
    : ident EQ literalOrParameter -> ^(GENERIC_PROPERTY ident literalOrParameter)
    | ident EQ literalList -> ^(GENERIC_PROPERTY ident literalList)
    ;

literalList
    : '[' literalOrParameter (',' literalOrParameter)* ']' -> ^(LITERAL_LIST literalOrParameter+)
    ;

primaryKeyConstraint
    : PRIMARY_KEY columnList -> ^(PRIMARY_KEY columnList)
    ;

clusteredInto
    : CLUSTERED INTO integer SHARDS -> ^(CLUSTERED integer)
    ;

clusteredBy
    : CLUSTERED (BY '(' subscript ')' )? (INTO integer SHARDS)? -> ^(CLUSTERED subscript? integer?)
    ;

partitionedBy
    : PARTITIONED BY columnList -> ^(PARTITIONED columnList)
    ;


createAnalyzerStmt
    : CREATE ANALYZER ident extendsAnalyzer? analyzerElementList -> ^(ANALYZER ident extendsAnalyzer? analyzerElementList)
    ;

extendsAnalyzer
    : EXTENDS ident -> ^(EXTENDS ident)
    ;

analyzerElementList
    : WITH? '(' analyzerElement ( ',' analyzerElement )* ')' -> ^(ANALYZER_ELEMENTS analyzerElement+)
    ;

analyzerElement
    : tokenizer
    | tokenFilters
    | charFilters
    | genericProperty
    ;

tokenizer
    : TOKENIZER namedProperties -> ^(TOKENIZER namedProperties)
    ;

tokenFilters
    : TOKEN_FILTERS '(' namedProperties (',' namedProperties )* ')' -> ^(TOKEN_FILTERS namedProperties+)
    ;

charFilters
    : CHAR_FILTERS '(' namedProperties (',' namedProperties )* ')' -> ^(CHAR_FILTERS namedProperties+)
    ;

namedProperties
    : ident (WITH '(' genericProperties ')' )? -> ^(NAMED_PROPERTIES ident genericProperties?)
    ;

refreshStmt
    : REFRESH TABLE table -> ^(REFRESH table)
    ;

nonReserved
    : SHOW | TABLES | COLUMNS | PARTITIONS | FUNCTIONS | SCHEMAS | CATALOGS
    | OVER | PARTITION | RANGE | ROWS | PRECEDING | FOLLOWING | CURRENT | ROW
    | REFRESH | MATERIALIZED | VIEW | ALIAS
    | DATE | TIME | TIMESTAMP | INTERVAL
    | YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    | EXPLAIN | FORMAT | TYPE | TEXT | GRAPHVIZ | LOGICAL | DISTRIBUTED
    | TABLESAMPLE | SYSTEM | BERNOULLI
    | DYNAMIC | STRICT | IGNORED
    | PLAIN | FULLTEXT | OFF
    | SHARDS | CLUSTERED | COPY | ANALYZER
    | EXTENDS | TOKENIZER | CHAR_FILTERS | TOKEN_FILTERS | BLOB
    | TO | PARTITIONED
    ;

SELECT: 'SELECT';
FROM: 'FROM';
TO: 'TO';
AS: 'AS';
ALL: 'ALL';
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
INTERVAL: 'INTERVAL';
YEAR: 'YEAR';
MONTH: 'MONTH';
DAY: 'DAY';
HOUR: 'HOUR';
MINUTE: 'MINUTE';
SECOND: 'SECOND';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
EXTRACT: 'EXTRACT';
COALESCE: 'COALESCE';
NULLIF: 'NULLIF';
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
ALTER: 'ALTER';

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
SHOW: 'SHOW';
TABLES: 'TABLES';
SCHEMAS: 'SCHEMAS';
CATALOGS: 'CATALOGS';
COLUMNS: 'COLUMNS';
PARTITIONS: 'PARTITIONS';
FUNCTIONS: 'FUNCTIONS';
MATERIALIZED: 'MATERIALIZED';
VIEW: 'VIEW';
REFRESH: 'REFRESH';
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
SET: 'SET';
RESET: 'RESET';
COPY: 'COPY';
CLUSTERED: 'CLUSTERED';
SHARDS: 'SHARDS';
PRIMARY_KEY: 'PRIMARY KEY';
OFF: 'OFF';
FULLTEXT: 'FULLTEXT';
PLAIN: 'PLAIN';
INDEX: 'INDEX';

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


EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
        { setText(getText().substring(1, getText().length() - 1).replace("''", "'")); }
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

IDENT
    : (LETTER | '_') (LETTER | DIGIT | '_' | '\@')*
    ;

DIGIT_IDENT
    : DIGIT (LETTER | DIGIT | '_' | '\@')+
    ;

QUOTED_IDENT
    : '"' ( ~'"' | '""' )* '"'
        { setText(getText().substring(1, getText().length() - 1).replace("\"\"", "\"")); }
    ;

BACKQUOTED_IDENT
    : '`' ( ~'`' | '``' )* '`'
        { setText(getText().substring(1, getText().length() - 1).replace("``", "`")); }
    ;

COLON_IDENT
    : (LETTER | DIGIT | '_' )+ ':' (LETTER | DIGIT | '_' )+
    ;

fragment EXPONENT
    : 'E' ('+' | '-')? DIGIT+
    ;

fragment DIGIT
    : '0'..'9'
    ;

fragment LETTER
    : 'A'..'Z'
    ;

COMMENT
    : '--' (~('\r' | '\n'))* ('\r'? '\n')?     { $channel=HIDDEN; }
    | '/*' (options {greedy=false;} : .)* '*/' { $channel=HIDDEN; }
    ;

WS
    : (' ' | '\t' | '\n' | '\r')+ { $channel=HIDDEN; }
    ;
