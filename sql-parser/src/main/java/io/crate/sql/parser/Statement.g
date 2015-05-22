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
    ADD_COLUMN;
    COLUMN_DEF;
    NESTED_COLUMN_DEF;
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
    COPY_FROM;
    COPY_TO;
    INDEX_COLUMNS;
    GENERIC_PROPERTIES;
    GENERIC_PROPERTY;
    LITERAL_LIST;
    OBJECT_COLUMNS;
    INDEX_OFF;
    ANALYZER_ELEMENTS;
    NAMED_PROPERTIES;
    ARRAY_CMP;
    ARRAY_LIKE;
    ARRAY_NOT_LIKE;
    ARRAY_LITERAL;
    OBJECT_LITERAL;
    ON_DUP_KEY;
    KEY_VALUE;
    MATCH;
    MATCH_PREDICATE_IDENT;
    MATCH_PREDICATE_IDENT_LIST;
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
//    | showTablesStmt
//    | showSchemasStmt
//    | showCatalogsStmt
//    | showColumnsStmt
//    | showPartitionsStmt
//    | showFunctionsStmt
    | CREATE createStatement -> createStatement
//    | createMaterializedViewStmt
    | ALTER alterStatement -> alterStatement
    | DROP dropStatement -> dropStatement
//    | refreshMaterializedViewStmt
    | insertStmt
    | deleteStmt
    | updateStmt
    | COPY copyStatement -> copyStatement
    | refreshStmt
    | setStmt
    | resetStmt
    | killStmt
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

tableWithPartition
    : qname ( PARTITION '(' assignmentList ')' )? -> ^(TABLE qname assignmentList?)
    ;

table
    : qname -> ^(TABLE qname)
    ;

tableOnly
    : ONLY qname -> ^(TABLE qname ONLY)
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
    : (MATCH) => matchPredicate -> matchPredicate
    | ((predicatePrimary -> predicatePrimary)
      ( cmpOp quant=setCmpQuantifier '(' e=predicatePrimary ')'       -> ^(ARRAY_CMP $predicate cmpOp $quant $e)
      | (LIKE setCmpQuantifier) => LIKE quant=setCmpQuantifier '(' e=predicatePrimary ')' (ESCAPE x=predicatePrimary)?          -> ^(ARRAY_LIKE $predicate $quant $e $x?)
      | LIKE e=predicatePrimary (ESCAPE x=predicatePrimary)?          -> ^(LIKE $predicate $e $x?)
      | (NOT LIKE setCmpQuantifier) => NOT LIKE quant=setCmpQuantifier '(' e=predicatePrimary ')' (ESCAPE x=predicatePrimary)?  -> ^(ARRAY_NOT_LIKE $predicate $quant $e $x?)
      | NOT LIKE e=predicatePrimary (ESCAPE x=predicatePrimary)?      -> ^(NOT ^(LIKE $predicate $e $x?))
      | cmpOp e=predicatePrimary                                      -> ^(cmpOp $predicate $e)
      | IS DISTINCT FROM e=predicatePrimary                           -> ^(IS_DISTINCT_FROM $predicate $e)
      | IS NOT DISTINCT FROM e=predicatePrimary                       -> ^(NOT ^(IS_DISTINCT_FROM $predicate $e))
      | BETWEEN min=predicatePrimary AND max=predicatePrimary         -> ^(BETWEEN $predicate $min $max)
      | NOT BETWEEN min=predicatePrimary AND max=predicatePrimary     -> ^(NOT ^(BETWEEN $predicate $min $max))
      | IS NULL                                                       -> ^(IS_NULL $predicate)
      | IS NOT NULL                                                   -> ^(IS_NOT_NULL $predicate)
      | IN inList                                                     -> ^(IN $predicate inList)
      | NOT IN inList                                                 -> ^(NOT ^(IN $predicate inList))
      )*)
    ;

matchPredicate
    : MATCH '(' matchPredicateIdentList ',' s=parameterOrSimpleLiteral ')' (USING matchMethod=ident ((WITH '(') => WITH '(' genericProperties ')' )?)? -> ^(MATCH matchPredicateIdentList $s $matchMethod? genericProperties?)
    ;

matchPredicateIdentList
    : ('(' matchPredicateIdent) => '(' matchPredicateIdent (',' matchPredicateIdent)* ')' -> ^(MATCH_PREDICATE_IDENT_LIST matchPredicateIdent+)
    | matchPredicateIdent  -> ^(MATCH_PREDICATE_IDENT_LIST matchPredicateIdent+)
    ;

matchPredicateIdent
    : subscriptSafe parameterOrSimpleLiteral? -> ^(MATCH_PREDICATE_IDENT subscriptSafe parameterOrSimpleLiteral?)
    ;


predicatePrimary
    : (numericExpr -> numericExpr)
      ( '||' e=numericExpr -> ^(FUNCTION_CALL ^(QNAME IDENT["concat"]) $predicatePrimary $e) )*
    ;


numericExpr
    : numericTerm (('+' | '-')^ numericTerm)*
    ;

numericTerm
    : numericFactor (('*' | '/' | '%')^ numericFactor)*
    ;

numericFactor
    : '+'? subscript -> subscript
    | '-' subscript  -> ^(NEGATIVE subscript)
    ;

subscript
    : exprPrimary ('['^ numericExpr ']'!)*
    ;

subscriptSafe
    : qname ('['^ numericExpr ']'!)*
    ;

exprPrimary
    : simpleExpr
    | caseExpression
    | ('(' expr ')') => ('(' expr ')' -> expr)
    | subquery
    ;

simpleExpr
    : NULL
    | (dateValue) => dateValue
    | (intervalValue) => intervalValue
    | ('[') => arrayLiteral
    | ('{') => objectLiteral
    | qnameOrFunction
    | specialFunction
    | number
    | parameterExpr
    | bool
    | STRING
    ;

parameterOrLiteral
    : parameterOrSimpleLiteral
    | ('[') => arrayLiteral
    | ('{') => objectLiteral
    ;

parameterOrSimpleLiteral
    : NULL
    | numericLiteral
    | parameterExpr
    | bool
    | STRING
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
    : EQ | NEQ | LT | LTE | GT | GTE | REGEX_MATCH | REGEX_NO_MATCH | REGEX_MATCH_CI | REGEX_NO_MATCH_CI
    ;

setCmpQuantifier
    : ANY | SOME | ALL
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
    | CAST '(' expr AS dataType ')'                -> ^(CAST expr dataType)
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

createMaterializedViewStmt
    : CREATE MATERIALIZED VIEW qname r=viewRefresh? AS s=restrictedSelectStmt -> ^(CREATE_MATERIALIZED_VIEW qname $r? $s)
    ;

refreshMaterializedViewStmt
    : REFRESH MATERIALIZED VIEW qname -> ^(REFRESH_MATERIALIZED_VIEW qname)
    ;

viewRefresh
    : REFRESH r=integer -> ^(REFRESH $r)
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

arrayLiteral
    : '[' ( parameterOrLiteral (',' parameterOrLiteral)* )? ']' -> ^(ARRAY_LITERAL parameterOrLiteral*)
    ;

objectLiteral
    : '{' (objectKeyValue (',' objectKeyValue)* )? '}' -> ^(OBJECT_LITERAL objectKeyValue*)
    ;

objectKeyValue
    : ident EQ parameterOrLiteral -> ^(KEY_VALUE ident parameterOrLiteral)
    ;

insertStmt
    : INSERT INTO table identList? insertSource onDuplicateKey? -> ^(INSERT insertSource table identList? onDuplicateKey?)
    ;

onDuplicateKey
    : ON DUPLICATE KEY UPDATE assignmentList -> ^(ON_DUP_KEY assignmentList)
    ;

insertSource
   : VALUES values=insertValues -> $values
   | '(' query ')' -> query
   ;

identList
    : '(' ident ( ',' ident )* ')' -> ^(IDENT_LIST ident+)
    ;

columnList
    : numericExpr ( ',' numericExpr )* -> ^(COLUMN_LIST numericExpr+)
    ;

insertValues
    : valuesList ( ',' valuesList )* -> ^(INSERT_VALUES valuesList+)
    ;

valuesList
    : '(' expr (',' expr)* ')' -> ^(VALUES_LIST expr+)
    ;


deleteStmt
    : DELETE FROM tablePrimary whereClause? -> ^(DELETE tablePrimary whereClause?)
    ;


updateStmt
    : UPDATE tablePrimary SET assignmentList whereClause? -> ^(UPDATE tablePrimary assignmentList whereClause?)
    ;

assignmentList
    : assignment ( ',' assignment )* -> ^(ASSIGNMENT_LIST assignment+)
    ;

assignment
    : numericExpr EQ expr -> ^(ASSIGNMENT numericExpr expr)
    ;

// COPY STATEMENTS
copyStatement
    : tableWithPartition (
        (FROM) => FROM expr ( WITH '(' genericProperties ')' )? -> ^(COPY_FROM tableWithPartition expr genericProperties?)
        |
        ( '(' columnList ')' )? TO DIRECTORY? expr ( WITH '(' genericProperties ')' )? -> ^(COPY_TO tableWithPartition columnList? DIRECTORY? expr genericProperties?)
    )
    ;
// END COPY STATEMENT

// CREATE STATEMENTS

createStatement
    : TABLE createTableStmt -> createTableStmt
    | BLOB TABLE createBlobTableStmt -> createBlobTableStmt
    | ALIAS createAliasStmt -> createAliasStmt
    | ANALYZER createAnalyzerStmt -> createAnalyzerStmt
    ;

createTableStmt
    : ( IF NOT EXISTS )? table
      tableElementList
      crateTableOption*
      (WITH '(' genericProperties ')' )? -> ^(CREATE_TABLE EXISTS? table tableElementList crateTableOption* genericProperties?)
    ;

createBlobTableStmt
    : table clusteredInto?
      (WITH '(' genericProperties ')' )? -> ^(CREATE_BLOB_TABLE table clusteredInto? genericProperties?)
    ;

createAliasStmt
    : qname forRemote -> ^(CREATE_ALIAS qname forRemote)
    ;

createAnalyzerStmt
    : ident extendsAnalyzer? analyzerElementList -> ^(ANALYZER ident extendsAnalyzer? analyzerElementList)
    ;

// END CREATE STATEMENTS

// ALTER STATEMENTS

alterStatement
    : TABLE alterTableStmt -> alterTableStmt
    | BLOB TABLE alterBlobTableStmt -> alterBlobTableStmt
    ;

alterBlobTableStmt
    : (table SET) => table SET '(' genericProperties ')' -> ^(ALTER_BLOB_TABLE genericProperties table)
    | table RESET identList -> ^(ALTER_BLOB_TABLE identList table)
    ;

alterTableStmt
    : (alterTableDefinition SET) => alterTableDefinition SET '(' genericProperties ')' -> ^(ALTER_TABLE genericProperties alterTableDefinition)
    | (tableWithPartition ADD) => tableWithPartition ADD COLUMN? nestedColumnDefinition -> ^(ADD_COLUMN tableWithPartition nestedColumnDefinition)
    | alterTableDefinition RESET identList -> ^(ALTER_TABLE identList alterTableDefinition)
    ;

alterTableDefinition
    : tableOnly
    | tableWithPartition
    ;
// END ALTER STATEMENTS

// DROP STATEMENTS

dropStatement
	: TABLE ( IF EXISTS )? table -> ^(DROP_TABLE EXISTS? table)
	| BLOB TABLE ( IF EXISTS )? table -> ^(DROP_BLOB_TABLE EXISTS? table)
	| ALIAS qname -> ^(DROP_ALIAS qname)
	;
// END DROP STATEMENTS

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

nestedColumnDefinition
    : expr dataType columnConstDef* -> ^(NESTED_COLUMN_DEF expr dataType columnConstDef*)
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
    | GEO_POINT
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
    : INDEX ident USING indexMethod=ident '(' columnList ')' (WITH '(' genericProperties ')' )? -> ^(INDEX ident $indexMethod columnList genericProperties?)
    ;

genericProperties
    :  genericProperty ( ',' genericProperty )* -> ^(GENERIC_PROPERTIES genericProperty+)
    ;

genericProperty
    : ident EQ expr -> ^(GENERIC_PROPERTY ident expr)
    ;

primaryKeyConstraint
    : PRIMARY_KEY '(' columnList ')' -> ^(PRIMARY_KEY columnList)
    ;

clusteredInto
    : CLUSTERED INTO parameterOrSimpleLiteral SHARDS -> ^(CLUSTERED parameterOrSimpleLiteral)
    ;

clusteredBy
    : CLUSTERED (BY '(' numericExpr ')' )? (INTO parameterOrSimpleLiteral SHARDS)? -> ^(CLUSTERED numericExpr? parameterOrSimpleLiteral?)
    ;

partitionedBy
    : PARTITIONED BY '(' columnList ')' -> ^(PARTITIONED columnList)
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
    : REFRESH TABLE tableWithPartition -> ^(REFRESH tableWithPartition)
    ;

setStmt
    : SET GLOBAL settingsType? assignmentList -> ^(SET settingsType? assignmentList)
    ;

resetStmt
    : RESET GLOBAL columnList -> ^(RESET columnList)
    ;

settingsType
    : TRANSIENT
    | PERSISTENT
    ;

killStmt
    : KILL ALL -> ^(KILL ALL)
    ;

nonReserved
    : ALIAS | ANALYZER | BERNOULLI | BLOB | CATALOGS | CHAR_FILTERS | CLUSTERED
    | COLUMNS | COPY | CURRENT | DATE | DAY | DISTRIBUTED | DUPLICATE | DYNAMIC | EXPLAIN
    | EXTENDS | FOLLOWING | FORMAT | FULLTEXT | FUNCTIONS | GEO_POINT | GLOBAL
    | GRAPHVIZ | HOUR | IGNORED | INTERVAL | KEY | KILL | LOGICAL | MATERIALIZED | MINUTE
    | MONTH | OFF | ONLY | OVER | PARTITION | PARTITIONED | PARTITIONS | PLAIN
    | PRECEDING | RANGE | REFRESH | ROW | ROWS | SCHEMAS | SECOND
    | SHARDS | SHOW | STRICT | SYSTEM | TABLES | TABLESAMPLE | TEXT | TIME
    | TIMESTAMP | TO | TOKENIZER | TOKEN_FILTERS | TYPE | VALUES | VIEW | YEAR
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
KILL: 'KILL';
ONLY: 'ONLY';

ADD: 'ADD';
COLUMN: 'COLUMN';

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
GLOBAL : 'GLOBAL';

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
KEY: 'KEY';
DUPLICATE: 'DUPLICATE';
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

TRANSIENT: 'TRANSIENT';
PERSISTENT: 'PERSISTENT';

MATCH: 'MATCH';


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
