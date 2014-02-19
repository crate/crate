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

/** Features of the parser grammar. 
 * In particular, dialect-specific constructs that can be turned off for use with
 * ordinary databases.
 */

package org.cratedb.sql.parser.parser;

public enum SQLParserFeature
{
    GEO_INDEX_DEF_FUNC,
    MYSQL_COLUMN_AS_FUNCS,
    MYSQL_LEFT_RIGHT_FUNC,
    DIV_OPERATOR, // integer division
    GROUPING,
    MYSQL_HINTS,
    MYSQL_INTERVAL,
    UNSIGNED,
    INFIX_MOD,
    INFIX_BIT_OPERATORS,
    INFIX_LOGICAL_OPERATORS,
    DOUBLE_QUOTED_STRING
}
