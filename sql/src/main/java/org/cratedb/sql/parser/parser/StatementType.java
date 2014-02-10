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

/* The original from which this derives bore the following: */

package org.cratedb.sql.parser.parser;

/**
 * Different types of statements
 *
 */
public interface StatementType
{
    // TODO: A bunch of separate enums.

    public static final int UNKNOWN = 0;
    public static final int INSERT = 1;
    public static final int BULK_INSERT_REPLACE = 2;
    public static final int UPDATE = 3;
    public static final int DELETE = 4;
    public static final int ENABLED = 5;
    public static final int DISABLED = 6;

    public static final int DROP_CASCADE = 0;
    public static final int DROP_RESTRICT = 1;
    public static final int DROP_DEFAULT = 2;

    public static final int RA_CASCADE = 0;
    public static final int RA_RESTRICT = 1;
    public static final int RA_NOACTION = 2;    //default value
    public static final int RA_SETNULL = 3;
    public static final int RA_SETDEFAULT = 4;

    public static final int SET_SCHEMA_USER = 1;
    public static final int SET_SCHEMA_DYNAMIC = 2;

    public static final int SET_ROLE_DYNAMIC = 1;

}
