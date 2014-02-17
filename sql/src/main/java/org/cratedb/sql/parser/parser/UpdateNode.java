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
 * An UpdateNode represents an UPDATE statement.    It is the top node of the
 * query tree for that statement.
 * For positioned update, there may be no from table specified.
 * The from table will be derived from the cursor specification of
 * the named cursor.
 *
 */

public final class UpdateNode extends DMLModStatementNode
{

    /**
     * Initializer for an UpdateNode.
     *
     * @param targetTableName The name of the table to update
     * @param resultSet The ResultSet that will generate
     *                                  the rows to update from the given table
     */

    public void init(Object targetTableName,
                     Object resultSet,
                     Object returningList) {
        super.init(resultSet);
        this.targetTableName = (TableName)targetTableName;
        this.returningColumnList = (ResultColumnList)returningList;
    }

    public String statementToString() {
        return "UPDATE";
    }
    
    /**
     * Return the type of statement, something from
     * StatementType.
     *
     * @return the type of statement
     */
    protected final int getStatementType() {
        return StatementType.UPDATE;
    }

}
