/**
 * Copyright 2011-2013 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* The original from which this derives bore the following: */

/*

   Derby - Class org.apache.derby.impl.sql.compile.FromList

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;

import java.util.Properties;

/**
 * A FromList represents the list of tables in a FROM clause in a DML
 * statement.    It extends QueryTreeNodeList.
 *
 */

public class FromList extends QueryTreeNodeList<FromTable>
{
    Properties properties;
    boolean fixedJoinOrder;

    /* Whether or not this FromList is transparent.  A "transparent" FromList
     * is one in which all FromTables are bound based on an outer query's
     * FromList.    This means that the FromTables in the transparent list are
     * allowed to see and reference FromTables in the outer query's list.
     * Or put differently, a FromTable which sits in a transparent FromList
     * does not "see" the transparent FromList when binding; rather, it sees
     * (and can therefore reference) the FromList of an outer query.
     */
    private boolean isTransparent;

    /** Initializer for a FromList */

    public void init(Object optimizeJoinOrder) {
        fixedJoinOrder = ! (((Boolean)optimizeJoinOrder).booleanValue());
        isTransparent = false;
    }

    /**
     * Initializer for a FromList
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object optimizeJoinOrder, Object fromTable)
            throws StandardException {
        init(optimizeJoinOrder);

        addFromTable((FromTable)fromTable);
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        FromList other = (FromList)node;
        this.properties = other.properties; // TODO: Clone?
        this.fixedJoinOrder = other.fixedJoinOrder;
        this.isTransparent = other.isTransparent;
    }

    /**
     * Add a table to the FROM list.
     *
     * @param fromTable A FromTable to add to the list
     *
     * @exception StandardException Thrown on error
     */

    public void addFromTable(FromTable fromTable) throws StandardException {
        /* Don't worry about checking TableOperatorNodes since
         * they don't have exposed names.    This will potentially
         * allow duplicate exposed names in some degenerate cases,
         * but the binding of the ColumnReferences will catch those
         * cases with a different error.    If the query does not have
         * any ColumnReferences from the duplicate exposed name, the
         * user is executing a really dumb query and we won't throw
         * and exception - consider it an ANSI extension.
         */
        TableName leftTable = null;
        TableName rightTable = null;
        if (!(fromTable instanceof TableOperatorNode)) {
            /* Check for duplicate table name in FROM list */
            leftTable = fromTable.getTableName();
            int size = size();
            for (int index = 0; index < size; index++) {
                if (get(index) instanceof TableOperatorNode) {
                    continue;
                }
                else {                                      
                    rightTable = get(index).getTableName();
                }
                if (leftTable.equals(rightTable)) {
                    throw new StandardException("Table duplicated in FROM list: " + 
                                                fromTable.getExposedName());
                }
            }
        }

        add(fromTable);
    }

    /**
     * Set the Properties list for this FromList.
     *
     * @exception StandardException Thrown on error
     */
    public void setProperties(Properties props) throws StandardException {
        properties = props;
    }

}
