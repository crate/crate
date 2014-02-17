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
 * A GroupByList represents the list of expressions in a GROUP BY clause in
 * a SELECT statement.
 *
 */

public class GroupByList extends OrderedColumnList<GroupByColumn>
{
    boolean rollup = false;

    /**
     * Add a column to the list
     *
     * @param column The column to add to the list
     */
    public void addGroupByColumn(GroupByColumn column) {
        add(column);
    }

    /**
     * Get a column from the list
     * 
     * @param position The column to get from the list
     */
    public GroupByColumn getGroupByColumn(int position) {
        return get(position);
    }

    public void setRollup() {
        rollup = true;
    }
    public boolean isRollup() {
        return rollup;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */
    public String toString() {
        return super.toString();
    }

}
