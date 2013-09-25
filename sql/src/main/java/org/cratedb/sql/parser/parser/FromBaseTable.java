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

   Derby - Class org.apache.derby.impl.sql.compile.FromBaseTable

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
 * A FromBaseTable represents a table in the FROM list of a DML statement,
 * as distinguished from a FromSubquery, which represents a subquery in the
 * FROM list. A FromBaseTable may actually represent a view.    During parsing,
 * we can't distinguish views from base tables. During binding, when we
 * find FromBaseTables that represent views, we replace them with FromSubqueries.
 * By the time we get to code generation, all FromSubqueries have been eliminated,
 * and all FromBaseTables will represent only true base tables.
 * <p>
 * <B>Positioned Update</B>: Currently, all columns of an updatable cursor
 * are selected to deal with a positioned update.    This is because we don't
 * know what columns will ultimately be needed from the UpdateNode above
 * us.  For example, consider:<pre><i>
 *
 * get c as 'select cint from t for update of ctinyint'
 *  update t set ctinyint = csmallint
 *
 * </pre></i> Ideally, the cursor only selects cint.    Then,
 * something akin to an IndexRowToBaseRow is generated to
 * take the CursorResultSet and get the appropriate columns
 * out of the base table from the RowLocation retunrned by the
 * cursor.  Then the update node can generate the appropriate
 * NormalizeResultSet (or whatever else it might need) to
 * get things into the correct format for the UpdateResultSet.
 * See CurrentOfNode for more information.
 *
 */

public class FromBaseTable extends FromTable
{
    public static enum UpdateOrDelete {
        UPDATE, DELETE
    }

    private TableName tableName;
    private UpdateOrDelete updateOrDelete;
    private ResultColumnList templateColumns;
    private IndexHintList indexHints;

    /**
     * Initializer for a table in a FROM list. Parameters are as follows:
     * <ul>
     * <li>tableName The name of the table</li>
     * <li>correlationName The correlation name</li>
     * <li>derivedRCL The derived column list</li>
     * <li>tableProperties The Properties list associated with the table.</li>
     * </ul>
     */
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5) {
        init(arg2, arg4);
        tableName = (TableName)arg1;
        resultColumns = (ResultColumnList)arg3;
        indexHints = (IndexHintList)arg5;
        setOrigTableName(this.tableName);
        templateColumns = resultColumns;
    }

    /**
     * Initializer for a table in a DELETE/ UPDATE. Parameters are as follows:
     * <ul>
     * <li>tableName The name of the table</li>
     * <li>correlationName The correlation name</li>
     * <li>updateOrDelete Table is being updated/deleted from. </li>
     * </ul>
     */
    public void init(Object arg1,
                     Object arg2,
                     Object arg3) {
        init(arg2, null);        
        tableName = (TableName)arg1;
        updateOrDelete = (UpdateOrDelete)arg3;
        setOrigTableName(this.tableName);
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        FromBaseTable other = (FromBaseTable)node;
        this.tableName = (TableName)
            getNodeFactory().copyNode(other.tableName, getParserContext());
        this.updateOrDelete = other.updateOrDelete;
        this.templateColumns = (ResultColumnList)
            getNodeFactory().copyNode(other.templateColumns, getParserContext());
        this.indexHints = (IndexHintList)
            getNodeFactory().copyNode(other.indexHints, getParserContext());
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "tableName: " +
            (tableName != null ? tableName.toString() : "null") + "\n" +
            "updateOrDelete: " + updateOrDelete + "\n" +
            (tableProperties != null ?
             tableProperties.toString() : "null") + "\n" +
            super.toString();
    }

    /**
     * Set the table properties for this table.
     *
     * @param tableProperties The new table properties.
     */
    public void setTableProperties(Properties tableProperties) {
        this.tableProperties = tableProperties;
    }

    /**
     * Get the exposed name for this table, which is the name that can
     * be used to refer to it in the rest of the query.
     *
     * @return The exposed name of this table.
     *
     */
    public String getExposedName() {
        if (correlationName != null)
            return correlationName;
        else
            return getOrigTableName().getFullTableName();
    }

    /**
     * Get the exposed table name for this table, which is the name that can
     * be used to refer to it in the rest of the query.
     *
     * @return TableName The exposed name of this table.
     *
     * @exception StandardException  Thrown on error
     */
    public TableName getExposedTableName() throws StandardException {
        if (correlationName != null)
            return makeTableName(null, correlationName);
        else
            return getOrigTableName();
    }

    /**
     * Return the table name for this table.
     *
     * @return The table name for this table.
     */

    public TableName getTableName() throws StandardException {
        TableName tn = super.getTableName();
        if (tn != null)
            return tn;
        else
            return tableName;
    }

    public IndexHintList getIndexHints() {
        return indexHints;
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        if (indexHints != null) {
            printLabel(depth, "indexHints: ");
            indexHints.treePrint(depth + 1);
        }
    }

    /**
     * Accept the visitor for all visitable children of this node.
     * 
     * @param v the visitor
     *
     * @exception StandardException on error
     */
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        if (indexHints != null) {
            indexHints = (IndexHintList)indexHints.accept(v);
        }
    }

}
