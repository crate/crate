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

   Derby - Class org.apache.derby.impl.sql.compile.ColumnReference

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

/**
 * A ColumnReference represents a column in the query tree.  The parser generates a
 * ColumnReference for each column reference.    A column refercence could be a column in
 * a base table, a column in a view (which could expand into a complex
 * expression), or a column in a subquery in the FROM clause.
 *
 */

public class ColumnReference extends ValueNode
{
    private String columnName;

    /*
    ** This is the user-specified table name.    It will be null if the
    ** user specifies a column without a table name.    Leave it null even
    ** when the column is bound as it is only used in binding.
    */
    private TableName tableName;

    /**
     * Initializer.
     * This one is called by the parser where we could
     * be dealing with delimited identifiers.
     *
     * @param columnName The name of the column being referenced
     * @param tableName The qualification for the column
     * @param tokBeginOffset begin position of token for the column name 
     *              identifier from parser.
     * @param tokEndOffsetend position of token for the column name 
     *              identifier from parser.
     */

    public void init(Object columnName, 
                     Object tableName,
                     Object tokBeginOffset,
                     Object tokEndOffset) {
        this.columnName = (String)columnName;
        this.tableName = (TableName)tableName;
        this.setBeginOffset(((Integer)tokBeginOffset).intValue());
        this.setEndOffset(((Integer)tokEndOffset).intValue());
    }

    /**
     * Initializer.
     *
     * @param columnName The name of the column being referenced
     * @param tableName The qualification for the column
     */

    public void init(Object columnName, Object tableName) {
        this.columnName = (String)columnName;
        this.tableName = (TableName)tableName;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        ColumnReference other = (ColumnReference)node;
        this.columnName = other.columnName;
        this.tableName = (TableName)
            getNodeFactory().copyNode(other.tableName, getParserContext());
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "columnName: " + columnName + "\n" +
            "tableName: " + ( ( tableName != null) ?
                              tableName.toString() :
                              "null") + "\n" +
            super.toString();
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);
    }

    /**
     * Get the column name for purposes of error
     * messages or debugging. This returns the column
     * name as used in the SQL statement. Thus if it was qualified
     * with a table, alias name that will be included.
     *
     * @return The column name in the form [[schema.]table.]column
     */

    public String getSQLColumnName() {
        if (tableName == null)
            return columnName;

        return tableName.toString() + "." + columnName;
    }

    /**
     * Get the name of this column
     *
     * @return The name of this column
     */

    public String getColumnName() {
        return columnName;
    }

    /**
     * Get the user-supplied table name of this column.  This will be null
     * if the user did not supply a name (for example, select a from t).
     * The method will return B for this example, select b.a from t as b
     * The method will return T for this example, select t.a from t
     *
     * @return The user-supplied name of this column.    Null if no user-
     *               supplied name.
     */

    public String getTableName() {
        return ((tableName != null) ? tableName.getTableName() : null);
    }

    /**
       Return the table name as the node it is.
       @return the column's table name.
    */
    public TableName getTableNameNode() {
        return tableName;
    }

    public void setTableNameNode(TableName tableName) {
        this.tableName = tableName;
    }

    /**
     * Get the user-supplied schema name of this column.    This will be null
     * if the user did not supply a name (for example, select t.a from t).
     * Another example for null return value (for example, select b.a from t as b).
     * But for following query select app.t.a from t, this will return APP
     * Code generation of aggregate functions relies on this method
     *
     * @return The user-supplied schema name of this column.    Null if no user-
     *               supplied name.
     */

    public String getSchemaName() {
        return ((tableName != null) ? tableName.getSchemaName() : null);
    }

    protected boolean isEquivalent(ValueNode o) throws StandardException {
        if (!isSameNodeType(o)) {
            return false;
        }
        ColumnReference other = (ColumnReference)o;
        if (!columnName.equals(other.getColumnName())) {
            return false;
        }
        if (tableName == null)
            return other.tableName == null;
        else
            return tableName.equals(other.tableName);
    }

}
