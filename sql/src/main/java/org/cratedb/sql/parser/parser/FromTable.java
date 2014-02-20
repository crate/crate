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

import org.cratedb.sql.parser.StandardException;

import java.util.Properties;

/**
 * A FromTable represents a table in the FROM clause of a DML statement.
 * It can be either a base table, a subquery or a project restrict.
 *
 * @see FromBaseTable
 * @see FromSubquery
 * @see ProjectRestrictNode
 *
 */
public abstract class FromTable extends ResultSetNode
{
    protected Properties tableProperties;
    protected String correlationName;
    private TableName corrTableName;

    /** the original unbound table name */
    // TODO: Still need these two separate names?
    protected TableName origTableName;

    /**
     * Initializer for a table in a FROM list.
     *
     * @param correlationName The correlation name
     * @param tableProperties Properties list associated with the table
     */
    public void init(Object correlationName, Object tableProperties) {
        this.correlationName = (String)correlationName;
        this.tableProperties = (Properties)tableProperties;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        FromTable other = (FromTable)node;
        this.tableProperties = other.tableProperties; // TODO: Clone?
        this.correlationName = other.correlationName;
        this.corrTableName = (TableName)getNodeFactory().copyNode(other.corrTableName,
                                                                  getParserContext());
        this.origTableName = (TableName)getNodeFactory().copyNode(other.origTableName,
                                                                  getParserContext());
    }

    /**
     * Get this table's correlation name, if any.
     */
    public String getCorrelationName() { 
        return correlationName; 
    }

    /**
     * Set this table's correlation name.
     */
    public void setCorrelationName(String correlationName) { 
        this.correlationName = correlationName; 
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "correlation Name: " + correlationName + "\n" +
            (corrTableName != null ?
             corrTableName.toString() : "null") + "\n" +
            super.toString();
    }

    /**
     * Return a TableName node representing this FromTable.
     * Expect this to be overridden (and used) by subclasses
     * that may set correlationName to null.
     *
     * @return a TableName node representing this FromTable.
     * @exception StandardException Thrown on error
     */
    public TableName getTableName() throws StandardException {
        if (correlationName == null) return null;

        if (corrTableName == null) {
            corrTableName = makeTableName(null, correlationName);
        }
        return corrTableName;
    }

    public String getExposedName() throws StandardException {
        return null;
    }

    /**
     * Sets the original or unbound table name for this FromTable.  
     * 
     * @param tableName the unbound table name
     *
     */
    public void setOrigTableName(TableName tableName) {
        this.origTableName = tableName;
    }

    /**
     * Gets the original or unbound table name for this FromTable.  
     * The tableName field can be changed due to synonym resolution.
     * Use this method to retrieve the actual unbound tablename.
     * 
     * @return TableName the original or unbound tablename
     *
     */
    public TableName getOrigTableName() {
        return this.origTableName;
    }

}
