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

   Derby - Class org.apache.derby.impl.sql.compile.CreateTableNode

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
 * A CreateTableNode is the root of a QueryTree that represents a CREATE TABLE or DECLARE GLOBAL TEMPORARY TABLE
 * statement.
 *
 */

public class CreateTableNode extends DDLStatementNode
{
    public static final int BASE_TABLE_TYPE = 0;
    public static final int SYSTEM_TABLE_TYPE = 1;
    public static final int VIEW_TYPE = 2;
    public static final int GLOBAL_TEMPORARY_TABLE_TYPE = 3;
    public static final int SYNONYM_TYPE = 4;
    public static final int VTI_TYPE = 5;

    public static final char ROW_LOCK_GRANULARITY = 'R';
    public static final char TABLE_LOCK_GRANULARITY = 'T';
    public static final char DEFAULT_LOCK_GRANULARITY = ROW_LOCK_GRANULARITY;

    private char lockGranularity;
    private boolean onCommitDeleteRows; //If true, on commit delete rows else on commit preserve rows of temporary table.
    private boolean onRollbackDeleteRows; //If true, on rollback delete rows from temp table if it was logically modified in that UOW. true is the only supported value
    private Properties properties;
    private TableElementList tableElementList;
    protected int tableType; //persistent table or global temporary table
    private ResultColumnList resultColumns;
    private ResultSetNode queryExpression;
    private boolean withData;
    private ExistenceCheck existenceCheck;

    private Properties crateProperties;

    /**
     * Initializer for a CreateTableNode for a base table
     *
     * @param newObjectName The name of the new object being created (ie base table)
     * @param tableElementList The elements of the table: columns,
     *              constraints, etc.
     * @param properties The optional list of properties associated with
     *              the table.
     * @param lockGranularity The lock granularity.
     *
     * @exception StandardException Thrown on error
     */

    public void init(Object newObjectName,
                     Object tableElementList,
                     Object properties,
                     Object lockGranularity,
                     Object existenceCheck)
            throws StandardException {
        tableType = BASE_TABLE_TYPE;
        this.lockGranularity = ((Character)lockGranularity).charValue();
        implicitCreateSchema = true;

        assert (this.lockGranularity == TABLE_LOCK_GRANULARITY ||
                this.lockGranularity == ROW_LOCK_GRANULARITY);

        initAndCheck(newObjectName);
        this.tableElementList = (TableElementList)tableElementList;
        this.properties = (Properties)properties;
        this.existenceCheck = (ExistenceCheck)existenceCheck;
    }

    /**
     * Initializer for a CreateTableNode for a global temporary table
     *
     * @param newObjectName The name of the new object being declared (ie temporary table)
     * @param tableElementList The elements of the table: columns,
     *              constraints, etc.
     * @param properties The optional list of properties associated with
     *              the table.
     * @param onCommitDeleteRows If true, on commit delete rows else on commit preserve rows of temporary table.
     * @param onRollbackDeleteRows If true, on rollback, delete rows from temp tables which were logically modified. true is the only supported value
     *
     * @exception StandardException Thrown on error
     */

    public void init(Object newObjectName,
                     Object tableElementList,
                     Object properties,
                     Object onCommitDeleteRows,
                     Object onRollbackDeleteRows,
                     Object existenceCheck)
            throws StandardException {
        tableType = GLOBAL_TEMPORARY_TABLE_TYPE;
        newObjectName = tempTableSchemaNameCheck(newObjectName);
        this.onCommitDeleteRows = ((Boolean)onCommitDeleteRows).booleanValue();
        this.onRollbackDeleteRows = ((Boolean)onRollbackDeleteRows).booleanValue();
        initAndCheck(newObjectName);
        this.tableElementList = (TableElementList)tableElementList;
        this.properties = (Properties)properties;
        this.existenceCheck = (ExistenceCheck)existenceCheck;
        assert this.onRollbackDeleteRows;
    }



    /**
     * Initializer for a CreateTableNode for a base table create from a query
     * 
     * @param newObjectName The name of the new object being created
     *              (ie base table).
     * @param resultColumns The optional column list.
     * @param queryExpression The query expression for the table.
     */
    public void init(Object newObjectName,
                     Object resultColumns,
                     Object queryExpression,
                     Object c) 
            throws StandardException {
        tableType = BASE_TABLE_TYPE;
        lockGranularity = DEFAULT_LOCK_GRANULARITY;
        implicitCreateSchema = true;
        initAndCheck(newObjectName);
        this.resultColumns = (ResultColumnList)resultColumns;
        this.queryExpression = (ResultSetNode)queryExpression;
        this.existenceCheck = (ExistenceCheck) c;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        CreateTableNode other = (CreateTableNode)node;
        this.lockGranularity = other.lockGranularity;
        this.onCommitDeleteRows = other.onCommitDeleteRows;
        this.onRollbackDeleteRows = other.onRollbackDeleteRows;
        this.properties = other.properties; // TODO: Clone?
        this.tableElementList = (TableElementList)
            getNodeFactory().copyNode(other.tableElementList, getParserContext());
        this.tableType = other.tableType;
        this.resultColumns = (ResultColumnList)
            getNodeFactory().copyNode(other.resultColumns, getParserContext());
        this.queryExpression = (ResultSetNode)
            getNodeFactory().copyNode(other.queryExpression, getParserContext());
        this.withData = other.withData;
        this.existenceCheck = other.existenceCheck;
    }

    /**
     * If no schema name specified for global temporary table, SESSION is the implicit schema.
     * Otherwise, make sure the specified schema name for global temporary table is SESSION.
     * @param objectName The name of the new object being declared (ie temporary table)
     */
    private Object tempTableSchemaNameCheck(Object objectName) throws StandardException {
        TableName tempTableName = (TableName)objectName;
        if (tempTableName != null) {
            if (tempTableName.getSchemaName() == null)
                tempTableName.setSchemaName("SESSION"); //If no schema specified, SESSION is the implicit schema.
            else if (!"SESSION".equals(tempTableName.getSchemaName()))
                throw new StandardException("Must specify SESSION schema");
        }
        return(tempTableName);
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        String tempString = "";
        if (tableType == GLOBAL_TEMPORARY_TABLE_TYPE) {
            tempString = tempString + "onCommitDeleteRows: " + "\n" + onCommitDeleteRows + "\n";
            tempString = tempString + "onRollbackDeleteRows: " + "\n" + onRollbackDeleteRows + "\n";
        } 
        else
            tempString = tempString +
                (properties != null ? "properties: " + "\n" + properties + "\n" : "") +
                (crateProperties != null ? "crateProperties: " + "\n" + crateProperties + "\n" : "") +
                (withData ? "withData: " + withData + "\n" : "") +
                "lockGranularity: " + lockGranularity + "\n";
        tempString += "existenceCheck: " + existenceCheck + "\n";
        return super.toString() +    tempString;
    }

    public TableElementList getTableElementList() {
        return tableElementList;
    }

    public ResultSetNode getQueryExpression() {
        return queryExpression;
    }

    public boolean isWithData() {
        return withData;
    }

    public void markWithData() {
        withData = true;
    }
    
    public ExistenceCheck getExistenceCheck()
    {
        return existenceCheck;
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     * @param depth The depth to indent the sub-nodes
     */
    public void printSubNodes(int depth) {
        if (tableElementList != null) {
            printLabel(depth, "tableElementList: ");
            tableElementList.treePrint(depth + 1);
        }
        if (queryExpression != null) {
            printLabel(depth, "queryExpression: ");
            queryExpression.treePrint(depth + 1);
        }
    }

    public String statementToString() {
        if (tableType == GLOBAL_TEMPORARY_TABLE_TYPE)
            return "DECLARE GLOBAL TEMPORARY TABLE";
        else
            return "CREATE TABLE";
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

        if (tableElementList != null) {
            tableElementList.accept(v);
        }
        if (queryExpression != null) {
            queryExpression.accept(v);
        }
    }


    public void setCrateProperties(Properties properties) {
        this.crateProperties = properties;
    }
}
