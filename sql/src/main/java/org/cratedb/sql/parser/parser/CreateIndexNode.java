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

   Derby - Class org.apache.derby.impl.sql.compile.CreateIndexNode

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
import org.cratedb.sql.parser.parser.JoinNode.JoinType;

/**
 * A CreateIndexNode is the root of a QueryTree that represents a CREATE INDEX
 * statement.
 *
 */

public class CreateIndexNode extends DDLStatementNode implements IndexDefinition
{
    boolean unique;
    TableName indexName;
    TableName tableName;
    String indexMethod;
    IndexColumnList columnList;
    GenericProperties indexProperties;
    ExistenceCheck existenceCheck;

    /**
     * Initializer for a CreateIndexNode
     *
     * @param unique True means it's a unique index
     * @param indexName The name of the index
     * @param tableName The name of the table the index will be on
     * @param columnList A list of columns, in the order they
     *                   appear in the index.
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object unique,
                     Object indexName,
                     Object tableName,
                     Object indexMethod,
                     Object columnList,
                     Object indexProperties,
                     Object existenceCheck)
            throws StandardException {
        initAndCheck(indexName);
        this.unique = (Boolean)unique;
        this.indexName = (TableName)indexName;
        this.tableName = (TableName)tableName;
        this.indexMethod = (String)indexMethod;
        this.columnList = (IndexColumnList)columnList;
        this.indexProperties = (GenericProperties)indexProperties;
        this.existenceCheck = (ExistenceCheck)existenceCheck;

    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        CreateIndexNode other = (CreateIndexNode)node;
        this.unique = other.unique;
        this.indexName = (TableName)
            getNodeFactory().copyNode(other.indexName, getParserContext());
        this.tableName = (TableName)
            getNodeFactory().copyNode(other.tableName, getParserContext());
        this.indexMethod = other.indexMethod;
        this.columnList = (IndexColumnList)
            getNodeFactory().copyNode(other.columnList, getParserContext());
        this.indexProperties = (GenericProperties) getNodeFactory().copyNode(other.indexProperties, getParserContext());
        this.existenceCheck = other.existenceCheck;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return super.toString() +
            "unique: " + unique + "\n" +
            "indexName: " + indexName + "\n" +
            "tableName: " + tableName + "\n" +
            "indexMethod: " + indexMethod + "\n" +
            "indexColumns: " + columnList + "\n" +
            "indexProperties: " + indexProperties + "\n" +
            "existenceCheck: " + existenceCheck + "\n";
    }

    public void printSubNodes(int depth) {
        if (columnList != null) {
            columnList.treePrint(depth+1);
        }
    }
    public String statementToString() {
        return "CREATE INDEX";
    }

    public boolean getUniqueness() { 
        return unique; 
    }

    @Override
    public JoinType getJoinType() {
        throw new UnsupportedOperationException("JoinType not supported on Crate Index");
    }

    public TableName getIndexName() {
        return indexName; 
    }

    public String getIndexMethod() {
        return indexMethod;
    }
    public GenericProperties getIndexProperties() {
        return indexProperties;
    }

    public IndexColumnList getColumnList() {
        return columnList;
    }
    public IndexColumnList getIndexColumnList() {
        return columnList;
    }

    public TableName getIndexTableName() {
        return tableName; 
    }

    public ExistenceCheck getExistenceCheck()
    {
        return existenceCheck;
    }
}
