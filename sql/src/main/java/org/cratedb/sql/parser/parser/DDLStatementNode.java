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

/**
 * A DDLStatementNode represents any type of DDL statement: CREATE TABLE,
 * CREATE INDEX, ALTER TABLE, etc.
 *
 */

public abstract class DDLStatementNode extends StatementNode
{
    public static final int UNKNOWN_TYPE = 0;
    public static final int ADD_TYPE = 1;
    public static final int DROP_TYPE = 2;
    public static final int MODIFY_TYPE = 3;
    public static final int LOCKING_TYPE = 4;

    private TableName objectName;
    private boolean initOk;

    /**
       sub-classes can set this to be true to allow implicit
       creation of the main object's schema at execution time.
    */
    boolean implicitCreateSchema;

    public void init(Object objectName) throws StandardException {
        initAndCheck(objectName);
    }

    /**
       Initialize the object name we will be performing the DDL
       on and check that we are not in the system schema
       and that DDL is allowed.
    */
    protected void initAndCheck(Object objectName) throws StandardException {
        this.objectName = (TableName)objectName;
        initOk = true;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        DDLStatementNode other = (DDLStatementNode)node;
        this.objectName = (TableName)getNodeFactory().copyNode(other.objectName,
                                                               getParserContext());
        this.initOk = other.initOk;
        this.implicitCreateSchema = other.implicitCreateSchema;
    }

    /**
     * A DDL statement is always atomic
     *
     * @return true 
     */
    public boolean isAtomic() {
        return true;
    }

    /**
     * Return the name of the table being dropped.
     * This is the unqualified table name.
     *
     * @return the relative name
     */
    public String getRelativeName() {
        return objectName.getTableName() ;
    }

    /**
     * Return the full dot expression name of the 
     * object being dropped.
     * 
     * @return the full name
     */
    public String getFullName() {
        return objectName.getFullTableName() ;
    }

    public final TableName getObjectName() { 
        return objectName; 
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return ((objectName==null)?"":
                "name: " + objectName.toString() +"\n") + super.toString();
    }
        
}
