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

   Derby - Class org.apache.derby.impl.sql.compile.AllResultColumn

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
 * An AllResultColumn represents a "*" result column in a SELECT
 * statement.    It gets replaced with the appropriate set of columns
 * at bind time.
 *
 */

public class AllResultColumn extends ResultColumn
{
    private TableName tableName;
    private boolean recursive;

    /**
     * This initializer is for use in the parser for a "*".
     * 
     * @param arg TableName Dot expression qualifying "*" or Boolean recursive
     */
    public void init(Object arg) {
        if (arg instanceof Boolean)
            this.recursive = (Boolean)arg;
        else
            this.tableName = (TableName)arg;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        AllResultColumn other = (AllResultColumn)node;
        this.tableName = (TableName)getNodeFactory().copyNode(other.tableName,
                                                              getParserContext());
        this.recursive = other.recursive;
    }

    /** 
     * Return the full table name qualification for this node
     *
     * @return Full table name qualification as a String
     */
    public String getFullTableName() {
        if (tableName == null) {
            return null;
        }
        else {
            return tableName.getFullTableName();
        }
    }

    public TableName getTableNameObject() {
        return tableName;
    }

    public boolean isRecursive() {
        return recursive;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */
    // TODO: Somewhat of a mess: the superclass has a tableName field of a different type.
    public String toString() {
        return "tableName: " + tableName + "\n" +
            super.toString();
    }
}
