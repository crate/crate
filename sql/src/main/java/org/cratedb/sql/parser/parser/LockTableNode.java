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

   Derby - Class org.apache.derby.impl.sql.compile.LockTableNode

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
 * A LockTableNode is the root of a QueryTree that represents a LOCK TABLE command:
 *  LOCK TABLE <TableName> IN SHARE/EXCLUSIVE MODE
 *
 */

public class LockTableNode extends MiscellaneousStatementNode
{
    private TableName tableName;
    private boolean exclusiveMode;

    /**
     * Initializer for LockTableNode
     *
     * @param tableName The table to lock
     * @param exclusiveMode boolean, whether or not to get an exclusive lock.
     */
    public void init(Object tableName, Object exclusiveMode) {
        this.tableName = (TableName)tableName;
        this.exclusiveMode = ((Boolean)exclusiveMode).booleanValue();
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        LockTableNode other = (LockTableNode)node;
        this.tableName = (TableName)getNodeFactory().copyNode(other.tableName,
                                                              getParserContext());
        this.exclusiveMode = other.exclusiveMode;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "tableName: " + tableName + "\n" +
            "exclusiveMode: " + exclusiveMode + "\n" +
            super.toString();
    }

    public String statementToString() {
        return "LOCK TABLE";
    }

}
