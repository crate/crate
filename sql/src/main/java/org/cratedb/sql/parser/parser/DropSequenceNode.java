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

   Derby - Class org.apache.derby.impl.sql.compile.DropSequenceNode

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
 * A DropSequenceNode    represents a DROP SEQUENCE statement.
 */

public class DropSequenceNode extends DDLStatementNode 
{
    private TableName dropItem;
    private ExistenceCheck existenceCheck;
    private int dropBehavior;
    /**
     * Initializer for a DropSequenceNode
     *
     * @param dropSequenceName The name of the sequence being dropped
     * @throws StandardException
     */
    public void init(Object dropSequenceName, Object dropBehavior, Object ec) throws StandardException {
        dropItem = (TableName)dropSequenceName;
        initAndCheck(dropItem);
        this.dropBehavior = ((Integer)dropBehavior).intValue();
        this.existenceCheck = (ExistenceCheck)ec;
    }

    public int getDropBehavior() {
        return dropBehavior;
    }
    
    public ExistenceCheck getExistenceCheck()
    {
        return existenceCheck;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        DropSequenceNode other = (DropSequenceNode)node;
        this.dropBehavior = other.dropBehavior;
        this.existenceCheck = other.existenceCheck;
        this.dropItem = (TableName)getNodeFactory().copyNode(other.dropItem,
                                                             getParserContext());
    }

    public String statementToString() {
        return "DROP SEQUENCE ".concat(dropItem.getTableName());
    }

    public String toString() {
        return super.toString() + 
                "dropBehavior: " + dropBehavior + "\n"
                + "existenceCheck: " + existenceCheck + "\n";
    }
}
