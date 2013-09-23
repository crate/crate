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

   Derby - Class org.apache.derby.impl.sql.compile.DropSchemaNode

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
 * A DropSchemaNode is the root of a QueryTree that represents 
 * a DROP SCHEMA statement.
 *
 */

public class DropSchemaNode extends DDLStatementNode
{
    private int dropBehavior;
    private String schemaName;
    private ExistenceCheck existenceCheck;

    /**
     * Initializer for a DropSchemaNode
     *
     * @param schemaName The name of the object being dropped
     * @param dropBehavior Drop behavior (RESTRICT | CASCADE)
     *
     */
    public void init(Object schemaName, Object dropBehavior, Object ec) throws StandardException {
        initAndCheck(null);
        this.schemaName = (String)schemaName;
        this.dropBehavior = ((Integer)dropBehavior).intValue();
        this.existenceCheck = (ExistenceCheck)ec;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        DropSchemaNode other = (DropSchemaNode)node;
        this.dropBehavior = other.dropBehavior;
        this.schemaName = other.schemaName;
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
            "schemaName: " + "\n" + schemaName + "\n" +
            "dropBehavior: " + "\n" + dropBehavior + "\n";
        //TODO: add existence check here
    }

    public String statementToString() {
        return "DROP SCHEMA";
    }

    public int getDropBehavior() {
        return this.dropBehavior;
    }
    
    public String getSchemaName() {
        return this.schemaName;
    }
    
    public ExistenceCheck getExistenceCheck()
    {
        return existenceCheck;
    }
}
