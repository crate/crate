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

package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;

/**
 * FETCH rows from declared cursor.
 */

public class FetchStatementNode extends StatementNode
{
    private String name;
    private int count;

    /**
     * Initializer for an FetchStatementNode
     *
     * @param name The name of the cursor
     * @param count The number of rows to fetch
     */

    public void init(Object name,
                     Object count) {
        this.name = (String)name;
        this.count = (Integer)count;
    }
    
    public String getName() {
        return name;
    }

    public int getCount() {
        return count;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);
        
        FetchStatementNode other = (FetchStatementNode)node;
        this.name = other.name;
        this.count = other.count;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "name: " + name + "\n" +
            "count: " + ((count < 0) ? "ALL" : Integer.toString(count)) + "\n" +
            super.toString();
    }

    public String statementToString() {
        return "FETCH";
    }

}
