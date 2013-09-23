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
 * An ExplainStatementNode represents the EXPLAIN command.
 *
 */

public class ExplainStatementNode extends StatementNode
{
    public enum Detail {
        BRIEF, NORMAL, VERBOSE
    }
    
    private StatementNode statement;
    private Detail detail;

    /**
     * Initializer for an ExplainStatementNode
     *
     * @param statement The statement to be explained.
     * @param detail Level of detail.
     */

    public void init(Object statement,
                     Object detail) {
        this.statement = (StatementNode)statement;
        this.detail = (Detail)detail;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);
        
        ExplainStatementNode other = (ExplainStatementNode)node;
        this.statement = (StatementNode)getNodeFactory().copyNode(other.statement,
                                                                  getParserContext());
        this.detail = other.detail;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return super.toString();
    }

    public String statementToString() {
        return "EXPLAIN";
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        printLabel(depth, "statement: ");
        statement.treePrint(depth + 1);
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

        statement = (StatementNode)statement.accept(v);
    }

    public StatementNode getStatement() {
        return statement;
    }

    public Detail getDetail() {
        return detail;
    }

}
