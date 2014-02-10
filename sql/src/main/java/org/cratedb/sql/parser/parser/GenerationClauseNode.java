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
 * This node describes a Generation Clause in a column definition.
 *
 */
public class GenerationClauseNode extends ValueNode
{
    private ValueNode generationExpression;
    private String expressionText;

    public void init(Object generationExpression, Object expressionText) {
        this.generationExpression = (ValueNode)generationExpression;
        this.expressionText = (String)expressionText;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        GenerationClauseNode other = (GenerationClauseNode)node;
        this.generationExpression = (ValueNode)
            getNodeFactory().copyNode(other.generationExpression, getParserContext());
        this.expressionText = other.expressionText;
    }

    /** Get the defining text of this generation clause */
    public String getExpressionText() { 
        return expressionText; 
    }

    protected boolean isEquivalent(ValueNode other) throws StandardException {
        if (!(other instanceof GenerationClauseNode)) { 
            return false; 
        }

        GenerationClauseNode that = (GenerationClauseNode)other;
        return this.generationExpression.isEquivalent(that.generationExpression);
    }
        
    public String toString() {
        return
            "expressionText: GENERATED ALWAYS AS ( " +
            expressionText + " )\n" +
            super.toString();
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */
    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        printLabel(depth, "generationExpression: ");
        generationExpression.treePrint(depth + 1);
    }

}
