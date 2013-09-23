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

   Derby - Class org.apache.derby.impl.sql.compile.OrderByColumn

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
 * An OrderByColumn is a column in the ORDER BY clause.  An OrderByColumn
 * can be ordered ascending or descending.
 *
 * We need to make sure that the named columns are
 * columns in that query, and that positions are within range.
 *
 */
public class OrderByColumn extends OrderedColumn 
{
    private ValueNode expression;
    private boolean ascending = true;
    private boolean nullsOrderedLow = false;

    /**
     * Initializer.
     *
     * @param expression Expression of this column
     */
    public void init(Object expression) {
        this.expression = (ValueNode)expression;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        OrderByColumn other = (OrderByColumn)node;
        this.expression = (ValueNode)getNodeFactory().copyNode(other.expression,
                                                               getParserContext());
        this.ascending = other.ascending;
        this.nullsOrderedLow = other.nullsOrderedLow;
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

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */
    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        if (expression != null) {
            printLabel(depth, "expression: ");
            expression.treePrint(depth + 1);
        }
    }

    public ValueNode getExpression() {
        return expression;
    }

    /**
     * Mark the column as descending order
     */
    public void setDescending() {
        ascending = false;
    }

    /**
     * Get the column order.    Overrides 
     * OrderedColumn.isAscending.
     *
     * @return true if ascending, false if descending
     */
    public boolean isAscending() {
        return ascending;
    }

    /**
     * Mark the column as ordered NULL values lower than non-NULL values.
     */
    public void setNullsOrderedLow() {
        nullsOrderedLow = true;
    }

    /**
     * Get the column NULL ordering. Overrides
     * OrderedColumn.getIsNullsOrderedLow.
     *
     * @return true if NULLs ordered low, false if NULLs ordered high
     */
    public boolean isNullsOrderedLow() {
        return nullsOrderedLow;
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

        if (expression != null) {
            expression = (ValueNode)expression.accept(v);
        }
    }

}
