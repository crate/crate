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

import java.util.List;

/**
 * A SelectNode represents the result set for any of the basic DML
 * operations: SELECT, INSERT, UPDATE, and DELETE.  (A RowResultSetNode
 * will be used for an INSERT with a VALUES clause.)    For INSERT - SELECT,
 * any of the fields in a SelectNode can be used (the SelectNode represents
 * the SELECT statement in the INSERT - SELECT).    For UPDATE and
 * DELETE, there will be one table in the fromList, and the groupByList
 * fields will be null. For both INSERT and UPDATE,
 * the resultColumns in the selectList will contain the names of the columns
 * being inserted into or updated.
 *
 */

public class SelectNode extends ResultSetNode
{
    /**
     * List of tables in the FROM clause of this SELECT
     */
    private FromList fromList;

    /**
     * The ValueNode for the WHERE clause must represent a boolean
     * expression.  The binding phase will enforce this - the parser
     * does not have enough information to enforce it in all cases
     * (for example, user methods that return boolean).
     */
    private ValueNode whereClause;

    /**
     * List of result columns in GROUP BY clause
     */
    private GroupByList groupByList;

    /**
     * List of windows.
     */
    private WindowList windows;

    private boolean isDistinct, isStraightJoin;

    private ValueNode havingClause;

    public void init(Object selectList,
                     Object aggregateList,
                     Object fromList,
                     Object whereClause,
                     Object groupByList,
                     Object havingClause,
                     Object windowDefinitionList)
            throws StandardException {
        /* RESOLVE - remove aggregateList from constructor.
         * Consider adding selectAggregates and whereAggregates 
         */
        resultColumns = (ResultColumnList)selectList;
        if (resultColumns != null)
            resultColumns.markInitialSize();
        this.fromList = (FromList)fromList;
        this.whereClause = (ValueNode)whereClause;
        this.groupByList = (GroupByList)groupByList;
        this.havingClause = (ValueNode)havingClause;

        // This initially represents an explicit <window definition list>, as
        // opposed to <in-line window specifications>, see 2003, 6.10 and 6.11.
        // <in-line window specifications> are added later, see right below for
        // in-line window specifications used in window functions in the SELECT
        // column list and in genProjectRestrict for such window specifications
        // used in window functions in ORDER BY.
        this.windows = (WindowList)windowDefinitionList;

        // TODO: Walking to find window and subqueries in WHERE.
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        SelectNode other = (SelectNode)node;
        this.fromList = (FromList)getNodeFactory().copyNode(other.fromList,
                                                            getParserContext());
        this.whereClause = (ValueNode)getNodeFactory().copyNode(other.whereClause,
                                                                getParserContext());
        this.groupByList = (GroupByList)getNodeFactory().copyNode(other.groupByList,
                                                                  getParserContext());
        this.windows = (WindowList)getNodeFactory().copyNode(other.windows,
                                                             getParserContext());
        this.isDistinct = other.isDistinct;
        this.havingClause = (ValueNode)getNodeFactory().copyNode(other.havingClause,
                                                                 getParserContext());
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "isDistinct: "+ isDistinct + "\n"+
            super.toString();
    }

    public String statementToString() {
        return "SELECT";
    }

    public void makeDistinct() {
        isDistinct = true;
    }

    public void clearDistinct() {
        isDistinct = false;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public void makeStraightJoin() {
        isStraightJoin = true;
    }

    public boolean isStraightJoin() {
        return isStraightJoin;
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        printLabel(depth, "fromList: ");

        if (fromList != null) {
            fromList.treePrint(depth + 1);
        }

        if (whereClause != null) {
            printLabel(depth, "whereClause: ");
            whereClause.treePrint(depth + 1);
        }

        if (groupByList != null) {
            printLabel(depth, "groupByList:");
            groupByList.treePrint(depth + 1);
        }

        if (havingClause != null) {
            printLabel(depth, "havingClause:");
            havingClause.treePrint(depth + 1);
        }

        if (windows != null) {
            printLabel(depth, "windows: ");
            windows.treePrint(depth + 1);
        }
    }

    /**
     * Return the fromList for this SelectNode.
     *
     * @return FromList The fromList for this SelectNode.
     */
    public FromList getFromList() {
        return fromList;
    }

    /**
     * Return the whereClause for this SelectNode.
     *
     * @return ValueNode The whereClause for this SelectNode.
     */
    public ValueNode getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(ValueNode whereClause) {
        this.whereClause = whereClause;
    }

    public GroupByList getGroupByList() {
        return groupByList;
    }

    public ValueNode getHavingClause() {
        return havingClause;
    }

    public void setHavingClause(ValueNode havingClause) {
        this.havingClause = havingClause;
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

        if (fromList != null) {
            fromList = (FromList)fromList.accept(v);
        }

        if (whereClause != null) {
            whereClause = (ValueNode)whereClause.accept(v);
        }

        if (groupByList != null) {
            groupByList = (GroupByList)groupByList.accept(v);
        }

        if (havingClause != null) {
            havingClause = (ValueNode)havingClause.accept(v);
        }

        if (windows != null) {
            windows = (WindowList)windows.accept(v);
        }

    }

    /**
     * Used by SubqueryNode to avoid flattening of a subquery if a window is
     * defined on it. Note that any inline window definitions should have been
     * collected from both the selectList and orderByList at the time this
     * method is called, so the windows list is complete. This is true after
     * preprocess is completed.
     *
     * @return true if this select node has any windows on it
     */
    public boolean hasWindows() {
        return windows != null;
    }

    public WindowList getWindows() {
        return windows;
    }

}
