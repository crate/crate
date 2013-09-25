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

   Derby - Class org.apache.derby.impl.sql.compile.InsertNode

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

import java.util.Properties;

/**
 * An InsertNode is the top node in a query tree for an
 * insert statement.
 * <p>
 * After parsing, the node contains
 *   targetTableName: the target table for the insert
 *   collist: a list of column names, if specified
 *   queryexpr: the expression being inserted, either
 *              a values clause or a select form; both
 *                  of these are represented via the SelectNode,
 *              potentially with a TableOperatorNode such as
 *              UnionNode above it.
 * <p>
 * After binding, the node has had the target table's
 * descriptor located and inserted, and the queryexpr
 * and collist have been massaged so that they are identical
 * to the table layout.  This involves adding any default
 * values for missing columns, and reordering the columns
 * to match the table's ordering of them.
 * <p>
 * After optimizing, ...
 */
public final class InsertNode extends DMLModStatementNode
{
    private ResultColumnList targetColumnList;
    private Properties targetProperties;
    private OrderByList orderByList;
    private ValueNode offset;
    private ValueNode fetchFirst;

    /**
     * Initializer for an InsertNode.
     *
     * @param targetName The name of the table/VTI to insert into
     * @param insertColumns A ResultColumnList with the names of the
     *                                          columns to insert into.  May be null if the
     *                                          user did not specify the columns - in this
     *                                          case, the binding phase will have to figure
     *                                          it out.
     * @param queryExpression The query expression that will generate
     *                                              the rows to insert into the given table
     * @param targetProperties The properties specified on the target table
     * @param orderByList The order by list for the source result set, null if
     *                                      no order by list
     */

    public void init(Object targetName,
                     Object insertColumns,
                     Object queryExpression,
                     Object targetProperties,
                     Object orderByList,
                     Object offset,
                     Object fetchFirst,
                     Object returningList) {
        /* statementType gets set in super() before we've validated
         * any properties, so we've kludged the code to get the
         * right statementType for a bulk insert replace.
         */
        super.init(queryExpression,
                   getStatementType((Properties)targetProperties));
        setTarget((QueryTreeNode)targetName);
        targetColumnList = (ResultColumnList)insertColumns;
        this.targetProperties = (Properties)targetProperties;
        this.orderByList = (OrderByList)orderByList;
        this.offset = (ValueNode)offset;
        this.fetchFirst = (ValueNode)fetchFirst;
        this.returningColumnList = (ResultColumnList)returningList;

        /* Remember that the query expression is the source to an INSERT */
        getResultSetNode().setInsertSource();
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        InsertNode other = (InsertNode)node;
        this.targetColumnList = (ResultColumnList)
            getNodeFactory().copyNode(other.targetColumnList, getParserContext());
        this.targetProperties = other.targetProperties; // TODO: Clone?
        this.orderByList = (OrderByList)
            getNodeFactory().copyNode(other.orderByList, getParserContext());
        this.offset = (ValueNode)
            getNodeFactory().copyNode(other.offset, getParserContext());
        this.fetchFirst = (ValueNode)
            getNodeFactory().copyNode(other.fetchFirst, getParserContext());
    }

    public String statementToString() {
        return "INSERT";
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        if (targetColumnList != null) {
            printLabel(depth, "targetColumnList: ");
            targetColumnList.treePrint(depth + 1);
        }

        if (orderByList != null) {
            printLabel(depth, "orderByList: ");
            orderByList.treePrint(depth + 1);
        }
        
        /* RESOLVE - need to print out targetTableDescriptor */
    }

    /**
     * Return the type of statement, something from
     * StatementType.
     *
     * @return the type of statement
     */
    protected int getStatementType() {
        return StatementType.INSERT;
    }

    /**
     * Return the statement type, where it is dependent on
     * the targetProperties.    (insertMode = replace causes
     * statement type to be BULK_INSERT_REPLACE.
     *
     * @return the type of statement
     */
    static int getStatementType(Properties targetProperties) {
        int retval = StatementType.INSERT;

        // The only property that we're currently interested in is insertMode
        String insertMode = (targetProperties == null) ? null : targetProperties.getProperty("insertMode");
        if (insertMode != null) {
            if ("REPLACE".equalsIgnoreCase(insertMode)) {
                retval = StatementType.BULK_INSERT_REPLACE;
            }
        }
        return retval;
    }

    public ResultColumnList getTargetColumnList() {
        return targetColumnList;
    }

    public Properties getTargetProperties() {
        return targetProperties;
    }

    public OrderByList getOrderByList() {
        return orderByList;
    }

    public ValueNode getOffset() {
        return offset;
    }

    public ValueNode getFetchFirst() {
        return fetchFirst;
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

        if (targetColumnList != null) {
            targetColumnList.accept(v);
        }
    }

}
