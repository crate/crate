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
 * A SubqueryNode represents a subquery.    Subqueries return values to their
 * outer queries. An quantified subquery is one that appears under a quantified
 * operator (like IN or EXISTS) - quantified subqueries can return more than
 * one value per invocation. An expression subquery is one that is not directly
 * under a quantified operator - expression subqueries are allowed to return
 * at most one value per invocation (returning no value is considered to be
 * equivalent to returning NULL).
 *
 * There are a large number of subquery types.  Because of the large number of
 * types, and the large amount of shared code, we have decided to have 1 SubqueryNode
 * without any subclasses.  The subquery type (and operator) is encoded in the
 * subqueryType field.
 *
 * The query optimizer is responsible for optimizing subqueries, and also for
 * transforming them so that code can be generated for them. The optimizer may
 * eliminate some subqueries by transforming them into joins, or it may
 * change the internal form of a subquery (for example, transforming
 * 'where x in (select y from z where ...)' into
 * 'where (select true from z where x = y and ...)').
 *
 * Note that aggregates present some additional issues.  A transformation
 * such as:
 *  <UL> where x in (SELECT <I>expression</I> FROM z) </UL>
 * has to be treated specially if <I>expression</I> has an aggregate.
 * We change it to:
 *  <UL> where x = (SELECT true FROM (SELECT MAX(x) FROM z) WHERE SQLCOL1 = y) </UL>
 *
 */

public class SubqueryNode extends ValueNode
{
    /*
    ** This must be a single-column result set.  If the subquery is
    ** not quantified, it must also be a single-row result set - that is,
    ** expression subqueries are allowed to return only a single value
    ** per invocation.
    ** NOTE: SubqueryNodes are used as an intermediate step within the parser
    ** for building a derived table.    Derived tables can be multi-column and 
    ** multi-table.
    */
    private ResultSetNode resultSet;

    /* Type of this subquery */
    private SubqueryType subqueryType;

    /* Since we do not have separate subquery operator nodes, the
     * type of the subquery is stored in the subqueryType field.    Most subquery
     * types take a left operand (except for expression and exists).    We could
     * either add a leftOperand field here or subclass SubqueryNode for those
     * types that take a left operand.  We have decided to add the left operand
     * here for now.
     */
    private ValueNode leftOperand;

    private OrderByList orderByList;
    private ValueNode offset;
    private ValueNode fetchFirst;

    /* Subquery types.
     * NOTE: FROM only exists for a brief second in the parser.  It
     * should never appear in a query tree.
     * NOTE: NOT EXISTS and NOT IN subquery types do not exist prior to NOT 
     * elimination during preprocessing.    Prior to that, there is a separate
     * NotNode above the SubqueryNode in the tree.
     *
     */
    public static enum SubqueryType {
        FROM, IN, NOT_IN, EQ_ANY, EQ_ALL, NE_ANY, NE_ALL, GT_ANY, GT_ALL, GE_ANY, GE_ALL,
        LT_ANY, LT_ALL, LE_ANY, LE_ALL, EXISTS, NOT_EXISTS, EXPRESSION
    }

    /**
     * Initializer.
     *
     * @param resultSet The ResultSetNode for the subquery
     * @param subqueryType The type of the subquery
     * @param leftOperand The left operand, if any, of the subquery
     * @param orderCols ORDER BY list
     * @param offset OFFSET n ROWS
     * @param fetchFirst FETCH FIRST n ROWS ONLY
     */

    public void init(Object resultSet,
                     Object subqueryType,
                     Object leftOperand,
                     Object orderCols,
                     Object offset,
                     Object fetchFirst) {
        this.resultSet = (ResultSetNode)resultSet;
        this.subqueryType = (SubqueryType)subqueryType;
        this.orderByList = (OrderByList)orderCols;
        this.offset = (ValueNode)offset;
        this.fetchFirst = (ValueNode)fetchFirst;
        this.leftOperand = (ValueNode)leftOperand;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        SubqueryNode other = (SubqueryNode)node;
        this.resultSet = (ResultSetNode)getNodeFactory().copyNode(other.resultSet,
                                                                  getParserContext());
        this.subqueryType = other.subqueryType;
        this.leftOperand = (ValueNode)getNodeFactory().copyNode(other.leftOperand,
                                                                getParserContext());
        this.orderByList = (OrderByList)getNodeFactory().copyNode(other.orderByList,
                                                                  getParserContext());
        this.offset = (ValueNode)getNodeFactory().copyNode(other.offset,
                                                           getParserContext());
        this.fetchFirst = (ValueNode)getNodeFactory().copyNode(other.fetchFirst,
                                                               getParserContext());
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "subqueryType: " + subqueryType + "\n" +
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

        if (resultSet != null) {
            printLabel(depth, "resultSet: ");
            resultSet.treePrint(depth + 1);
        }

        if (leftOperand != null) {
            printLabel(depth, "leftOperand: ");
            leftOperand.treePrint(depth + 1);
        }

        if (orderByList != null) {
            printLabel(depth, "orderByList: ");
            orderByList.treePrint(depth + 1);
        }

        if (offset != null) {
            printLabel(depth, "offset: ");
            offset.treePrint(depth + 1);
        }

        if (fetchFirst != null) {
            printLabel(depth, "fetchFirst: ");
            fetchFirst.treePrint(depth + 1);
        }
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

        if (resultSet != null) {
            resultSet = (ResultSetNode)resultSet.accept(v);
        }

        if (leftOperand != null) {
            leftOperand = (ValueNode)leftOperand.accept(v);
        }

        if (orderByList != null) {
            orderByList = (OrderByList)orderByList.accept(v);
        }

        if (offset != null) {
            offset = (ValueNode)offset.accept(v);
        }

        if (fetchFirst != null) {
            fetchFirst = (ValueNode)fetchFirst.accept(v);
        }
    }

    /**
     * Return the resultSet for this SubqueryNode.
     *
     * @return ResultSetNode underlying this SubqueryNode.
     */
    public ResultSetNode getResultSet() {
        return resultSet;
    }

    public void setResultSet(ResultSetNode resultSet) {
        this.resultSet = resultSet;
    }

    /**
     * Return the type of this subquery.
     *
     * @return int Type of this subquery.
     */
    public SubqueryType getSubqueryType() {
        return subqueryType;
    }

    /**
     * Set the type of this subquery.
     *
     * @param subqueryType of this subquery.
     */
    public void setSubqueryType(SubqueryType subqueryType) {
        this.subqueryType = subqueryType;
    }
                
    /**
     * {@inheritDoc}
     */      
    protected boolean isEquivalent(ValueNode o) {
        return false;
    }

    /**
     * Get ORDER BY list (used to construct FROM only), cf.
     * FromSubquery, for which this node is transient.
     *
     * @return order by list if specified, else null.
     */
    public OrderByList getOrderByList() {
        return orderByList;
    }

    /**
     * Get OFFSET    (used to construct FROM only), cf.
     * FromSubquery, for which this node is transient.
     *
     * @return offset if specified, else null.
     */
    public ValueNode getOffset() {
        return offset;
    }

    /**
     * Get FETCH FIRST (used to construct FROM only), cf.
     * FromSubquery, for which this node is transient.
     *
     * @return fetch first if specified, else null.
     */
    public ValueNode getFetchFirst() {
        return fetchFirst;
    }

    public ValueNode getLeftOperand() {
        return leftOperand;
    }

}
