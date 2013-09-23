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

   Derby - Class org.apache.derby.impl.sql.compile.FromSubquery

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
 * A FromSubquery represents a subquery in the FROM list of a DML statement.
 *
 * The current implementation of this class is only
 * sufficient for Insert's need to push a new
 * select on top of the one the user specified,
 * to make the selected structure match that
 * of the insert target table.
 *
 */
public class FromSubquery extends FromTable
{
    ResultSetNode subquery;
    private OrderByList orderByList;
    private ValueNode offset;
    private ValueNode fetchFirst;

    /**
     * Intializer for a table in a FROM list.
     *
     * @param subquery The subquery
     * @param orderByList       ORDER BY list if any, or null
     * @param offset                OFFSET if any, or null
     * @param fetchFirst        FETCH FIRST if any, or null
     * @param correlationName The correlation name
     * @param derivedRCL The derived column list
     * @param tableProperties Properties list associated with the table
     */
    public void init(Object subquery,
                     Object orderByList,
                     Object offset,
                     Object fetchFirst,
                     Object correlationName,
                     Object derivedRCL,
                     Object tableProperties)
    {
        super.init(correlationName, tableProperties);
        this.subquery = (ResultSetNode)subquery;
        this.orderByList = (OrderByList)orderByList;
        this.offset = (ValueNode)offset;
        this.fetchFirst = (ValueNode)fetchFirst;
        resultColumns = (ResultColumnList)derivedRCL;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        FromSubquery other = (FromSubquery)node;
        this.subquery = (ResultSetNode)getNodeFactory().copyNode(other.subquery,
                                                                 getParserContext());
        this.orderByList = (OrderByList)getNodeFactory().copyNode(other.orderByList,
                                                                  getParserContext());
        this.offset = (ValueNode)getNodeFactory().copyNode(other.offset,
                                                           getParserContext());
        this.fetchFirst = (ValueNode)getNodeFactory().copyNode(other.fetchFirst,
                                                               getParserContext());
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        if (subquery != null) {
            printLabel(depth, "subquery: ");
            subquery.treePrint(depth + 1);
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
     * Return the "subquery" from this node.
     *
     * @return ResultSetNode The "subquery" from this node.
     */
    public ResultSetNode getSubquery() {
        return subquery;
    }

    /**
     * Get the exposed name for this table, which is the name that can
     * be used to refer to it in the rest of the query.
     *
     * @return The exposed name for this table.
     */

    public String getExposedName() {
        return correlationName;
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

        if (subquery != null) {
            subquery = (ResultSetNode)subquery.accept(v);
        }
        if (orderByList != null) {
            orderByList = (OrderByList)orderByList.accept(v);
        }
    }

}
