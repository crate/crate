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

package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;

import java.util.ArrayList;
import java.util.List;

/**
 * A RowsResultSetNode represents the result set for a multi row VALUES clause.
 *
 */

public class RowsResultSetNode extends FromTable
{
    private List<RowResultSetNode> rows;

    /**
     * Initializer for a RowsResultSetNode.
     *
     * @param firstRow The initial row.
     */
    public void init(Object firstRow) throws StandardException {
        super.init(null, tableProperties);
        RowResultSetNode row = (RowResultSetNode)firstRow;
        rows = new ArrayList<RowResultSetNode>();
        rows.add(row);
        resultColumns = (ResultColumnList)
            getNodeFactory().copyNode(row.getResultColumns(), getParserContext());
    }

    public List<RowResultSetNode> getRows() {
        return rows;
    }

    public void addRow(RowResultSetNode row) {
        rows.add(row);
    }

    public String statementToString() {
        return "VALUES";
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        RowsResultSetNode other = (RowsResultSetNode)node;
        rows = new ArrayList<RowResultSetNode>(other.rows.size());
        for (RowResultSetNode row : other.rows)
            rows.add((RowResultSetNode)getNodeFactory().copyNode(row, getParserContext()));
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);
        for (int index = 0; index < rows.size(); index++) {
            debugPrint(formatNodeString("[" + index + "]:", depth));
            RowResultSetNode row = rows.get(index);
            row.treePrint(depth);
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

        int size = rows.size();
        for (int index = 0; index < size; index++) {
            rows.set(index, (RowResultSetNode)rows.get(index).accept(v));
        }
    }

}
