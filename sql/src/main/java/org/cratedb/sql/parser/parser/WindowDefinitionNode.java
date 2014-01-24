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
 * This class represents an OLAP window definition.
 */
public final class WindowDefinitionNode extends WindowNode
{
    /**
     * True of the window definition was inlined.
     */
    private boolean inlined;

    /**
     * The partition by list if the window definition contains a <window partition
     * clause>, else null.
     */
    private PartitionByList partitionByList;

    /**
     * The order by list if the window definition contains a <window order
     * clause>, else null.
     */
    private OrderByList orderByList;

    /**
     * Initializer.
     *
     * @param arg1 The window name, null if in-lined definition
     * @param arg2 PARTITION BY list
     * @param arg3 ORDER BY list
     * @exception StandardException
     */
    public void init(Object arg1, Object arg2, Object arg3) throws StandardException {
        String name = (String)arg1;

        partitionByList = (PartitionByList)arg2;
        orderByList = (OrderByList)arg3;

        if (name != null) {
            super.init(arg1);
            inlined = false;
        } 
        else {
            super.init("IN-LINE");
            inlined = true;
        }
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        WindowDefinitionNode other = (WindowDefinitionNode)node;
        this.inlined = other.inlined;
        this.partitionByList = (PartitionByList)getNodeFactory().copyNode(other.partitionByList,
                                                                          getParserContext());
        this.orderByList = (OrderByList)getNodeFactory().copyNode(other.orderByList,
                                                                  getParserContext());
    }

    /**
     * java.lang.Object override.
     * @see QueryTreeNode#toString
     */
    public String toString() {
        return ("name: " + getName() + "\n" +
                "inlined: " + inlined + "\n" +
                "()\n");
    }

    /**
     * QueryTreeNode override. Prints the sub-nodes of this object.
     * @see QueryTreeNode#printSubNodes
     *
     * @param depth         The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        if (partitionByList != null) {
            printLabel(depth, "partitionByList: ");
            partitionByList.treePrint(depth + 1);
        }
        if (orderByList != null) {
            printLabel(depth, "orderByList: ");
            orderByList.treePrint(depth + 1);
        }
    }

    /**
     * Used to merge equivalent window definitions.
     *
     * @param wl list of window definitions
     * @return an existing window definition from wl, if 'this' is equivalent
     * to a window in wl.
     */
    public WindowDefinitionNode findEquivalentWindow(WindowList wl) {
        for (int i = 0; i < wl.size(); i++) {
            WindowDefinitionNode old = wl.get(i);
            if (isEquivalent(old)) {
                return old;
            }
        }
        return null;
    }

    /**
     * @return true if the window specifications are equal; no need to create
     * more than one window then.
     */
    private boolean isEquivalent(WindowDefinitionNode other) {
        if (orderByList == null && other.getOrderByList() == null &&
            partitionByList == null && other.getPartitionByList() == null) {
            return true;
        }

        assert false : "FIXME: ordering in windows not implemented yet";
        return false;
    }

    /**
     * @return whether this definition is inline
     */
    public boolean isInline() {
        return inlined;
    }

    /**
     * @return the order by list of this window definition if any, else null.
     */
    public OrderByList getOrderByList() {
        return orderByList;
    }

    /**
     * @return the partition by list of this window definition if any, else null.
     */
    public PartitionByList getPartitionByList() {
        return partitionByList;
    }

}
