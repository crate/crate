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

   Derby - Class org.apache.derby.impl.sql.compile.QueryTreeNodeVector

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * QueryTreeNodeList is the root class for all lists of query tree nodes.
 * It provides a wrapper for java.util.List. All
 * lists of query tree nodes inherit from QueryTreeNodeList.
 *
 */

public abstract class QueryTreeNodeList<N extends QueryTreeNode> 
    extends QueryTreeNode implements Iterable<N>
{
    private List<N> list = new ArrayList<N>();

    public final int size() {
        return list.size();
    }

    protected List<N> getList()
    {
        return list;
    }

    public final boolean isEmpty() {
        return list.isEmpty();
    }

    public N get(int index) {
        return list.get(index);
    }

    public void add(N n) {
        list.add(n);
    }

    public final N remove(int index) {
        return list.remove(index);
    }

    public final void remove(N n) {
        list.remove(n);
    }

    public final int indexOf(N n) {
        return list.indexOf(n);
    }

    public final void set(int index, N n) {
        list.set(index, n);
    }

    public final void add(int index, N n) {
        list.add(index, n);
    }

    public final void addAll(QueryTreeNodeList<N> other) {
        list.addAll(other.list);
    }

    public final void clear() {
        list.clear();
    }

    public final Iterator<N> iterator() {
        return list.iterator();
    }

    public final void destructiveAddAll(QueryTreeNodeList<N> other) {
        addAll(other);
        other.clear();
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        QueryTreeNodeList<N> other = (QueryTreeNodeList<N>)node;
        for (N n : other.list)
            list.add((N)getNodeFactory().copyNode(n, getParserContext()));
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     * @param depth     The depth to indent the sub-nodes
     */
    public void printSubNodes(int depth) {
        for (int index = 0; index < size(); index++) {
            debugPrint(formatNodeString("[" + index + "]:", depth));
            N elt = get(index);
            elt.treePrint(depth);
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

        int size = size();
        for (int index = 0; index < size; index++) {
            set(index, (N)get(index).accept(v));
        }
    }
}
