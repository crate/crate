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
 * A visitor is an object that traverses the querytree
 * and performs some action. 
 *
 */
public interface Visitor
{
    /**
     * This is the default visit operation on a 
     * QueryTreeNode.    It just returns the node.  This
     * will typically suffice as the default visit 
     * operation for most visitors unless the visitor 
     * needs to count the number of nodes visited or 
     * something like that.
     * <p>
     * Visitors will overload this method by implementing
     * a version with a signature that matches a specific
     * type of node.    For example, if I want to do
     * something special with aggregate nodes, then
     * that Visitor will implement a 
     *              <I> visit(AggregateNode node)</I>
     * method which does the aggregate specific processing.
     *
     * @param node      the node to process
     *
     * @return a query tree node.    Often times this is
     * the same node that was passed in, but Visitors that
     * replace nodes with other nodes will use this to
     * return the new replacement node.
     *
     * @exception StandardException may be throw an error
     *      as needed by the visitor (i.e. may be a normal error
     *      if a particular node is found, e.g. if checking 
     *      a group by, we don't expect to find any ColumnReferences
     *      that aren't under an AggregateNode -- the easiest
     *      thing to do is just throw an error when we find the
     *      questionable node).
     */
    Visitable visit(Visitable node) throws StandardException;

    /**
     * Method that is called to see if {@code visit()} should be called on
     * the children of {@code node} before it is called on {@code node} itself.
     * If this method always returns {@code true}, the visitor will walk the
     * tree bottom-up. If it always returns {@code false}, the tree is visited
     * top-down.
     *
     * @param node the top node of a sub-tree about to be visited
     * @return {@code true} if {@code node}'s children should be visited
     * before {@code node}, {@code false} otherwise
     */
    boolean visitChildrenFirst(Visitable node);

    /**
     * Method that is called to see
     * if query tree traversal should be
     * stopped before visiting all nodes.
     * Useful for short circuiting traversal
     * if we already know we are done.
     *
     * @return true/false
     */
    boolean stopTraversal();

    /**
     * Method that is called to indicate whether
     * we should skip all nodes below this node
     * for traversal.    Useful if we want to effectively
     * ignore/prune all branches under a particular 
     * node.    
     * <p>
     * Differs from stopTraversal() in that it
     * only affects subtrees, rather than the
     * entire traversal.
     *
     * @param node the node to process
     * 
     * @return true/false
     */
    boolean skipChildren(Visitable node) throws StandardException;
}
