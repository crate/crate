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
 * A NonStaticMethodCallNode is really a node to represent a (static or non-static)
 * method call from an object (as opposed to a static method call from a class.
 */
public class NonStaticMethodCallNode extends MethodCallNode
{
    /*
    ** The receiver for a non-static method call is an object, represented
    ** by a ValueNode.
    */
    JavaValueNode receiver; 

    /**
     * Initializer for a NonStaticMethodCallNode
     *
     * @param methodName    The name of the method to call
     * @param receiver      A JavaValueNode representing the receiving object
     * @exception StandardException     Thrown on error
     */
    public void init(Object methodName, Object receiver) throws StandardException {
        super.init(methodName);

        /*
        ** If the receiver is a Java value that has been converted to a
        ** SQL value, get rid of the conversion and just use the Java value
        ** as-is.    If the receiver is a "normal" SQL value, then convert
        ** it to a Java value to use as the receiver.
        */
        if (receiver instanceof JavaToSQLValueNode) {
            this.receiver = ((JavaToSQLValueNode)receiver).getJavaValueNode();
        }
        else {
            this.receiver = (JavaValueNode)
                getNodeFactory().getNode(NodeType.SQL_TO_JAVA_VALUE_NODE,
                                         receiver,
                                         getParserContext());
        }
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        NonStaticMethodCallNode other = (NonStaticMethodCallNode)node;
        this.receiver = (JavaValueNode)getNodeFactory().copyNode(other.receiver,
                                                                 getParserContext());
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth     The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);
        if (receiver != null) {
            printLabel(depth, "receiver :");
            receiver.treePrint(depth + 1);
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

        if (receiver != null) {
            receiver = (JavaValueNode)receiver.accept(v);
        }
    }
}
