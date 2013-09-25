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

   Derby - Class org.apache.derby.impl.sql.compile.JavaToSQLValueNode

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
 * This node type converts a value from the Java domain to the SQL domain.
 */

public class JavaToSQLValueNode extends ValueNode
{
    JavaValueNode javaNode;

    /**
     * Initializer for a JavaToSQLValueNode
     *
     * @param value The Java value to convert to the SQL domain
     */
    public void init(Object value) {
        this.javaNode = (JavaValueNode)value;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        JavaToSQLValueNode other = (JavaToSQLValueNode)node;
        this.javaNode = (JavaValueNode)getNodeFactory().copyNode(other.javaNode,
                                                                 getParserContext());
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        printLabel(depth, "javaNode: ");
        javaNode.treePrint(depth + 1);
    }

    /**
     * Get the JavaValueNode that lives under this JavaToSQLValueNode.
     *
     * @return The JavaValueNode that lives under this node.
     */

    public JavaValueNode getJavaValueNode() {
        return javaNode;
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

        if (javaNode != null) {
            javaNode = (JavaValueNode)javaNode.accept(v);
        }
    }
                
    /**
     * {@inheritDoc}
     */
    protected boolean isEquivalent(ValueNode o) {
        // anything in the java domain is not equiavlent.
        return false;
    }

}
