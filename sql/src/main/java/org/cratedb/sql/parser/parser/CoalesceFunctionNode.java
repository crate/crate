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

   Derby - Class org.apache.derby.impl.sql.compile.CoalesceFunctionNode

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
 * This node represents coalesce/value function which returns the first argument that is not null.
 * The arguments are evaluated in the order in which they are specified, and the result of the
 * function is the first argument that is not null. The result can be null only if all the arguments
 * can be null. The selected argument is converted, if necessary, to the attributes of the result.
 *
 */

public class CoalesceFunctionNode extends ValueNode
{
    private String functionName; //Are we here because of COALESCE function or VALUE function
    private ValueNodeList argumentsList; //this is the list of arguments to the function. We are interested in the first not-null argument

    /**
     * Initializer for a CalesceFunctionNode
     *
     * @param functionName Tells if the function was called with name COALESCE or with name VALUE
     * @param argumentsList The list of arguments to the coalesce/value function
     */
    public void init(Object functionName, Object argumentsList) {
        this.functionName = (String)functionName;
        this.argumentsList = (ValueNodeList)argumentsList;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        CoalesceFunctionNode other = (CoalesceFunctionNode)node;
        this.functionName = other.functionName;
        this.argumentsList = (ValueNodeList)
            getNodeFactory().copyNode(other.argumentsList, getParserContext());
    }

    public String getFunctionName() {
        return functionName;
    }

    public ValueNodeList getArgumentsList() {
        return argumentsList;
    }

    /*
     * print the non-node subfields
     */
    public String toString() {
        return "functionName: " + functionName + "\n" +
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

        printLabel(depth, "argumentsList: ");
        argumentsList.treePrint(depth + 1);
    }

    /**
     * {@inheritDoc}
     */
    protected boolean isEquivalent(ValueNode o) throws StandardException {
        if (!isSameNodeType(o)) {
            return false;
        }

        CoalesceFunctionNode other = (CoalesceFunctionNode)o;

        if (!argumentsList.isEquivalent(other.argumentsList)) {
            return false;
        }

        return true;
    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     * @throws StandardException on error in the visitor
     */
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        argumentsList = (ValueNodeList)argumentsList.accept(v);
    }

}
