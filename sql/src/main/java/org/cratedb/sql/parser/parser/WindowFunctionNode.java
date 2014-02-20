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
 * Superclass of any window function call.
 */
public abstract class WindowFunctionNode extends UnaryOperatorNode
{
    private WindowNode window;      // definition or reference

    /**
     * Initializer for a WindowFunctionNode
     * @param arg1 null (operand)
     * @param arg2 function mame (operator)
     * @param arg3 window node (definition or reference)
     * @exception StandardException
     */
    public void init(Object arg1, Object arg2, Object arg3) throws StandardException {
        super.init(arg1, arg2, null);
        this.window = (WindowNode)arg3;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        WindowFunctionNode other = (WindowFunctionNode)node;
        this.window = (WindowNode)getNodeFactory().copyNode(other.window,
                                                            getParserContext());
    }

    /**
     * ValueNode override.
     * @see ValueNode#isConstantExpression
     */
    public boolean isConstantExpression() {
        return false;
    }

    /**
     * @return window associated with this window function
     */
    public WindowNode getWindow() {
        return window;
    }

    /**
     * Set window associated with this window function call.
     * @param wdn window definition
     */
    public void setWindow(WindowDefinitionNode wdn) {
        this.window = wdn;
    }

    /**
     * @return if name matches a defined window (in windows), return the
     * definition of that window, else null.
     */
    private WindowDefinitionNode definedWindow(WindowList windows, String name) {
        for (int i = 0; i < windows.size(); i++) {
            WindowDefinitionNode wdn = windows.get(i);
            if (wdn.getName().equals(name)) {
                return wdn;
            }
        }
        return null;
    }

    /**
     * QueryTreeNode override.
     * @see QueryTreeNode#printSubNodes
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        printLabel(depth, "window: ");
        window.treePrint(depth + 1);
    }

}
