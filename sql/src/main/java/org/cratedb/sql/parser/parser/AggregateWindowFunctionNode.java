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
   Derby - Class org.apache.derby.impl.sql.compile.AggregateWindowFunctionNode

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
 * Represents aggregate function calls on a window
 */
public final class AggregateWindowFunctionNode extends WindowFunctionNode
{
    private AggregateNode aggregateFunction;

    /**
     * Initializer. QueryTreeNode override.
     *
     * @param arg1 The window definition or reference
     * @param arg2 aggregate function node
     *
     * @exception StandardException
     */
    public void init(Object arg1, Object arg2) throws StandardException {
        super.init(null, "?", arg1);
        aggregateFunction = (AggregateNode)arg2;
    }

    public AggregateNode getAggregateFunction() {
        return aggregateFunction;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);
        
        AggregateWindowFunctionNode other = (AggregateWindowFunctionNode)node;
        aggregateFunction = (AggregateNode)getNodeFactory().copyNode(other.aggregateFunction,
                                                                     getParserContext());
    }

    /**
     * QueryTreeNode override. Prints the sub-nodes of this object.
     * @see QueryTreeNode#printSubNodes
     *
     * @param depth         The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);
        printLabel(depth, "aggregate: ");
        aggregateFunction.treePrint(depth + 1);
    }
}
