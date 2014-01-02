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

   Derby - Class org.apache.derby.impl.sql.compile.NodeFactoryImpl

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
    Create new parser AST nodes.
    <p>
    There is one of these per parser context, possibly wrapped for higher-level uses.
 */

public final class NodeFactoryImpl extends NodeFactory
{
    /**
     * Get a node that takes no initializer arguments.
     *
     * @param nodeType Identifier for the type of node.
     * @param pc A SQLParserContext
     *
     * @return A new QueryTree node.
     *
     * @exception StandardException Thrown on error.
     */
    public QueryTreeNode getNode(NodeType nodeType, SQLParserContext pc)
            throws StandardException {
        QueryTreeNode retval = nodeType.newNode();
        retval.setParserContext(pc);
        retval.setNodeType(nodeType);
        return retval;
    }



}
