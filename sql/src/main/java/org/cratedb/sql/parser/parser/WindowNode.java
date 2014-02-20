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
 * Superclass of window definition and window reference.
 */
public abstract class WindowNode extends QueryTreeNode
{
    /**
     * The provided name of the window if explicitly defined in a window
     * clause. If the definition is inlined, currently the definition has
     * windowName "IN_LINE".    The standard 2003 sec. 4.14.9 calls for a
     * impl. defined one.
     */
    private String windowName;

    /**
     * Initializer
     *
     * @param arg1 The window name
     *
     * @exception StandardException
     */
    public void init(Object arg1) throws StandardException {
        windowName = (String)arg1;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        WindowNode other = (WindowNode)node;
        this.windowName = other.windowName;
    }

    /**
     * @return the name of this window
     */
    public String getName() {
        return windowName;
    }

}
