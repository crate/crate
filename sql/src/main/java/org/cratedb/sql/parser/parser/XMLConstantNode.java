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

public final class XMLConstantNode extends ConstantNode
{
    /**
     * Initializer for an XMLConstantNode.
     *
     * @param arg1 The TypeId for the type of the node
     *
     * @exception StandardException
     */
    public void init(Object arg1) throws StandardException {
        super.init(arg1,
                   Boolean.TRUE,
                   0);
    }

    /**
     * Return an Object representing the bind time value of this
     * expression tree.  If the expression tree does not evaluate to
     * a constant at bind time then we return null.
     *
     * @return An Object representing the bind time value of this
     *               expression tree (null if not a bind time constant).
     *
     * @exception StandardException Thrown on error
     */
    Object getConstantValueAsObject() throws StandardException 
    {
        return value;
    }

}
