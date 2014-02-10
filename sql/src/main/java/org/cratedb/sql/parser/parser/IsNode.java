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

public class IsNode extends BinaryLogicalOperatorNode
{
    private boolean notMe; // set to true if we're to negate the sense of this node

    /**
     * Initializer for an IsNode
     *
     * @param leftOperand The left operand of the IS
     * @param rightOperand The right operand of the IS
     * @param notMe Whether to reverse the sense of this node.
     */

    public void init(Object leftOperand,
                     Object rightOperand) {
        super.init(leftOperand, rightOperand, "is");
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        IsNode other = (IsNode)node;
        this.notMe = other.notMe;
    }

    public boolean isNegated() {
        return notMe;
    }

    public void toggleNegated() {
        notMe = !notMe;
    }

}
