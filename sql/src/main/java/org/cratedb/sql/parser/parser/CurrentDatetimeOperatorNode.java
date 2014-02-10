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

import java.sql.Types;

/**
 * The CurrentDatetimeOperator operator is for the builtin CURRENT_DATE,
 * CURRENT_TIME, and CURRENT_TIMESTAMP operations.
 *
 */
public class CurrentDatetimeOperatorNode extends ValueNode 
{
    public static enum Field {
        DATE("CURRENT DATE", Types.DATE),
        TIME("CURRENT TIME", Types.TIME),
        TIMESTAMP("CURRENT TIMESTAMP", Types.TIMESTAMP);

        String methodName;
        int jdbcTypeId;

        Field(String methodName, int jdbcTypeId) {
            this.methodName = methodName;
            this.jdbcTypeId = jdbcTypeId;
        }
    }

    private Field field;

    public void init(Object field) {
        this.field = (Field)field;
    }

    public Field getField() {
        return field;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        CurrentDatetimeOperatorNode other = (CurrentDatetimeOperatorNode)node;
        this.field = other.field;
    }

    public String toString() {
        return "methodName: " + field.methodName + "\n" +
            super.toString();
    }

    /**
     * {@inheritDoc}
     */
    protected boolean isEquivalent(ValueNode o) {
        if (isSameNodeType(o)) {
            CurrentDatetimeOperatorNode other = (CurrentDatetimeOperatorNode)o;
            return other.field == field;
        }
        return false;
    }
}
