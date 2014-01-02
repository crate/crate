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

   Derby - Class org.apache.derby.impl.sql.compile.BinaryRelationalOperatorNode

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
 * This class represents the 6 binary operators: LessThan, LessThanEquals,
 * Equals, NotEquals, GreaterThan and GreaterThanEquals.
 *
 */

public class BinaryRelationalOperatorNode extends BinaryComparisonOperatorNode 
{
    // TODO: Is there any point to this?

    public static final int EQUALS_RELOP = 1;
    public static final int NOT_EQUALS_RELOP = 2;
    public static final int GREATER_THAN_RELOP = 3;
    public static final int GREATER_EQUALS_RELOP = 4;
    public static final int LESS_THAN_RELOP = 5;
    public static final int LESS_EQUALS_RELOP = 6;
    public static final int IS_NULL_RELOP = 7;
    public static final int IS_NOT_NULL_RELOP = 8;

    private int operatorType;

    public void init(Object leftOperand, Object rightOperand) {
        String methodName = "";
        String operatorName = "";

        switch (getNodeType()) {
        case BINARY_EQUALS_OPERATOR_NODE:
            methodName = "equals";
            operatorName = "=";
            operatorType = EQUALS_RELOP;
            break;

        case BINARY_GREATER_EQUALS_OPERATOR_NODE:
            methodName = "greaterOrEquals";
            operatorName = ">=";
            operatorType = GREATER_EQUALS_RELOP;
            break;

        case BINARY_GREATER_THAN_OPERATOR_NODE:
            methodName = "greaterThan";
            operatorName = ">";
            operatorType = GREATER_THAN_RELOP;
            break;

        case BINARY_LESS_EQUALS_OPERATOR_NODE:
            methodName = "lessOrEquals";
            operatorName = "<=";
            operatorType =  LESS_EQUALS_RELOP;
            break;

        case BINARY_LESS_THAN_OPERATOR_NODE:
            methodName = "lessThan";
            operatorName = "<";
            operatorType = LESS_THAN_RELOP;
            break;
        case BINARY_NOT_EQUALS_OPERATOR_NODE:
            methodName = "notEquals";
            operatorName = "<>";
            operatorType = NOT_EQUALS_RELOP;
            break;

        default:
            assert false : "init for BinaryRelationalOperator called with wrong nodeType = " + getNodeType();
            break;
        }
        super.init(leftOperand, rightOperand, operatorName, methodName);
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        BinaryRelationalOperatorNode other = (BinaryRelationalOperatorNode)node;
        this.operatorType = other.operatorType;
    }

    public int getOperatorType() {
        return operatorType;
    }

}
