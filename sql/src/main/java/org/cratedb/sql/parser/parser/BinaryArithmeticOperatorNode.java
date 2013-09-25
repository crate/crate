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

   Derby - Class org.apache.derby.impl.sql.compile.BinaryArithmeticOperatorNode

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

import org.cratedb.sql.parser.types.ValueClassName;

/**
 * This node represents a binary arithmetic operator, like + or *.
 *
 */

public class BinaryArithmeticOperatorNode extends BinaryOperatorNode
{
    /**
     * Initializer for a BinaryArithmeticOperatorNode
     *
     * @param leftOperand The left operand
     * @param rightOperand  The right operand
     */

    public void init(Object leftOperand, Object rightOperand) {
        super.init(leftOperand, rightOperand,
                   ValueClassName.NumberDataValue, ValueClassName.NumberDataValue);
    }

    public void setNodeType(int nodeType) {
        String operator = null;
        String methodName = null;

        switch (nodeType) {
        case NodeTypes.BINARY_DIVIDE_OPERATOR_NODE:
            operator = "/";
            methodName = "divide";
            break;

        case NodeTypes.BINARY_MINUS_OPERATOR_NODE:
            operator = "-";
            methodName = "minus";
            break;

        case NodeTypes.BINARY_PLUS_OPERATOR_NODE:
            operator = "+";
            methodName = "plus";
            break;

        case NodeTypes.BINARY_TIMES_OPERATOR_NODE:
            operator = "*";
            methodName = "times";
            break;

        case NodeTypes.MOD_OPERATOR_NODE:
            operator = "mod";
            methodName = "mod";
            break;

        case NodeTypes.BINARY_DIV_OPERATOR_NODE:
            operator = "div";
            methodName = "div";
            break;
            
        default:
            assert false : "Unexpected nodeType:" + nodeType;
        }
        setOperator(operator);
        setMethodName(methodName);
        super.setNodeType(nodeType);
    }

}
