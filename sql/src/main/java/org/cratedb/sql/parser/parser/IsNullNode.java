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

/**
 * This node represents either a unary 
 * IS NULL or IS NOT NULL comparison operator
 *
 */

public final class IsNullNode extends UnaryComparisonOperatorNode
{

    public void setNodeType(NodeType nodeType)
    {
        String operator;
        String methodName;

        if (nodeType == NodeType.IS_NULL_NODE) {
            /* By convention, the method name for the is null operator is "isNull" */
            operator = "is null";
            methodName = "isNull";
        }
        else {
            assert (nodeType == NodeType.IS_NOT_NULL_NODE);
            /* By convention, the method name for the is not null operator is 
             * "isNotNull" 
             */
            operator = "is not null";
            methodName = "isNotNull";
        }
        setOperator(operator);
        setMethodName(methodName);
        super.setNodeType(nodeType);
    }

}
