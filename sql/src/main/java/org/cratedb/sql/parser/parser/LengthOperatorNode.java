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

import org.cratedb.sql.parser.types.TypeId;

import java.sql.Types;

/**
 * This node represents a unary XXX_length operator
 *
 */

public final class LengthOperatorNode extends UnaryOperatorNode
{
    private int parameterType;
    private int parameterWidth;

    public void setNodeType(NodeType nodeType) {
        String operator = null;
        String methodName = null;

        if (nodeType == NodeType.CHAR_LENGTH_OPERATOR_NODE) {
            operator = "char_length";
            methodName = "charLength";
            parameterType = Types.VARCHAR;
            parameterWidth = TypeId.VARCHAR_MAXWIDTH;
        }
        else {
            assert false : "Unexpected nodeType = " + nodeType;
        }
        setOperator(operator);
        setMethodName(methodName);
        super.setNodeType(nodeType);
    }

}
