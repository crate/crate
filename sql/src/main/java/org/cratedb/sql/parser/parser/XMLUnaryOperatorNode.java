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

   Derby - Class org.apache.derby.impl.sql.compile.UnaryOperatorNode

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
import org.cratedb.sql.parser.types.ValueClassName;

/**
 * A UnaryOperatorNode represents a built-in unary operator as defined by
 * the ANSI/ISO SQL standard.    This covers operators like +, -, NOT, and IS NULL.
 * Java operators are not represented here: the JSQL language allows Java
 * methods to be called from expressions, but not Java operators.
 *
 */

public class XMLUnaryOperatorNode extends UnaryOperatorNode
{
    public static enum OperatorType {
        PARSE("xmlparse", "XMLParse",
              ValueClassName.XMLDataValue,
              ValueClassName.StringDataValue),
        SERIALIZE("xmlserialize", "XMLSerialize",
                  ValueClassName.StringDataValue,
                  ValueClassName.XMLDataValue);
        
        String operator, methodName;
        String resultType, argType;
        OperatorType(String operator, String methodName,
                     String resultType, String argType) {
            this.operator = operator;
            this.methodName = methodName;
            this.resultType = resultType;
            this.argType = argType;
        }
    }

    private OperatorType operatorType;

    // Array to hold Objects that contain primitive
    // args required by the operator method call.
    private Object[] additionalArgs;

    /**
     * Initializer for a UnaryOperatorNode.
     *
     * <ul>
     * @param operand The operand of the node
     * @param operatorType The operatorType for this operator.
     * @param addedArgs An array of Objects
     *  from which primitive method parameters can be
     *  retrieved.
     */

    public void init(Object operand,
                     Object operatorType,
                     Object addedArgs) 
            throws StandardException {
        this.operand = (ValueNode)operand;
        this.operatorType = (OperatorType)operatorType;
        this.operator = this.operatorType.operator;
        this.methodName = this.operatorType.methodName;
        this.resultInterfaceType = this.operatorType.resultType;
        this.receiverInterfaceType = this.operatorType.argType;
        this.additionalArgs = (Object[])addedArgs;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        XMLUnaryOperatorNode other = (XMLUnaryOperatorNode)node;
        this.operatorType = other.operatorType;
        this.additionalArgs = other.additionalArgs; // TODO: Clone?
    }

}
