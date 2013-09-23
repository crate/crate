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

   Derby - Class org.apache.derby.impl.sql.compile.BinaryOperatorNode

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
 * A BinaryOperatorNode represents a built-in binary operator as defined by
 * the ANSI/ISO SQL standard.    This covers operators like +, -, *, /, =, <, etc.
 * Java operators are not represented here: the JSQL language allows Java
 * methods to be called from expressions, but not Java operators.
 *
 */

public class XMLBinaryOperatorNode extends BinaryOperatorNode
{
    // Derby did the following, which just make things too messy:
    //   At the time of adding XML support, it was decided that
    //   we should avoid creating new OperatorNodes where possible.
    //   So for the XML-related binary operators we just add the
    //   necessary code to _this_ class, similar to what is done in
    //   TernarnyOperatorNode. Subsequent binary operators (whether
    //   XML-related or not) should follow this example when
    //   possible.

    public static enum OperatorType {
        EXISTS("xmlexists", "XMLExists",
               ValueClassName.BooleanDataValue,
               new String[] { ValueClassName.StringDataValue, ValueClassName.XMLDataValue }),
        QUERY("xmlquery", "XMLQuery", 
              ValueClassName.XMLDataValue,
              new String [] { ValueClassName.StringDataValue, ValueClassName.XMLDataValue });

        String operator, methodName;
        String resultType;
        String[] argTypes;
        OperatorType(String operator, String methodName,
                     String resultType, String[] argTypes) {
            this.operator = operator;
            this.methodName = methodName;
            this.resultType = resultType;
            this.argTypes = argTypes;
        }
    }

    public static enum PassByType {
        REF, VALUE
    }
    public static enum ReturnType {
        SEQUENCE, CONTENT
    }
    public static enum OnEmpty {
        EMPTY, NULL
    }

    /**
     * Initializer for a BinaryOperatorNode
     *
     * @param leftOperand The left operand of the node
     * @param rightOperand The right operand of the node
     * @param opType    An Integer holding the operatorType
     *  for this operator.
     */
    public void init(Object leftOperand,
                     Object rightOperand,
                     Object opType) {
        this.leftOperand = (ValueNode)leftOperand;
        this.rightOperand = (ValueNode)rightOperand;
        OperatorType operatorType = (OperatorType)opType;
        this.operator = operatorType.operator;
        this.methodName = operatorType.operator;
        this.leftInterfaceType = operatorType.argTypes[0];
        this.rightInterfaceType = operatorType.argTypes[1];
        this.resultInterfaceType = operatorType.resultType;
    }

}
