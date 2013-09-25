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

package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.types.ValueClassName;

public class LeftRightFuncOperatorNode extends BinaryOperatorNode
{
    @Override
    public void init (Object leftOperand, Object rightOperand)
    {
        super.init(leftOperand, rightOperand,
                ValueClassName.StringDataValue, ValueClassName.NumberDataValue);
    }
    
    @Override
    public void setNodeType(int nodeType) 
    {
        String op = null;
        String method = null;
        
        switch(nodeType)
        {
            case NodeTypes.LEFT_FN_NODE:
                op = "LEFT";
                method = "getLeft";
                break;
            case NodeTypes.RIGHT_FN_NODE:
                op = "RIGHT";
                method = "getRight";
                break;
            default:
                assert false : "Unexpected nodeType: " + nodeType;
        }
        setOperator(op);
        setMethodName(method);
        super.setNodeType(nodeType);
    }
}
