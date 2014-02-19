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
import org.cratedb.sql.parser.types.TypeId;

/**
 * ConstantNode holds literal constants as well as nulls.
 * <p>
 * A NULL from the parser may not yet know its type; that
 * must be set during binding, as it is for parameters.
 * <p>
 * the DataValueDescriptor methods want to throw exceptions
 * when they are of the wrong type, but to do that they
 * must check typeId when the value is null, rather than
 * the instanceof check they do for returning a valid value.
 * <p>
 * For code generation, we generate a static field.  Then we set the 
 * field be the proper constant expression (something like <code>
 * getDatavalueFactory().getCharDataValue("hello", ...)) </code>)
 * in the constructor of the generated method.  Ideally
 * we would have just 
 */
public abstract class ConstantNode extends ValueNode
{
    Object value;

    /**
     * Initializer for non-numeric types
     *
     * @param typeId The Type ID of the datatype
     * @param nullable True means the constant is nullable
     * @param maximumWidth The maximum number of bytes in the data value
     *
     * @exception StandardException
     */
    public void init(Object typeId,
                     Object nullable,
                     Object maximumWidth) 
            throws StandardException {
        setType((TypeId)typeId,
                ((Boolean)nullable).booleanValue(),
                ((Integer)maximumWidth).intValue());
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        ConstantNode other = (ConstantNode)node;
        this.value = other.value;       // Assumed to be immutable.
    }

    /**
     * Get the value in this ConstantNode
     */
    public Object getValue() {
        return value;
    }

    /**
     * Set the value in this ConstantNode.
     */
    public void setValue(Object value) {
        this.value = value;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "value: " + value + "\n" +
            super.toString();
    }

    /**
     * Return whether or not this expression tree represents a constant expression.
     *
     * @return Whether or not this expression tree represents a constant expression.
     */
    public boolean isConstantExpression() {
        return true;
    }

    /**
     * Return whether or not this node represents a typed null constant.
     *
     */
    boolean isNull() {
        return (value == null);
    }
                
    protected boolean isEquivalent(ValueNode o) throws StandardException {
        if (isSameNodeType(o)) {
            ConstantNode other = (ConstantNode)o;

            // value can be null which represents a SQL NULL value.
            return ((other.getValue() == null && getValue() == null) || 
                    (other.getValue() != null && other.getValue().equals(getValue())));
        }
        return false;
    }
}
