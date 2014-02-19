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
import org.cratedb.sql.parser.types.DataTypeDescriptor;
import org.cratedb.sql.parser.types.TypeId;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
        User type constants.    These are created by built-in types
        that use user types as their implementation. This could also
        potentially be used by an optimizer that wanted to store plans
        for frequently-used parameter values.

        This is also used to represent nulls in user types, which occurs
        when NULL is inserted into or supplied as the update value for
        a usertype column.

 */
public class UserTypeConstantNode extends ConstantNode 
{
    /*
    ** This value field hides the value in the super-type.  It is here
    ** Because user-type constants work differently from built-in constants.
    ** User-type constant values are stored as Objects, while built-in
    ** constants are stored as StorableDataValues.
    **
    ** RESOLVE: This is a bit of a mess, and should be fixed.    All constants
    ** should be represented the same way.
    */
    Object value;

    /**
     * Initializer for a typed null node
     * or a date, time, or timestamp value. Parameters may be:
     *
     * <ul>
     * <li>arg1 The TypeId for the type of the node</li>
     * </ul>
     *
     * <p>
     * - OR -
     * </p>
     *
     * <ul>
     * <li>arg1 the date, time, or timestamp value</li>
     * </ul>
     *
     * @exception StandardException thrown on failure
     */
    public void init(Object arg1) throws StandardException {
        if (arg1 instanceof TypeId) {
            super.init(arg1,
                       Boolean.TRUE,
                       DataTypeDescriptor.MAXIMUM_WIDTH_UNKNOWN);
        }
        else {
            Integer maxWidth = null;
            TypeId typeId = null;

            if (arg1 instanceof Date) {
                maxWidth = TypeId.DATE_MAXWIDTH;
                typeId = TypeId.getBuiltInTypeId(Types.DATE);
            }
            else if (arg1 instanceof Time) {
                maxWidth = TypeId.TIME_MAXWIDTH;
                typeId = TypeId.getBuiltInTypeId(Types.TIME);
            }
            else if (arg1 instanceof Timestamp) {
                maxWidth = TypeId.TIMESTAMP_MAXWIDTH;
                typeId = TypeId.getBuiltInTypeId(Types.TIMESTAMP);
            }
            else {
                assert false : "Unexpected class " + arg1.getClass().getName();
            }

            super.init(typeId,
                       (arg1 == null) ? Boolean.TRUE : Boolean.FALSE,
                       maxWidth);

            // TODO: For now, these are the same.
            setValue(arg1);
            value = arg1;
        }
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        UserTypeConstantNode other = (UserTypeConstantNode)node;
        this.value = other.value;
    }

    /**
     * Return the object value of this user defined type.
     *
     * @return the value of this constant. can't use getValue() for this.
     *               getValue() returns the DataValueDescriptor for the built-in
     *               types that are implemented as user types (date, time, timestamp)
     */
    public Object getObjectValue() {
        return value; 
    }

    /**
     * Return whether or not this node represents a typed null constant.
     *
     */
    public boolean isNull() {
        return (value == null);
    }

    /**
     * Return an Object representing the bind time value of this
     * expression tree.  If the expression tree does not evaluate to
     * a constant at bind time then we return null.
     * This is useful for bind time resolution of VTIs.
     * RESOLVE: What do we do for primitives?
     *
     * @return An Object representing the bind time value of this expression tree.
     *               (null if not a bind time constant.)
     *
     */
    public Object getConstantValueAsObject() {
        return value;
    }

}
