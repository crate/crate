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

import java.math.BigDecimal;
import java.sql.Types;

public final class NumericConstantNode extends ConstantNode
{
    /**
     * Initializer for a typed null node
     *
     * @param arg1 The TypeId for the type of node OR An object containing the value of the constant.
     *
     * @exception StandardException
     */
    public void init(Object arg1) throws StandardException {
        int precision = 0, scal = 0, maxwidth = 0;
        Boolean isNullable;
        boolean valueInP; // value in Predicate-- if TRUE a value was passed in
        TypeId typeId = null;
        int typeid = 0;

        if (arg1 instanceof TypeId) {
            typeId = (TypeId)arg1;
            isNullable = Boolean.TRUE;
            valueInP = false;
            maxwidth = 0;
        }
        else {
            isNullable = Boolean.FALSE;
            valueInP = true;
        }

        switch (getNodeType()) {
        case TINYINT_CONSTANT_NODE:
            precision = TypeId.SMALLINT_PRECISION;
            scal = TypeId.SMALLINT_SCALE;
            if (valueInP) {
                maxwidth = TypeId.SMALLINT_MAXWIDTH;
                typeid = Types.TINYINT;
                setValue((Byte)arg1);
            } 
            break;

        case INT_CONSTANT_NODE:
            precision = TypeId.INT_PRECISION;
            scal = TypeId.INT_SCALE;
            if (valueInP) {
                maxwidth = TypeId.INT_MAXWIDTH;
                typeid = Types.INTEGER;
                setValue((Integer)arg1);
            }
            break;

        case SMALLINT_CONSTANT_NODE:
            precision = TypeId.SMALLINT_PRECISION;
            scal = TypeId.SMALLINT_SCALE;
            if (valueInP) {
                maxwidth = TypeId.SMALLINT_MAXWIDTH;
                typeid = Types.SMALLINT;
                setValue((Short)arg1);
            }
            break;

        case LONGINT_CONSTANT_NODE:
            precision = TypeId.LONGINT_PRECISION;
            scal = TypeId.LONGINT_SCALE;
            if (valueInP) {
                maxwidth = TypeId.LONGINT_MAXWIDTH;
                typeid = Types.BIGINT;
                setValue((Long)arg1);
            }
            break;

        case DECIMAL_CONSTANT_NODE:
            if (valueInP) {
                typeid = Types.DECIMAL;
                String image = (String)arg1;
                int length = image.length();
                int idx = image.indexOf('.');
                precision = length;
                if (!Character.isDigit(image.charAt(0)) && (idx != 0))
                    precision--;                    // Has a sign.
                if (idx < 0)
                    scal = 0;
                else {
                    precision--;
                    scal = length - idx - 1;
                }
                maxwidth = length;
                setValue(new BigDecimal(image));
            }
            else {
                precision = TypeId.DEFAULT_DECIMAL_PRECISION;
                scal = TypeId.DEFAULT_DECIMAL_SCALE;
                maxwidth = TypeId.DECIMAL_MAXWIDTH;
            }
            break;

        case DOUBLE_CONSTANT_NODE:
            precision = TypeId.DOUBLE_PRECISION;
            scal = TypeId.DOUBLE_SCALE;
            if (valueInP) {
                maxwidth = TypeId.DOUBLE_MAXWIDTH;
                typeid = Types.DOUBLE;
                setValue((Double)arg1);
            }
            break;

        case FLOAT_CONSTANT_NODE:
            precision = TypeId.REAL_PRECISION;
            scal = TypeId.REAL_SCALE;
            if (valueInP) {
                maxwidth = TypeId.REAL_MAXWIDTH;
                typeid = Types.REAL;
                setValue((Float)arg1);
            }
            break;

        default:
            assert false : "Unexpected nodeType = " + getNodeType();
            break;
        }

        setType((typeId != null) ? typeId : TypeId.getBuiltInTypeId(typeid),
                precision, 
                scal, 
                isNullable.booleanValue(), 
                maxwidth);
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
     * @exception StandardException Thrown on error
     */
    Object getConstantValueAsObject() throws StandardException {
        return value;
    }

}
