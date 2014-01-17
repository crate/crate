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

package org.cratedb.sql.parser.compiler;

import org.cratedb.sql.parser.parser.*;

import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.types.DataTypeDescriptor;
import org.cratedb.sql.parser.types.TypeId;

import java.sql.Types;

public class TimeTypeCompiler extends TypeCompiler
{
    protected TimeTypeCompiler(TypeId typeId) {
        super(typeId);
    }

    /**
     * User types are convertible to other user types only if
     * (for now) they are the same type and are being used to
     * implement some JDBC type.    This is sufficient for
     * date/time types; it may be generalized later for e.g.
     * comparison of any user type with one of its subtypes.
     *
     * @see TypeCompiler#convertible 
     */
    public boolean convertible(TypeId otherType, boolean forDataTypeFunction) {

        if (otherType.isStringTypeId() && 
            (!otherType.isLOBTypeId()) &&
            !otherType.isLongVarcharTypeId()) {
            return true;
        }
        // If same type, convert always ok.
        return (getStoredFormatIdFromTypeId() == otherType.getTypeFormatId());
    }

    /** @see TypeCompiler#compatible */
    public boolean compatible(TypeId otherType) {
        return convertible(otherType, false);
    }
            
    /**
     * @see TypeCompiler#getCorrespondingPrimitiveTypeName
     */

    public String getCorrespondingPrimitiveTypeName() {
        return "java.sql.Time";
    }

    /**
     * Get the method name for getting out the corresponding primitive
     * Java type.
     *
     * @return String The method call name for getting the
     *                corresponding primitive Java type.
     */
    public String getPrimitiveMethodName() {
        return "getTime";
    }

    /**
     * @see TypeCompiler#getCastToCharWidth
     */
    public int getCastToCharWidth(DataTypeDescriptor dts) {
        return TypeId.TIME_MAXWIDTH;
    }

    /**
     * @see TypeCompiler#resolveArithmeticOperation
     *
     * @exception StandardException     Thrown on error
     */
    public DataTypeDescriptor resolveArithmeticOperation(DataTypeDescriptor leftType,
                                                         DataTypeDescriptor rightType,
                                                         String operator)
            throws StandardException {
        TypeId rightTypeId = rightType.getTypeId();
        boolean nullable = leftType.isNullable() || rightType.isNullable();
        if (rightTypeId.isDateTimeTimeStampTypeId()) {
            if (operator.equals(TypeCompiler.MINUS_OP)) {
                switch (rightTypeId.getTypeFormatId()) {
                case TypeId.FormatIds.TIME_TYPE_ID:
                    // TIME - TIME is INTERVAL HOUR TO SECOND
                    return new DataTypeDescriptor(TypeId.INTERVAL_HOUR_SECOND_ID, nullable);
                }
                // TIME - other datetime is INTERVAL DAY TO SECOND
                return new DataTypeDescriptor(TypeId.INTERVAL_DAY_SECOND_ID, nullable);
            }
        }
        else if (rightTypeId.isIntervalTypeId()) {
            if (operator.equals(TypeCompiler.PLUS_OP) ||
                operator.equals(TypeCompiler.MINUS_OP)) {
                switch (rightTypeId.getTypeFormatId()) {
                case TypeId.FormatIds.INTERVAL_DAY_SECOND_ID:
                    if ((rightTypeId == TypeId.INTERVAL_HOUR_ID) ||
                        (rightTypeId == TypeId.INTERVAL_MINUTE_ID) ||
                        (rightTypeId == TypeId.INTERVAL_SECOND_ID) ||
                        (rightTypeId == TypeId.INTERVAL_HOUR_MINUTE_ID) ||
                        (rightTypeId == TypeId.INTERVAL_HOUR_SECOND_ID) ||
                        (rightTypeId == TypeId.INTERVAL_MINUTE_SECOND_ID))
                        // TIME +/- sub day interval is TIME
                        return leftType.getNullabilityType(nullable);
                }
                // TIME +/- other interval is TIMESTAMP
                return new DataTypeDescriptor(TypeId.TIMESTAMP_ID, nullable);
            }
        }

        // Unsupported
        return super.resolveArithmeticOperation(leftType, rightType, operator);
    }

}
