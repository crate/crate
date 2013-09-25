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

   Derby - Class org.apache.derby.impl.sql.compile.TimestampTypeCompiler

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

package org.cratedb.sql.parser.compiler;

import org.cratedb.sql.parser.parser.*;

import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.types.DataTypeDescriptor;
import org.cratedb.sql.parser.types.TypeId;

import java.sql.Types;

public class TimestampTypeCompiler extends TypeCompiler
{
    protected TimestampTypeCompiler(TypeId typeId) {
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
     *
     */
    public boolean convertible(TypeId otherType, boolean forDataTypeFunction) {
        if (otherType.isStringTypeId() &&
            (!otherType.isLongConcatableTypeId())) {
            return true;
        }

        int otherJDBCTypeId = otherType.getJDBCTypeId();

        /*
        ** At this point, we have only date/time.    If
        ** same type, convert always ok.
        */
        if (otherJDBCTypeId == Types.TIMESTAMP) {
            return true;
        }

        /*
        ** Otherwise, we can convert timestamp to
        ** date or time only.
        */
        return ((otherJDBCTypeId == Types.DATE) ||
                (otherJDBCTypeId == Types.TIME));
    }

    /**
     * Tell whether this type (timestamp) is compatible with the given type.
     *
     * @param otherType The TypeId of the other type.
     */
    public boolean compatible(TypeId otherType) {
        if (otherType.isStringTypeId() &&
            (!otherType.isLongConcatableTypeId())) {
            return true;
        }
        /*
        ** Both are timestamp datatypes and hence compatible.
        */
        return (getStoredFormatIdFromTypeId() == otherType.getTypeFormatId());
    }
            
    /**
     * @see TypeCompiler#getCorrespondingPrimitiveTypeName
     */

    public String getCorrespondingPrimitiveTypeName() {
        return "java.sql.Timestamp";
    }

    /**
     * Get the method name for getting out the corresponding primitive
     * Java type.
     *
     * @return String The method call name for getting the
     *                              corresponding primitive Java type.
     */
    public String getPrimitiveMethodName() {
        return "getTimestamp";
    }

    /**
     * @see TypeCompiler#getCastToCharWidth
     */
    public int getCastToCharWidth(DataTypeDescriptor dts) {
        return 26; // DATE TIME.milliseconds (extra few for good measure)
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
                // TIMESTAMP - other datetime is INTERVAL DAY TO SECOND
                return new DataTypeDescriptor(TypeId.INTERVAL_DAY_SECOND_ID, nullable);
            }
        }
        else if (rightTypeId.isIntervalTypeId()) {
            if (operator.equals(TypeCompiler.PLUS_OP) ||
                operator.equals(TypeCompiler.MINUS_OP)) {
                // TIMESTAMP +/- interval is TIMESTAMP
                return leftType.getNullabilityType(nullable);
            }
        }

        // Unsupported
        return super.resolveArithmeticOperation(leftType, rightType, operator);
    }

}
