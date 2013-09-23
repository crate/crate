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

   Derby - Class org.apache.derby.iapi.sql.compile.TypeCompiler

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

/**
 * This interface defines methods associated with a TypeId that are used
 * by the compiler.
 */

public abstract class TypeCompiler
{
    /**
     * Various fixed numbers related to datatypes.
     */
    // Need to leave space for '-'
    public static final int LONGINT_MAXWIDTH_AS_CHAR = 20;

    // Need to leave space for '-'
    public static final int INT_MAXWIDTH_AS_CHAR = 11;

    // Need to leave space for '-'
    public static final int SMALLINT_MAXWIDTH_AS_CHAR = 6;

    // Need to leave space for '-'
    public static final int TINYINT_MAXWIDTH_AS_CHAR = 4;

    // Need to leave space for '-' and decimal point
    public static final int DOUBLE_MAXWIDTH_AS_CHAR = 54;

    // Need to leave space for '-' and decimal point
    public static final int REAL_MAXWIDTH_AS_CHAR = 25;

    public static final int BOOLEAN_MAXWIDTH_AS_CHAR = 5;

    public static final String PLUS_OP = "+";
    public static final String DIVIDE_OP = "/";
    public static final String DIV_OP = "div";
    public static final String MINUS_OP = "-";
    public static final String TIMES_OP = "*";
    public static final String SUM_OP = "sum";
    public static final String AVG_OP = "avg";
    public static final String MOD_OP = "mod";

    private TypeId typeId;

    protected TypeCompiler(TypeId typeId) {
        this.typeId = typeId;
    }

    public TypeId getTypeId() {
        return typeId;
    }

    /**
     * Type resolution methods on binary operators
     *
     * @param leftType The type of the left parameter
     * @param rightType The type of the right parameter
     * @param operator The name of the operator (e.g. "+").
     *
     * @return The type of the result
     *
     * @exception StandardException Thrown on error
     */

    public DataTypeDescriptor resolveArithmeticOperation(DataTypeDescriptor leftType,
                                                         DataTypeDescriptor rightType,
                                                         String operator)
            throws StandardException {
        TypeId leftTypeId = leftType.getTypeId();
        TypeId rightTypeId = rightType.getTypeId();
        if (leftTypeId.equals(rightTypeId))
            return leftType;

        throw new StandardException("Types not compatible for " + operator +
                                    ": " + leftTypeId.getSQLTypeName() +
                                    " and " + rightTypeId.getSQLTypeName());
    }

    /**
     * Determine if this type can be CONVERTed to some other type
     *
     * @param otherType The CompilationType of the other type to compare
     *                  this type to
     *
     * @param forDataTypeFunction true if this is a type function that
     *   requires more liberal behavior (e.g DOUBLE can convert a char but 
     *   you cannot cast a CHAR to double.
     *   
     * @return true if the types can be converted, false if conversion
     *              is not allowed
     */
    public abstract boolean convertible(TypeId otherType, boolean forDataTypeFunction);

    /**
     * Tell whether this numeric type can be converted to the given type.
     *
     * @param otherType The TypeId of the other type.
     * @param forDataTypeFunction was this called from a scalarFunction like
     *                                                      CHAR() or DOUBLE()
     */
    protected boolean numberConvertible(TypeId otherType, boolean forDataTypeFunction) {
        if (otherType.isAnsiUDT()) { 
            return false; 
        }

        // Can't convert numbers to long types
        if (otherType.isLongConcatableTypeId())
            return false;

        // Numbers can only be converted to other numbers, 
        // and CHAR, (not VARCHARS or LONGVARCHAR). 
        // Only with the CHAR() or VARCHAR()function can they be converted.
        boolean retval =((otherType.isNumericTypeId()) ||
                         (otherType.userType()));

        // For CHAR conversions, function can convert floating types.
        if (forDataTypeFunction)
            retval = retval || 
                (otherType.isFixedStringTypeId() &&
                 (getTypeId().isFloatingPointTypeId()));
         
        retval = retval ||
            (otherType.isFixedStringTypeId() &&                         
             (!getTypeId().isFloatingPointTypeId()));
        
        return retval;
    }

    /**
     * Determine if this type is compatible to some other type
     * (e.g. COALESCE(thistype, othertype)).
     *
     * @param otherType The CompilationType of the other type to compare
     *                                  this type to
     *
     * @return true if the types are compatible, false if not compatible
     */
    public abstract boolean compatible(TypeId otherType);

    /**
     * Get the name of the corresponding Java type.  For numerics and booleans
     * we will get the corresponding Java primitive type.
     e
     * Each SQL type has a corresponding Java type.  When a SQL value is
     * passed to a Java method, it is translated to its corresponding Java
     * type.    For example, a SQL Integer will be mapped to a Java int, but
     * a SQL date will be mapped to a java.sql.Date.
     *
     * @return The name of the corresponding Java primitive type.
     */
    public abstract String getCorrespondingPrimitiveTypeName();

    /**
     * Get the method name for getting out the corresponding primitive
     * Java type from a DataValueDescriptor.
     *
     * @return String The method call name for getting the
     *                              corresponding primitive Java type.
     */
    public abstract String getPrimitiveMethodName();

    /**
     * Return the maximum width for this data type when cast to a char type.
     *
     * @param dtd The associated DataTypeDescriptor for this TypeId.
     *
     * @return int The maximum width for this data type when cast to a char type.
     */
    public abstract int getCastToCharWidth(DataTypeDescriptor dtd);
    
    /**
     * Get the format id from the corresponding TypeId.
     *
     * @return The format from the corresponding TypeId.
     * @see TypeId.FormatIds
     */
    protected int getStoredFormatIdFromTypeId() {
        return getTypeId().getTypeFormatId();
    }

    // These are all the TypeCompilers that are stateless, so we can
    // use a single instance of each. Initialize all to null, and fault
    // them in.
    private static TypeCompiler bitTypeCompiler;
    private static TypeCompiler booleanTypeCompiler;
    private static TypeCompiler charTypeCompiler;
    private static TypeCompiler decimalTypeCompiler;
    private static TypeCompiler doubleTypeCompiler;
    private static TypeCompiler intTypeCompiler;
    private static TypeCompiler longintTypeCompiler;
    private static TypeCompiler longvarbitTypeCompiler;
    private static TypeCompiler longvarcharTypeCompiler;
    private static TypeCompiler realTypeCompiler;
    private static TypeCompiler smallintTypeCompiler;
    private static TypeCompiler tinyintTypeCompiler;
    private static TypeCompiler dateTypeCompiler;
    private static TypeCompiler timeTypeCompiler;
    private static TypeCompiler timestampTypeCompiler;
    private static TypeCompiler varbitTypeCompiler;
    private static TypeCompiler varcharTypeCompiler;
    private static TypeCompiler refTypeCompiler;
    private static TypeCompiler blobTypeCompiler;
    private static TypeCompiler clobTypeCompiler;
    private static TypeCompiler xmlTypeCompiler;
    private static TypeCompiler intervalMonthTypeCompiler, intervalSecondTypeCompiler;

    /**
     * Get the TypeCompiler that corresponds to the given TypeId.
     */
    public static TypeCompiler getTypeCompiler(TypeId typeId) {
        switch (typeId.getJDBCTypeId()) {
        case Types.BINARY:
            if (bitTypeCompiler == null)
                bitTypeCompiler = new BitTypeCompiler(typeId);
            return bitTypeCompiler;

        case Types.BIT:
        case Types.BOOLEAN:
            if (booleanTypeCompiler == null)
                booleanTypeCompiler = new BooleanTypeCompiler(typeId);
            return booleanTypeCompiler;

        case Types.CHAR:
            if (charTypeCompiler == null)
                charTypeCompiler = new CharTypeCompiler(typeId);
            return charTypeCompiler;

        case Types.NUMERIC:
        case Types.DECIMAL:
            if (decimalTypeCompiler == null)
                decimalTypeCompiler = new NumericTypeCompiler(typeId);
            return decimalTypeCompiler;

        case Types.DOUBLE:
            if (doubleTypeCompiler == null)
                doubleTypeCompiler = new NumericTypeCompiler(typeId);
            return doubleTypeCompiler;

        case Types.INTEGER:
            if (intTypeCompiler == null)
                intTypeCompiler = new NumericTypeCompiler(typeId);
            return intTypeCompiler;

        case Types.BIGINT:
            if (longintTypeCompiler == null)
                longintTypeCompiler = new NumericTypeCompiler(typeId);
            return longintTypeCompiler;

        case Types.BLOB:
            if (blobTypeCompiler == null)
                blobTypeCompiler = new LOBTypeCompiler(typeId);
            return blobTypeCompiler;

        case Types.LONGVARBINARY:
            if (longvarbitTypeCompiler == null)
                longvarbitTypeCompiler = new BitTypeCompiler(typeId);
            return longvarbitTypeCompiler;

        case Types.CLOB:
            if (clobTypeCompiler == null)
                clobTypeCompiler = new CLOBTypeCompiler(typeId);
            return clobTypeCompiler;

        case Types.LONGVARCHAR:
            if (longvarcharTypeCompiler == null)
                longvarcharTypeCompiler = new CharTypeCompiler(typeId);
            return longvarcharTypeCompiler;

        case Types.REAL:
            if (realTypeCompiler == null)
                realTypeCompiler = new NumericTypeCompiler(typeId);
            return realTypeCompiler;

        case Types.SMALLINT:
            if (smallintTypeCompiler == null)
                smallintTypeCompiler = new NumericTypeCompiler(typeId);
            return smallintTypeCompiler;

        case Types.TINYINT:
            if (tinyintTypeCompiler == null)
                tinyintTypeCompiler = new NumericTypeCompiler(typeId);
            return tinyintTypeCompiler;

        case Types.DATE:
            if (dateTypeCompiler == null)
                dateTypeCompiler = new DateTypeCompiler(typeId);
            return dateTypeCompiler;

        case Types.TIME:
            if (timeTypeCompiler == null)
                timeTypeCompiler = new TimeTypeCompiler(typeId);
            return timeTypeCompiler;

        case Types.TIMESTAMP:
            if (timestampTypeCompiler == null)
                timestampTypeCompiler = new TimestampTypeCompiler(typeId);
            return timestampTypeCompiler;

        case Types.VARBINARY:
            if (varbitTypeCompiler == null)
                varbitTypeCompiler = new BitTypeCompiler(typeId);
            return varbitTypeCompiler;

        case Types.VARCHAR:
            if (varcharTypeCompiler == null)
                varcharTypeCompiler = new CharTypeCompiler(typeId);
            return varcharTypeCompiler;

        case Types.JAVA_OBJECT:
        case Types.OTHER:
            if (typeId.isRefTypeId()) {
                if (refTypeCompiler == null)
                    refTypeCompiler = new RefTypeCompiler(typeId);
                return refTypeCompiler;
            }
            else if (typeId.isIntervalTypeId()) {
                switch (typeId.getTypeFormatId()) {
                case TypeId.FormatIds.INTERVAL_YEAR_MONTH_ID:
                    if (intervalMonthTypeCompiler == null)
                        intervalMonthTypeCompiler = new IntervalTypeCompiler(typeId);
                    return intervalMonthTypeCompiler;
                case TypeId.FormatIds.INTERVAL_DAY_SECOND_ID:                        
                    if (intervalSecondTypeCompiler == null)
                        intervalSecondTypeCompiler = new IntervalTypeCompiler(typeId);
                    return intervalSecondTypeCompiler;
                default:
                    return null;
                }
            }
            else {
                // Cannot re-use instances of user-defined type compilers,
                // because they contain the class name
                return new UserDefinedTypeCompiler(typeId);
            }

        case Types.SQLXML:
            if (xmlTypeCompiler == null)
                xmlTypeCompiler = new XMLTypeCompiler(typeId);
            return xmlTypeCompiler;

        default:
            assert false : "Unexpected JDBC type id " + typeId.getJDBCTypeId();
            return null;
        }
    }

}
