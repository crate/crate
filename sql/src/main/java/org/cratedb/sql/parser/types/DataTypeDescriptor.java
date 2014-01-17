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

package org.cratedb.sql.parser.types;

import org.cratedb.sql.parser.StandardException;

import java.sql.Types;

/** 
 * DataTypeDescriptor describes a runtime SQL type.
 * It consists of a catalog type (TypeDescriptor)
 * and runtime attributes. The list of runtime
 * attributes is:
 * <UL>
 * <LI> Collation Derivation
 * </UL>
 * <P>
 * A DataTypeDescriptor is immutable.
 */

// NOTE: The Derby original had two levels of type descriptor, this
// one for in memory and a simpler TypeDescriptor (the "catalog type")
// that was actually stored in the data dictionary. For now, they have
// been combined into this one.

public final class DataTypeDescriptor
{
    public static final int MAXIMUM_WIDTH_UNKNOWN = -1;

    public static final DataTypeDescriptor MEDIUMINT =
        new DataTypeDescriptor(TypeId.MEDIUMINT_ID, true);
    
    public static final DataTypeDescriptor MEDIUMINT_NOT_NULL =
        MEDIUMINT.getNullabilityType(true);
    
    /**
     * Runtime INTEGER type that is nullable.
     */
    public static final DataTypeDescriptor INTEGER =
        new DataTypeDescriptor(TypeId.INTEGER_ID, true);
        
    /**
     * Runtime INTEGER type that is not nullable.
     */
    public static final DataTypeDescriptor INTEGER_NOT_NULL =
        INTEGER.getNullabilityType(false);
        
    /**
     * Runtime SMALLINT type that is nullable.
     */
    public static final DataTypeDescriptor SMALLINT =
        new DataTypeDescriptor(TypeId.SMALLINT_ID, true);

    /**
     * Runtime INTEGER type that is not nullable.
     */
    public static final DataTypeDescriptor SMALLINT_NOT_NULL =
        SMALLINT.getNullabilityType(false);

    /**
     * Runtime OBJECT type that is nullable.
     */
    public static final DataTypeDescriptor OBJECT = new DataTypeDescriptor(TypeId.OBJECT_ID, true);

    /*
 *** Static creators
    */

    /**
     * Get a descriptor that corresponds to a nullable builtin JDBC type.
     * If a variable length type then the size information will be set 
     * to the maximum possible.
     * 
     * Collation type will be UCS_BASIC and derivation IMPLICIT.
     * 
     * For well known types code may also use the pre-defined
     * runtime types that are fields of this class, such as INTEGER.
     *
     * @param jdbcType The int type of the JDBC type for which to get
     *                               a corresponding SQL DataTypeDescriptor
     *
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     *               A null return value means there is no corresponding SQL type
     */
    public static DataTypeDescriptor getBuiltInDataTypeDescriptor(int jdbcType) {
        return getBuiltInDataTypeDescriptor(jdbcType, true);
    }
        
    /**
     * Get a descriptor that corresponds to a nullable builtin variable
     * length JDBC type.
     *
     * Collation type will be UCS_BASIC and derivation IMPLICIT.
     * 
     * @param jdbcType  The int type of the JDBC type for which to get
     *                                          a corresponding SQL DataTypeDescriptor
     *
     * @return  A new DataTypeDescriptor that corresponds to the Java type.
     *                  A null return value means there is no corresponding SQL type
     */
    public static DataTypeDescriptor getBuiltInDataTypeDescriptor(int jdbcType, 
                                                                  int length) {
        return getBuiltInDataTypeDescriptor(jdbcType, true, length);
    }

    /**
     * Get a descriptor that corresponds to a builtin JDBC type.
     * 
     * For well known types code may also use the pre-defined
     * runtime types that are fields of this class, such as INTEGER.
     * E.g. using DataTypeDescriptor.INTEGER is preferred to
     * DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER, true)
     * (both will return the same immutable object).
     *
     * @param jdbcType The int type of the JDBC type for which to get
     *                               a corresponding SQL DataTypeDescriptor
     * @param isNullable TRUE means it could contain NULL, FALSE means
     *                                   it definitely cannot contain NULL.
     *
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     *               A null return value means there is no corresponding SQL type
     */
    public static DataTypeDescriptor getBuiltInDataTypeDescriptor(int jdbcType, 
                                                                  boolean isNullable) {
        // Re-use pre-defined types wherever possible.
        switch (jdbcType) {
        case Types.INTEGER:
            return isNullable ? INTEGER : INTEGER_NOT_NULL;
        case Types.SMALLINT:
            return isNullable ? SMALLINT : SMALLINT_NOT_NULL;
        default:
            break;
        }

        TypeId typeId = TypeId.getBuiltInTypeId(jdbcType);
        if (typeId == null) {
            return null;
        }

        return new DataTypeDescriptor(typeId, isNullable);
    }

    /**
     * Get a descriptor that corresponds to a builtin JDBC type.
     * 
     * Collation type will be UCS_BASIC and derivation IMPLICIT.
     *
     * @param jdbcType The int type of the JDBC type for which to get
     *                               a corresponding SQL DataTypeDescriptor
     * @param isNullable TRUE means it could contain NULL, FALSE means
     *                                   it definitely cannot contain NULL.
     *
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     *               A null return value means there is no corresponding SQL type
     */
    public static DataTypeDescriptor getBuiltInDataTypeDescriptor(int jdbcType, 
                                                                  boolean isNullable, 
                                                                  int maxLength) {
        TypeId typeId = TypeId.getBuiltInTypeId(jdbcType);
        if (typeId == null) {
            return null;
        }

        return new DataTypeDescriptor(typeId, isNullable, maxLength);
    }

    /**
     * Get a DataTypeDescriptor that corresponds to a nullable builtin SQL type.
     * 
     * Collation type will be UCS_BASIC and derivation IMPLICIT.
     *
     * @param sqlTypeName The name of the type for which to get
     *                                      a corresponding SQL DataTypeDescriptor
     *
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     *               A null return value means there is no corresponding SQL type (only for 'char')
     */
    public static DataTypeDescriptor getBuiltInDataTypeDescriptor(String sqlTypeName) {
        return new DataTypeDescriptor(TypeId.getBuiltInTypeId(sqlTypeName), true);
    }

    /**
     * Get a DataTypeDescriptor that corresponds to a builtin SQL type
     * 
     * Collation type will be UCS_BASIC and derivation IMPLICIT.
     *
     * @param sqlTypeName The name of the type for which to get
     *                                      a corresponding SQL DataTypeDescriptor
     *
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     *               A null return value means there is no corresponding SQL type (only for 'char')
     */
    public static DataTypeDescriptor getBuiltInDataTypeDescriptor(String sqlTypeName, 
                                                                  int length) {
        return new DataTypeDescriptor(TypeId.getBuiltInTypeId(sqlTypeName), true, length);
    }

    /**
     * Get a DataTypeDescriptor that corresponds to a Java type
     *
     * @param javaTypeName The name of the Java type for which to get
     *                                       a corresponding SQL DataTypeDescriptor
     *
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     *               A null return value means there is no corresponding SQL type (only for 'char')
     */
    public static DataTypeDescriptor getSQLDataTypeDescriptor(String javaTypeName) 
            throws StandardException {
        return getSQLDataTypeDescriptor(javaTypeName, true);
    }

    /**
     * Get a DataTypeDescriptor that corresponds to a Java type
     *
     * @param javaTypeName The name of the Java type for which to get
     *                                       a corresponding SQL DataTypeDescriptor
     * @param isNullable TRUE means it could contain NULL, FALSE means
     *                                   it definitely cannot contain NULL.
     *
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     *               A null return value means there is no corresponding SQL type (only for 'char')
     */
    public static DataTypeDescriptor getSQLDataTypeDescriptor(String javaTypeName, 
                                                              boolean isNullable) 
            throws StandardException {
        TypeId typeId = TypeId.getSQLTypeForJavaType(javaTypeName);
        if (typeId == null) {
            return null;
        }

        return new DataTypeDescriptor(typeId, isNullable);
    }

    /**
     * Get a DataTypeDescriptor that corresponds to a Java type
     *
     * @param javaTypeName The name of the Java type for which to get
     *                                       a corresponding SQL DataTypeDescriptor
     * @param precision The number of decimal digits
     * @param scale The number of digits after the decimal point
     * @param isNullable TRUE means it could contain NULL, FALSE means
     *                                   it definitely cannot contain NULL.
     * @param maximumWidth The maximum width of a data value
     *                                       represented by this type.
     *
     * @return A new DataTypeDescriptor that corresponds to the Java type.
     *               A null return value means there is no corresponding SQL type.
     */
    public static DataTypeDescriptor getSQLDataTypeDescriptor(String javaTypeName, 
                                                              int precision, int scale, 
                                                              boolean isNullable, 
                                                              int maximumWidth)
            throws StandardException {
        TypeId typeId = TypeId.getSQLTypeForJavaType(javaTypeName);
        if (typeId == null) {
            return null;
        }

        return new DataTypeDescriptor(typeId,
                                      precision,
                                      scale,
                                      isNullable,
                                      maximumWidth);
    }
        
    /**
     * Get a catalog type that corresponds to a SQL Row Multiset
     *
     * @param columnNames       Names of the columns in the Row Muliset
     * @param catalogTypes  Types of the columns in the Row Muliset
     *
     * @return A new DataTypeDescriptor describing the SQL Row Multiset
     */
    public static DataTypeDescriptor getRowMultiSet(String[] columnNames,
                                                                                                    DataTypeDescriptor[] columnTypes) {
        return new DataTypeDescriptor(TypeId.getRowMultiSet(columnNames, columnTypes),
                                      true);
    }

    /*
    ** Instance fields & methods
    */
    private TypeId typeId;
    private int precision;
    private int scale;
    private boolean isNullable;
    private int maximumWidth;
    private CharacterTypeAttributes characterAttributes;

    /**
     * Constructor for use with numeric types
     *
     * @param typeId The typeId of the type being described
     * @param precision The number of decimal digits.
     * @param scale The number of digits after the decimal point.
     * @param isNullable TRUE means it could contain NULL, FALSE means
     *                                   it definitely cannot contain NULL.
     * @param maximumWidth The maximum number of bytes for this datatype
     */
    public DataTypeDescriptor(TypeId typeId, int precision, int scale,
                              boolean isNullable, int maximumWidth) {
        this.typeId = typeId;
        this.precision = precision;
        this.scale = scale;
        this.isNullable = isNullable;
        this.maximumWidth = maximumWidth;
    }

    /**
     * Constructor for use with non-numeric types
     *
     * @param typeId The typeId of the type being described
     * @param isNullable TRUE means it could contain NULL, FALSE means
     *                                   it definitely cannot contain NULL.
     * @param maximumWidth The maximum number of bytes for this datatype
     */
    public DataTypeDescriptor(TypeId typeId, boolean isNullable,
                              int maximumWidth) {
        this.typeId = typeId;
        this.isNullable = isNullable;
        this.maximumWidth = maximumWidth;
    }

    public DataTypeDescriptor(TypeId typeId, boolean isNullable) {

        this.typeId = typeId;
        this.precision = typeId.getMaximumPrecision();
        this.scale = typeId.getMaximumScale();
        this.isNullable = isNullable;
        this.maximumWidth = typeId.getMaximumMaximumWidth();
    }

    private DataTypeDescriptor(DataTypeDescriptor source, boolean isNullable) {
        this.typeId = source.typeId;
        this.precision = source.precision;
        this.scale = source.scale;
        this.isNullable = isNullable;
        this.maximumWidth = source.maximumWidth;
        this.characterAttributes = source.characterAttributes;
    }

    private DataTypeDescriptor(DataTypeDescriptor source, 
                               int precision, int scale,
                               boolean isNullable, int maximumWidth) {
        this.typeId = source.typeId;
        this.precision = precision;
        this.scale = scale;
        this.isNullable = isNullable;
        this.maximumWidth = maximumWidth;
    }

    public DataTypeDescriptor(TypeId typeId, boolean isNullable, int maximumWidth,
                              CharacterTypeAttributes characterAttributes) {
        this.typeId = typeId;
        this.isNullable = isNullable;
        this.maximumWidth = maximumWidth;
        this.characterAttributes = characterAttributes;
    }

    public DataTypeDescriptor(DataTypeDescriptor source,
                              CharacterTypeAttributes characterAttributes) {
        this.typeId = source.typeId;
        this.precision = source.precision;
        this.scale = source.scale;
        this.isNullable = source.isNullable;
        this.maximumWidth = source.maximumWidth;
        this.characterAttributes = characterAttributes;
    }

    /**
     * Get the dominant type (DataTypeDescriptor) of the 2.
     * For variable length types, the resulting type will have the
     * biggest max length of the 2.
     * If either side is nullable, then the result will also be nullable.
     * 
     * @param otherDTS DataTypeDescriptor to compare with.
     *
     * @return DataTypeDescriptor DTS for dominant type
     *
     * @exception StandardException Thrown on error
     */
    public DataTypeDescriptor getDominantType(DataTypeDescriptor otherDTS)
            throws StandardException {
        boolean nullable;
        TypeId thisType;
        TypeId otherType;
        DataTypeDescriptor higherType;
        DataTypeDescriptor lowerType = null;
        int maximumWidth;
        int precision = getPrecision();
        int scale = getScale();

        thisType = getTypeId();
        otherType = otherDTS.getTypeId();

        /* The result is nullable if either side is nullable */
        nullable = isNullable() || otherDTS.isNullable();

        /*
        ** The result will have the maximum width of both sides
        */
        maximumWidth = (getMaximumWidth() > otherDTS.getMaximumWidth())
                      ? getMaximumWidth() : otherDTS.getMaximumWidth();

        /* We need 2 separate methods of determining type dominance - 1 if both
         * types are system built-in types and the other if at least 1 is
         * a user type. (typePrecedence is meaningless for user types.)
         */
        if (!thisType.userType() && !otherType.userType()) {
                TypeId higherTypeId;
                TypeId lowerTypeId;
                if (thisType.typePrecedence() > otherType.typePrecedence()) {
                    higherType = this;
                    lowerType = otherDTS;
                    higherTypeId = thisType;
                    lowerTypeId = otherType;
                }
                else {
                    higherType = otherDTS;
                    lowerType = this;
                    higherTypeId = otherType;
                    lowerTypeId = thisType;
                }

                //Following is checking if higher type argument is real and other argument is decimal/bigint/integer/smallint,
                //then result type should be double
                if (higherTypeId.isRealTypeId() && 
                    !lowerTypeId.isRealTypeId() && 
                    lowerTypeId.isNumericTypeId()) {
                    higherType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE);
                    higherTypeId = TypeId.getBuiltInTypeId(Types.DOUBLE);
                }
                /*
                ** If we have a DECIMAL/NUMERIC we have to do some
                ** extra work to make sure the resultant type can
                ** handle the maximum values for the two input
                ** types.    We cannot just take the maximum for
                ** precision.    E.g. we want something like:
                **
                **          DEC(10,10) and DEC(3,0) => DEC(13,10)
                **
                ** (var)char type needs some conversion handled later.
                */
                if (higherTypeId.isDecimalTypeId() && 
                    !lowerTypeId.isStringTypeId()) {
                    precision = higherTypeId.getPrecision(this, otherDTS);
                    if (precision > 31) precision = 31; //db2 silently does this and so do we
                    scale = higherTypeId.getScale(this, otherDTS);

                    /* maximumWidth needs to count possible leading '-' and
                     * decimal point and leading '0' if scale > 0.  See also
                     * sqlgrammar.jj(exactNumericType).  Beetle 3875
                     */
                    maximumWidth = (scale > 0) ? precision + 3 : precision + 1;
                }
                else if (thisType.typePrecedence() != otherType.typePrecedence()) {
                    precision = higherType.getPrecision();
                    scale = higherType.getScale();

                    /* GROSS HACKS:
                     * If we are doing an implicit (var)char->(var)bit conversion
                     * then the maximum width for the (var)char as a (var)bit
                     * is really 16 * its width as a (var)char.  Adjust
                     * maximumWidth accordingly.
                     * If we are doing an implicit (var)char->decimal conversion
                     * then we need to increment the decimal's precision by
                     * 2 * the maximum width for the (var)char and the scale
                     * by the maximum width for the (var)char. The maximumWidth
                     * becomes the new precision + 3.    This is because
                     * the (var)char could contain any decimal value from XXXXXX
                     * to 0.XXXXX.  (In other words, we don't know which side of the
                     * decimal point the characters will be on.)
                     */
                    if (lowerTypeId.isStringTypeId()) {
                        if (higherTypeId.isBitTypeId() &&
                                !higherTypeId.isLongConcatableTypeId()) {
                            if (lowerTypeId.isLongConcatableTypeId()) {
                                if (maximumWidth > (Integer.MAX_VALUE / 16))
                                    maximumWidth = Integer.MAX_VALUE;
                                else
                                    maximumWidth *= 16;
                            }
                            else {
                                int charMaxWidth;

                                int fromWidth = lowerType.getMaximumWidth();
                                if (fromWidth > (Integer.MAX_VALUE / 16))
                                    charMaxWidth = Integer.MAX_VALUE;
                                else
                                    charMaxWidth = 16 * fromWidth;

                                maximumWidth = (maximumWidth >= charMaxWidth) ?
                                    maximumWidth : charMaxWidth;
                            }
                        }
                    }

                    /*
                     * If we are doing an implicit (var)char->decimal conversion
                     * then the resulting decimal's precision could be as high as 
                     * 2 * the maximum width (precisely 2mw-1) for the (var)char
                     * and the scale could be as high as the maximum width
                     * (precisely mw-1) for the (var)char.
                     * The maximumWidth becomes the new precision + 3.  This is
                     * because the (var)char could contain any decimal value from
                     * XXXXXX to 0.XXXXX.    (In other words, we don't know which
                     * side of the decimal point the characters will be on.)
                     *
                     * We don't follow this algorithm for long varchar because the
                     * maximum length of a long varchar is maxint, and we don't
                     * want to allocate a huge decimal value.    So in this case,
                     * the precision, scale, and maximum width all come from
                     * the decimal type.
                     */
                    if (lowerTypeId.isStringTypeId() &&
                        !lowerTypeId.isLongConcatableTypeId() &&
                        higherTypeId.isDecimalTypeId()) {
                        int charMaxWidth = lowerType.getMaximumWidth();
                        int charPrecision;

                        /*
                        ** Be careful not to overflow when calculating the
                        ** precision.    Remember that we will be adding
                        ** three to the precision to get the maximum width.
                        */
                        if (charMaxWidth > (Integer.MAX_VALUE - 3) / 2)
                            charPrecision = Integer.MAX_VALUE - 3;
                        else
                            charPrecision = charMaxWidth * 2;

                        if (precision < charPrecision)
                            precision = charPrecision;

                        if (scale < charMaxWidth)
                            scale = charMaxWidth;

                        maximumWidth = precision + 3;
                    }
                }
            }
        else {
            /* At least 1 type is not a system built-in type */
            if (!thisType.equals(otherType)) {
                throw new StandardException("Two different user-defined types");
            }
            higherType = this;
            precision = higherType.getPrecision();
            scale = higherType.getScale();
        }


        higherType = new DataTypeDescriptor(higherType, 
                                            precision, scale, nullable, maximumWidth);

        higherType.characterAttributes = 
            CharacterTypeAttributes.mergeCollations(characterAttributes, 
                                                    otherDTS.characterAttributes);

        return higherType;
    }

    /**
     * Get maximum width.
     */
    public int getMaximumWidth() {
        return maximumWidth;
    }

    /**
     * Gets the TypeId for the datatype.
     *
     * @return The TypeId for the datatype.
     */
    public TypeId getTypeId() {
        return typeId;
    }

    /**
     * Gets the name of this datatype.
     * 
     *
     *  @return the name of this datatype
     */
    public String getTypeName() {
        return typeId.getSQLTypeName();
    }

    /**
     * Get the jdbc type id for this type.  JDBC type can be
     * found in java.sql.Types. 
     *
     * @return a jdbc type, e.g. java.sql.Types.DECIMAL 
     *
     * @see Types
     */
    public int getJDBCTypeId() {
        return typeId.getJDBCTypeId();
    }

    /**
     * Returns the number of decimal digits for the datatype, if applicable.
     *
     * @return The number of decimal digits for the datatype.    Returns
     *               zero for non-numeric datatypes.
     * @see TypeDescriptor#getPrecision()
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * Returns the number of digits to the right of the decimal for
     * the datatype, if applicable.
     *
     * @return The number of digits to the right of the decimal for
     *               the datatype.  Returns zero for non-numeric datatypes.
     * @see TypeDescriptor#getScale()
     */
    public int getScale() {
        return scale;
    }

    /**
     * Returns TRUE if the datatype can contain NULL, FALSE if not.
     * JDBC supports a return value meaning "nullability unknown" -
     * I assume we will never have columns where the nullability is unknown.
     *
     * @return TRUE if the datatype can contain NULL, FALSE if not.
     */
    public boolean isNullable() {
        return isNullable;
    }
        
    public boolean isRowMultiSet() {
        return typeId.isRowMultiSet();
    }

    /**
     * Return a type descriptor identical to the this type
     * with the exception of its nullability. If the nullablity
     * required matches the nullability of this then this is returned.
     * 
     * @param isNullable True to return a nullable type, false otherwise.
     */
    public DataTypeDescriptor getNullabilityType(boolean isNullable) {
        if (isNullable() == isNullable)
            return this;
                
        return new DataTypeDescriptor(this, isNullable);
    }

    public CharacterTypeAttributes getCharacterAttributes() {
        return characterAttributes;
    }

    /**
     * Compare if two DataTypeDescriptors are exactly the same
     * @param other the type to compare to.
    */
    public boolean equals(Object other) {
        if (!(other instanceof DataTypeDescriptor))
            return false;
                
        DataTypeDescriptor odtd = (DataTypeDescriptor)other;
        if (!this.getTypeName().equals(odtd.getTypeName()) ||
            this.precision != odtd.getPrecision() ||
            this.scale != odtd.getScale() ||
            this.isNullable != odtd.isNullable() ||
            this.maximumWidth != odtd.getMaximumWidth() ||
            ((this.characterAttributes == null) ? (odtd.characterAttributes != null) : !this.characterAttributes.equals(odtd.characterAttributes)))
            return false;
        else
            return true;
    }

    /**
     * Check if this type is comparable with the passed type.
     * 
     * @param compareWithDTD the type of the instance to compare with this type.
     * @param forEquals True if this is an = or <> comparison, false
     *                                  otherwise.
     * @return true if compareWithDTD is comparable to this type, else false.
     */
    public boolean comparable(DataTypeDescriptor compareWithDTD, boolean forEquals) {
        TypeId compareWithTypeID = compareWithDTD.getTypeId();
        int compareWithJDBCTypeId = compareWithTypeID.getJDBCTypeId();

        // Incomparable types include:
            // XML (SQL/XML[2003] spec, section 4.2.2)
            // ref types
        if (!typeId.isComparable() || !compareWithTypeID.isComparable())
            return false;
        
        // if the two types are equal, they should be comparable
        if (typeId.equals(compareWithTypeID))
            return true;
        
        //If this DTD is not user defined type but the DTD to be compared with 
        //is user defined type, then let the other DTD decide what should be the
        //outcome of the comparable method.
        if (!(typeId.isUserDefinedTypeId()) && 
            (compareWithTypeID.isUserDefinedTypeId()))
            return compareWithDTD.comparable(this, forEquals);

        //Numeric types are comparable to numeric types
        if (typeId.isNumericTypeId())
            return (compareWithTypeID.isNumericTypeId());

        //CHAR, VARCHAR and LONGVARCHAR are comparable to strings, boolean, 
        //DATE/TIME/TIMESTAMP and to comparable user types
        if (typeId.isStringTypeId()) {
            if((compareWithTypeID.isDateTimeTimeStampTypeID() ||
                compareWithTypeID.isBooleanTypeId()))
                return true;
            //If both the types are string types, then we need to make sure
            //they have the same collation set on them
            if (compareWithTypeID.isStringTypeId() && typeId.isStringTypeId()) {
                return true; // TODO: compareCollationInfo(compareWithDTD);
            } 
            else
                return false;                       //can't be compared
        }

        //Are comparable to other bit types and comparable user types
        if (typeId.isBitTypeId()) 
            return (compareWithTypeID.isBitTypeId()); 

        //Booleans are comparable to Boolean, string, and to 
        //comparable user types. As part of the work on DERYB-887,
        //I removed the comparability of booleans to numerics; I don't
        //understand the previous statement about comparable user types.
        //I suspect that is wrong and should be addressed when we
        //re-enable UDTs (see DERBY-651).
        if (typeId.isBooleanTypeId())
            return (compareWithTypeID.getSQLTypeName().equals(typeId.getSQLTypeName()) ||
                    compareWithTypeID.isStringTypeId()); 

        //Dates are comparable to dates, strings and to comparable
        //user types.
        if (typeId.getJDBCTypeId() == Types.DATE)
            if (compareWithJDBCTypeId == Types.DATE || 
                compareWithJDBCTypeId == Types.TIMESTAMP || 
                compareWithTypeID.isStringTypeId())
                return true;
            else
                return false;

        //Times are comparable to times, strings and to comparable
        //user types.
        if (typeId.getJDBCTypeId() == Types.TIME)
            if (compareWithJDBCTypeId == Types.TIME || 
                compareWithTypeID.isStringTypeId())
                return true;
            else
                return false;

        //Timestamps are comparable to timestamps, strings and to
        //comparable user types.
        if (typeId.getJDBCTypeId() == Types.TIMESTAMP)
            if (compareWithJDBCTypeId == Types.TIMESTAMP || 
                compareWithJDBCTypeId == Types.DATE || 
                compareWithTypeID.isStringTypeId())
                return true;
            else
                return false;

        return false;
    }

    /**
     * Converts this data type descriptor (including length/precision)
     * to a string. E.g.
     *
     *   VARCHAR(30)
     *
     * or
     *
     *   java.util.Hashtable 
     *
     * @return String version of datatype, suitable for running through
     *               the Parser.
     */
    public String getSQLstring() {
        return typeId.toParsableString(this);
    }

    /**
     * Compare JdbcTypeIds to determine if they represent equivalent
     * SQL types. For example Types.NUMERIC and Types.DECIMAL are
     * equivalent
     *
     * @param existingType  JDBC type id of Derby data type
     * @param jdbcTypeId     JDBC type id passed in from application.
     *
     * @return boolean true if types are equivalent, false if not
     */

    public static boolean isJDBCTypeEquivalent(int existingType, int jdbcTypeId) {
        // Any type matches itself.
        if (existingType == jdbcTypeId)
            return true;

        // To a numeric type
        if (isNumericType(existingType)) {
            if (isNumericType(jdbcTypeId))
                return true;

            if (isCharacterType(jdbcTypeId))
                return true;

            return false;
        }

        // To character type.
        if (isCharacterType(existingType)) {

            if (isCharacterType(jdbcTypeId))
                return true;

            if (isNumericType(jdbcTypeId))
                return true;


            switch (jdbcTypeId) {
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return true;
            default:
                break;
            }
 
            return false;

        }

        // To binary type
        if (isBinaryType(existingType)) {

            if (isBinaryType(jdbcTypeId))
                return true;

            return false;
        }

        // To DATE, TIME
        if (existingType == Types.DATE || existingType == Types.TIME) {
            if (isCharacterType(jdbcTypeId))
                return true;

            if (jdbcTypeId == Types.TIMESTAMP)
                return true;

            return false;
        }

        // To TIMESTAMP
        if (existingType == Types.TIMESTAMP) {
            if (isCharacterType(jdbcTypeId))
                return true;

            if (jdbcTypeId == Types.DATE)
                return true;

            return false;
        }

        // To CLOB
        if (existingType == Types.CLOB && isCharacterType(jdbcTypeId))
            return true;

        return false;
    }

    public static boolean isNumericType(int jdbcType) {

        switch (jdbcType) {
        case Types.BIT:
        case Types.BOOLEAN:
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
        case Types.REAL:
        case Types.FLOAT:
        case Types.DOUBLE:
        case Types.DECIMAL:
        case Types.NUMERIC:
            return true;
        default:
            return false;
        }
    }

    /**
     * Check whether a JDBC type is one of the character types that are
     * compatible with the Java type <code>String</code>.
     *
     * <p><strong>Note:</strong> <code>CLOB</code> is not compatible with
     * <code>String</code>. See tables B-4, B-5 and B-6 in the JDBC 3.0
     * Specification.
     *
     * <p> There are some non-character types that are compatible with
     * <code>String</code> (examples: numeric types, binary types and
     * time-related types), but they are not covered by this method.
     *
     * @param jdbcType a JDBC type
     * @return <code>true</code> iff <code>jdbcType</code> is a character type
     * and compatible with <code>String</code>
     * @see java.sql.Types
     */
    private static boolean isCharacterType(int jdbcType) {

        switch (jdbcType) {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return true;
        default:
            return false;
        }
    }

    /**
     * Check whether a JDBC type is compatible with the Java type
     * <code>byte[]</code>.
     *
     * <p><strong>Note:</strong> <code>BLOB</code> is not compatible with
     * <code>byte[]</code>. See tables B-4, B-5 and B-6 in the JDBC 3.0
     * Specification.
     *
     * @param jdbcType a JDBC type
     * @return <code>true</code> iff <code>jdbcType</code> is compatible with
     * <code>byte[]</code>
     * @see java.sql.Types
     */
    private static boolean isBinaryType(int jdbcType) {
        switch (jdbcType) {
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            return true;
        default:
            return false;
        }
    }

    /**
     * Determine if an ASCII stream can be inserted into a column or parameter
     * of type <code>jdbcType</code>.
     *
     * @param jdbcType JDBC type of column or parameter
     * @return <code>true</code> if an ASCII stream can be inserted;
     *               <code>false</code> otherwise
     */
    public static boolean isAsciiStreamAssignable(int jdbcType) {
        return jdbcType == Types.CLOB || isCharacterType(jdbcType);
    }

    /**
     * Determine if a binary stream can be inserted into a column or parameter
     * of type <code>jdbcType</code>.
     *
     * @param jdbcType JDBC type of column or parameter
     * @return <code>true</code> if a binary stream can be inserted;
     *               <code>false</code> otherwise
     */
    public static boolean isBinaryStreamAssignable(int jdbcType) {
        return jdbcType == Types.BLOB || isBinaryType(jdbcType);
    }

    /**
     * Determine if a character stream can be inserted into a column or
     * parameter of type <code>jdbcType</code>.
     *
     * @param jdbcType JDBC type of column or parameter
     * @return <code>true</code> if a character stream can be inserted;
     *               <code>false</code> otherwise
     */
    public static boolean isCharacterStreamAssignable(int jdbcType) {
        // currently, we support the same types for ASCII streams and
        // character streams
        return isAsciiStreamAssignable(jdbcType);
    }

    public String toString() {
        String s = getSQLstring();
        if (characterAttributes != null)
            s += " " + characterAttributes;
        if (!isNullable())
            s += " NOT NULL";
        return s;
    }

    /**
     * Return the SQL type name and, if applicable, scale/precision/length
     * for this DataTypeDescriptor.  Note that we want the values from *this*
     * object specifically, not the max values defined on this.typeId.
     */
    public String getFullSQLTypeName() {
        StringBuffer sbuf = new StringBuffer(typeId.getSQLTypeName());
        if (typeId.isDecimalTypeId() || typeId.isNumericTypeId()) {
            sbuf.append("(");
            sbuf.append(getPrecision());
            sbuf.append(", ");
            sbuf.append(getScale());
            sbuf.append(")");
        }
        else if (typeId.isIntervalTypeId()) {
            if (typeId == TypeId.INTERVAL_SECOND_ID) {
                if (getPrecision() > 0) {
                    sbuf.append("(");
                    sbuf.append(getPrecision());
                    if (getScale() > 0) {
                        sbuf.append(", ");
                        sbuf.append(getScale());
                    }
                    sbuf.append(")");
                }
            }
            else {
                if (getPrecision() > 0) {
                    int idx = sbuf.indexOf(" ", 9);
                    if (idx < 0) idx = sbuf.length();
                    sbuf.insert(idx, ")");
                    sbuf.insert(idx, getPrecision());
                    sbuf.insert(idx, "(");
                }
                if (getScale() > 0) {
                    sbuf.append("(");
                    sbuf.append(getScale());
                    sbuf.append(")");
                }
            }
        }
        else if (typeId.variableLength()) {
            sbuf.append("(");
            sbuf.append(getMaximumWidth());
            sbuf.append(")");
        }

        return sbuf.toString();
    }

    /**
     * Compute the maximum width (column display width) of a decimal or numeric data value,
     * given its precision and scale.
     *
     * @param precision The precision (number of digits) of the data value.
     * @param scale The number of fractional digits (digits to the right of the decimal point).
     *
     * @return The maximum number of characters needed to display the value.
     */
    public static int computeMaxWidth (int precision, int scale) {
        // There are 3 possible cases with respect to finding the correct max
        // width for DECIMAL type.
        // 1. If scale = 0, only sign should be added to precision.
        // 2. scale = precision, 3 should be added to precision for sign, decimal and an additional char '0'.
        // 3. precision > scale > 0, 2 should be added to precision for sign and decimal.
        return (scale == 0) ? (precision + 1) : ((scale == precision) ? (precision + 3) : (precision + 2));
    }

    public DataTypeDescriptor getUnsigned() throws StandardException {

        TypeId unsignedTypeId;
        if (typeId == TypeId.SMALLINT_ID)
            unsignedTypeId = TypeId.SMALLINT_UNSIGNED_ID;
        else if (typeId == TypeId.MEDIUMINT_ID)
            unsignedTypeId = TypeId.MEDIUMINT_UNSIGNED_ID;
        else if (typeId == TypeId.INTEGER_ID)
            unsignedTypeId = TypeId.INTEGER_UNSIGNED_ID;
        else if (typeId == TypeId.TINYINT_ID)
            unsignedTypeId = TypeId.TINYINT_UNSIGNED_ID;
        else if (typeId == TypeId.BIGINT_ID)
            unsignedTypeId = TypeId.BIGINT_UNSIGNED_ID;
        else if (typeId == TypeId.REAL_ID)
            unsignedTypeId = TypeId.REAL_UNSIGNED_ID;
        else if (typeId == TypeId.DOUBLE_ID)
            unsignedTypeId = TypeId.DOUBLE_UNSIGNED_ID;
        else if (typeId == TypeId.DECIMAL_ID)
            unsignedTypeId = TypeId.DECIMAL_UNSIGNED_ID;
        else if (typeId == TypeId.NUMERIC_ID)
            unsignedTypeId = TypeId.NUMERIC_UNSIGNED_ID;
        else
            throw new StandardException("Not a numeric type: " + this);            
        return new DataTypeDescriptor(unsignedTypeId, precision, scale,
                                      isNullable, maximumWidth);
    }

    public static int intervalMaxWidth(TypeId typeId, 
                                       int precision, int scale) {
        int maxMax;
        if (typeId.getTypeFormatId() == TypeId.FormatIds.INTERVAL_YEAR_MONTH_ID) {
            if (precision == 0)
                precision = TypeId.INTERVAL_YEAR_MONTH_PRECISION;
            maxMax = TypeId.INTERVAL_YEAR_MONTH_MAXWIDTH;
        }
        else {
            if (precision == 0)
                precision = TypeId.INTERVAL_DAY_SECOND_PRECISION;
            maxMax = TypeId.INTERVAL_DAY_SECOND_MAXWIDTH;
        }
        if ((typeId == TypeId.INTERVAL_YEAR_ID) ||
            (typeId == TypeId.INTERVAL_MONTH_ID) ||
            (typeId == TypeId.INTERVAL_DAY_ID) ||
            (typeId == TypeId.INTERVAL_HOUR_ID) ||
            (typeId == TypeId.INTERVAL_MINUTE_ID)) {
            return precision;
        }
        else if (typeId == TypeId.INTERVAL_SECOND_ID) {
            if (scale == 0)
                return precision;
            else
                return precision + scale + 1;
        }
        else if (typeId == TypeId.INTERVAL_DAY_HOUR_ID) {
            return precision + 3;
        }
        else if (typeId == TypeId.INTERVAL_DAY_MINUTE_ID) {
            return precision + 6;
        }
        else if (typeId == TypeId.INTERVAL_DAY_SECOND_ID) {
            if (scale == 0)
                return precision + 9;
            else
                return precision + scale + 10;
        }
        else if (typeId == TypeId.INTERVAL_HOUR_MINUTE_ID) {
            return precision + 3;
        }
        else if (typeId == TypeId.INTERVAL_HOUR_SECOND_ID) {
            if (scale == 0)
                return precision + 6;
            else
                return precision + scale + 7;
        }
        else if (typeId == TypeId.INTERVAL_MINUTE_SECOND_ID) {
            if (scale == 0)
                return precision + 3;
            else
                return precision + scale + 4;
        }
        else
            return maxMax;
    }
}
