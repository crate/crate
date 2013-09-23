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

Derby - Class org.apache.derby.iapi.types.TypeId

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

package org.cratedb.sql.parser.types;

import org.cratedb.sql.parser.StandardException;

import java.sql.Types;

/**
 * TypeId describes the static information about a SQL type
 * independent of any specific attributes of the type such
 * as length. So the TypeId for CHARACTER describes the
 * fundamental information about CHARACTER. A specific
 * type (e.g. CHARACTER(10)) is described by a TypeDescriptor for
 * a catlog type and a DataTypeDescriptor for a runtime type.
 * (note a DataTypeDescriptor adds runtime attributes to the
 * TypeDescriptor it has).
 * <P>
 * A TypeId is immutable.
 <P>
 * The equals(Object) method can be used to determine if two typeIds are for the same type,
 * which defines type id equality.


 @see DataTypeDescriptor
*/

public class TypeId
{
    /**
     * Various fixed numbers related to datatypes.
     */
    public static final int LONGINT_PRECISION = 19;
    public static final int LONGINT_SCALE = 0;
    public static final int LONGINT_MAXWIDTH = 20;

    public static final int INT_PRECISION = 10;
    public static final int INT_SCALE = 0;
    public static final int INT_MAXWIDTH = 11;

    public static final int SMALLINT_PRECISION = 5;
    public static final int SMALLINT_SCALE = 0;
    public static final int SMALLINT_MAXWIDTH = 6;

    public static final int TINYINT_PRECISION = 3;
    public static final int TINYINT_SCALE = 0;
    public static final int TINYINT_MAXWIDTH = 4;

    // precision in number of bits 
    public static final int DOUBLE_PRECISION = 52;
    // the ResultSetMetaData needs to have the precision for numeric data
    // in decimal digits, rather than number of bits, so need a separate constant.
    public static final int DOUBLE_PRECISION_IN_DIGITS = 15;
    public static final int DOUBLE_SCALE = 0;
    public static final int DOUBLE_MAXWIDTH = 17;

    // precision in number of bits 
    public static final int REAL_PRECISION = 23;
    // the ResultSetMetaData needs to have the precision for numeric data
    // in decimal digits, rather than number of bits, so need a separate constant.
    public static final int REAL_PRECISION_IN_DIGITS = 7;
    public static final int REAL_SCALE = 0;
    public static final int REAL_MAXWIDTH = 9;

    public static final int DECIMAL_PRECISION = 31;
    public static final int DECIMAL_SCALE = 31;
    public static final int DECIMAL_MAXWIDTH = 31;
    // TODO: Is there a better place for these?
    public static final int DEFAULT_DECIMAL_PRECISION = 5;
    public static final int DEFAULT_DECIMAL_SCALE = 0;

    public static final int BOOLEAN_MAXWIDTH = 5; // false

    public static final int CHAR_MAXWIDTH = 254;
    public static final int VARCHAR_MAXWIDTH = 32672;
    public static final int LONGVARCHAR_MAXWIDTH = 32700;
    public static final int BIT_MAXWIDTH = 254;
    public static final int VARBIT_MAXWIDTH = 32672;
    public static final int LONGVARBIT_MAXWIDTH = 32700;

    // not supposed to be limited! 4096G should be ok(?), if Derby can handle...
    public static final int BLOB_MAXWIDTH = Integer.MAX_VALUE; // to change long
    public static final int CLOB_MAXWIDTH = Integer.MAX_VALUE; // to change long
    public static final int XML_MAXWIDTH = Integer.MAX_VALUE;

    // Max width for datetime values is the length of the
    // string returned from a call to "toString()" on the
    // java.sql.Date, java.sql.Time, and java.sql.Timestamp
    // classes (the result of toString() on those classes
    // is defined by the JDBC API).  This value is also
    // used as the "precision" for those types.
    public static final int DATE_MAXWIDTH = 10; // yyyy-mm-dd
    public static final int TIME_MAXWIDTH = 8; // hh:mm:ss

    // The format of java.sql.Timestamp.toString()
    // is yyyy-mm-dd hh:mm:ss.fffffffff
    public static final int TIMESTAMP_MAXWIDTH = 29; // yyyy-mm-dd hh:mm:ss.fffffffff

    // Scale DOES exist for time values.    For a TIMESTAMP value,
    // it's 9 ('fffffffff'); for a TIME value, it's 0 (because there
    // are no fractional seconds).  Note that date values do
    // not have a scale.
    public static final int TIME_SCALE = 0;
    public static final int TIMESTAMP_SCALE = 9;

    public static final int INTERVAL_YEAR_MONTH_PRECISION = 8;
    public static final int INTERVAL_YEAR_MONTH_SCALE = 0;
    public static final int INTERVAL_YEAR_MONTH_MAXWIDTH = 11; // yyyyyyyy-mm
    public static final int INTERVAL_DAY_SECOND_PRECISION = 8;
    public static final int INTERVAL_DAY_SECOND_SCALE = 6;
    public static final int INTERVAL_DAY_SECOND_MAXWIDTH = 24; // dddddddd hh:mm:ss.uuuuuu

    /* These define all the type names for SQL92 and JDBC 
     * NOTE: boolean is SQL3
     */
    //public static final String BIT_NAME = "BIT";
    //public static final String VARBIT_NAME = "BIT VARYING";
    //public static final String LONGVARBIT_NAME = "LONG BIT VARYING";

    public static final String BIT_NAME = "CHAR () FOR BIT DATA";
    public static final String VARBIT_NAME = "VARCHAR () FOR BIT DATA";
    public static final String LONGVARBIT_NAME = "LONG VARCHAR FOR BIT DATA";
    public static final String TINYINT_NAME = "TINYINT";
    public static final String SMALLINT_NAME = "SMALLINT";
    public static final String MEDIUMINT_NAME = "MEDIUMINT";
    public static final String INTEGER_NAME = "INTEGER";
    public static final String INT_NAME = "INT";
    public static final String LONGINT_NAME = "BIGINT";
    public static final String FLOAT_NAME = "FLOAT";
    public static final String REAL_NAME = "REAL";
    public static final String DOUBLE_NAME = "DOUBLE";
    public static final String NUMERIC_NAME = "NUMERIC";
    public static final String DECIMAL_NAME = "DECIMAL";
    public static final String CHAR_NAME = "CHAR";
    public static final String VARCHAR_NAME = "VARCHAR";
    public static final String LONGVARCHAR_NAME = "LONG VARCHAR";
    public static final String DATE_NAME = "DATE";
    public static final String TIME_NAME = "TIME";
    public static final String TIMESTAMP_NAME = "TIMESTAMP";
    public static final String BINARY_NAME = "BINARY";
    public static final String VARBINARY_NAME = "VARBINARY";
    public static final String LONGVARBINARY_NAME = "LONGVARBINARY";
    public static final String BOOLEAN_NAME = "BOOLEAN";
    public static final String REF_NAME = "REF";
    public static final String NATIONAL_CHAR_NAME = "NATIONAL CHAR";
    public static final String NATIONAL_VARCHAR_NAME = "NATIONAL CHAR VARYING";
    public static final String NATIONAL_LONGVARCHAR_NAME = "LONG NVARCHAR";
    public static final String BLOB_NAME = "BLOB";
    public static final String CLOB_NAME = "CLOB";
    public static final String NCLOB_NAME = "NCLOB";
    public static final String TEXT_NAME = "TEXT";
    public static final String TINYBLOB_NAME = "TINYBLOB";
    public static final String TINYTEXT_NAME = "TINYTEXT";
    public static final String MEDIUMBLOB_NAME = "MEDIUMBLOB";
    public static final String MEDIUMTEXT_NAME = "MEDIUMTEXT";
    public static final String LONGBLOB_NAME = "LONGBLOB";
    public static final String LONGTEXT_NAME = "LONGTEXT";
    public static final String INTERVAL_YEAR_NAME = "INTERVAL YEAR";
    public static final String INTERVAL_MONTH_NAME = "INTERVAL MONTH";
    public static final String INTERVAL_YEAR_MONTH_NAME = "INTERVAL YEAR TO MONTH";
    public static final String INTERVAL_DAY_NAME = "INTERVAL DAY";
    public static final String INTERVAL_HOUR_NAME = "INTERVAL HOUR";
    public static final String INTERVAL_MINUTE_NAME = "INTERVAL MINUTE";
    public static final String INTERVAL_SECOND_NAME = "INTERVAL SECOND";
    public static final String INTERVAL_DAY_HOUR_NAME = "INTERVAL DAY TO HOUR";
    public static final String INTERVAL_DAY_MINUTE_NAME = "INTERVAL DAY TO MINUTE";
    public static final String INTERVAL_DAY_SECOND_NAME = "INTERVAL DAY TO SECOND";
    public static final String INTERVAL_HOUR_MINUTE_NAME = "INTERVAL HOUR TO MINUTE";
    public static final String INTERVAL_HOUR_SECOND_NAME = "INTERVAL HOUR TO SECOND";
    public static final String INTERVAL_MINUTE_SECOND_NAME = "INTERVAL MINUTE TO SECOND";

    // Following use of "XML" is per SQL/XML (2003) spec,
    // section "10.2 Type name determination".
    public static final String XML_NAME = "XML";
                
    // ARRAY and STRUCT are JDBC 2.0 data types that are not
    // supported by Derby.
    public static final String ARRAY_NAME = "ARRAY";
    public static final String STRUCT_NAME = "STRUCT";

    // DATALINK is a JDBC 3.0 data type. Not supported by Derby.
    public static final String DATALINK_NAME = "DATALINK";

    // ROWID and SQLXML are new types in JDBC 4.0. Not supported
    // by Derby.
    public static final String ROWID_NAME = "ROWID";
    public static final String SQLXML_NAME = "SQLXML";

    // MySQL compatible types.
    public static final String TINYINT_UNSIGNED_NAME = "TINYINT UNSIGNED";
    public static final String SMALLINT_UNSIGNED_NAME = "SMALLINT UNSIGNED";
    public static final String MEDIUMINT_UNSIGNED_NAME = "MEDIUMINT UNSIGNED";
    public static final String INTEGER_UNSIGNED_NAME = "INTEGER UNSIGNED";
    public static final String INT_UNSIGNED_NAME = "INT UNSIGNED";
    public static final String LONGINT_UNSIGNED_NAME = "BIGINT UNSIGNED";
    public static final String FLOAT_UNSIGNED_NAME = "FLOAT UNSIGNED";
    public static final String REAL_UNSIGNED_NAME = "REAL UNSIGNED";
    public static final String DOUBLE_UNSIGNED_NAME = "DOUBLE UNSIGNED";
    public static final String NUMERIC_UNSIGNED_NAME = "NUMERIC UNSIGNED";
    public static final String DECIMAL_UNSIGNED_NAME = "DECIMAL UNSIGNED";
    public static final String DATETIME_NAME = "DATETIME";
    public static final String YEAR_NAME = "YEAR";

    /**
     * The following constants define the type precedence hierarchy.
     */
    public static final int USER_PRECEDENCE  = 1000;

    public static final int XML_PRECEDENCE = 180;
    public static final int BLOB_PRECEDENCE = 170;
    public static final int LONGVARBIT_PRECEDENCE = 160;
    public static final int VARBIT_PRECEDENCE = 150;
    public static final int BIT_PRECEDENCE = 140;
    public static final int BOOLEAN_PRECEDENCE = 130;
    public static final int INTERVAL_PRECEDENCE = 125;
    public static final int TIME_PRECEDENCE = 120;
    public static final int TIMESTAMP_PRECEDENCE = 110;
    public static final int DATE_PRECEDENCE = 100;
    public static final int DOUBLE_PRECEDENCE = 90;
    public static final int REAL_PRECEDENCE = 80;
    public static final int DECIMAL_PRECEDENCE = 70;
    public static final int NUMERIC_PRECEDENCE = 69;
    public static final int LONGINT_PRECEDENCE = 60;
    public static final int INT_PRECEDENCE = 50;
    public static final int SMALLINT_PRECEDENCE = 40;
    public static final int TINYINT_PRECEDENCE = 30;
    public static final int REF_PRECEDENCE = 25;
    public static final int CLOB_PRECEDENCE = 14;
    public static final int LONGVARCHAR_PRECEDENCE = 12;
    public static final int VARCHAR_PRECEDENCE = 10;
    public static final int CHAR_PRECEDENCE = 0;

    // This makes it easier to keep the modularity somewhat similar but
    // without all the extra instances.
    public static class FormatIds {
        public static final int BIT_TYPE_ID = 0;
        public static final int BOOLEAN_TYPE_ID = 1;
        public static final int CHAR_TYPE_ID = 2;
        public static final int DATE_TYPE_ID = 3;
        public static final int DECIMAL_TYPE_ID = 4;
        public static final int NUMERIC_TYPE_ID = 5;
        public static final int DOUBLE_TYPE_ID = 6;
        public static final int INT_TYPE_ID = 7;
        public static final int LONGINT_TYPE_ID = 8;
        public static final int LONGVARBIT_TYPE_ID = 9;
        public static final int LONGVARCHAR_TYPE_ID = 10;
        public static final int REAL_TYPE_ID = 11;
        public static final int REF_TYPE_ID = 12;
        public static final int SMALLINT_TYPE_ID = 13;
        public static final int TIME_TYPE_ID = 14;
        public static final int TIMESTAMP_TYPE_ID = 15;
        public static final int TINYINT_TYPE_ID = 16;
        public static final int USERDEFINED_TYPE_ID = 17;
        public static final int VARBIT_TYPE_ID = 18;
        public static final int BLOB_TYPE_ID = 19;
        public static final int VARCHAR_TYPE_ID = 20;
        public static final int CLOB_TYPE_ID = 21;
        public static final int XML_TYPE_ID = 22;
        public static final int ROW_MULTISET_TYPE_ID_IMPL = 23;
        public static final int INTERVAL_YEAR_MONTH_ID = 24;
        public static final int INTERVAL_DAY_SECOND_ID = 25;
        public static final int MEDIUMINT_ID = 26;
    }

    public static final TypeId BOOLEAN_ID = new TypeId(FormatIds.BOOLEAN_TYPE_ID);
    public static final TypeId SMALLINT_ID = new TypeId(FormatIds.SMALLINT_TYPE_ID);
    public static final TypeId MEDIUMINT_ID = new TypeId(FormatIds.MEDIUMINT_ID);
    public static final TypeId INTEGER_ID = new TypeId(FormatIds.INT_TYPE_ID);
    public static final TypeId CHAR_ID = new TypeId(FormatIds.CHAR_TYPE_ID);
    public static final TypeId TINYINT_ID = new TypeId(FormatIds.TINYINT_TYPE_ID);
    public static final TypeId BIGINT_ID = new TypeId(FormatIds.LONGINT_TYPE_ID);
    public static final TypeId REAL_ID = new TypeId(FormatIds.REAL_TYPE_ID);
    public static final TypeId DOUBLE_ID = new TypeId(FormatIds.DOUBLE_TYPE_ID);
    public static final TypeId DECIMAL_ID =    new TypeId(FormatIds.DECIMAL_TYPE_ID);
    public static final TypeId NUMERIC_ID =    new TypeId(FormatIds.NUMERIC_TYPE_ID);
    public static final TypeId VARCHAR_ID = new TypeId(FormatIds.VARCHAR_TYPE_ID);
    public static final TypeId DATE_ID = new TypeId(FormatIds.DATE_TYPE_ID);
    public static final TypeId TIME_ID = new TypeId(FormatIds.TIME_TYPE_ID);
    public static final TypeId TIMESTAMP_ID = new TypeId(FormatIds.TIMESTAMP_TYPE_ID);
    public static final TypeId BIT_ID = new TypeId(FormatIds.BIT_TYPE_ID);
    public static final TypeId VARBIT_ID = new TypeId(FormatIds.VARBIT_TYPE_ID);
    public static final TypeId REF_ID = new TypeId(FormatIds.REF_TYPE_ID);
    public static final TypeId LONGVARCHAR_ID = new TypeId(FormatIds.LONGVARCHAR_TYPE_ID);
    public static final TypeId LONGVARBIT_ID = new TypeId(FormatIds.LONGVARBIT_TYPE_ID);
    public static final TypeId BLOB_ID = new TypeId(FormatIds.BLOB_TYPE_ID);
    public static final TypeId CLOB_ID = new TypeId(FormatIds.CLOB_TYPE_ID);
    public static final TypeId XML_ID = new TypeId(FormatIds.XML_TYPE_ID);

    public static final TypeId INTERVAL_YEAR_ID = new TypeId(FormatIds.INTERVAL_YEAR_MONTH_ID, INTERVAL_YEAR_NAME);
    public static final TypeId INTERVAL_MONTH_ID = new TypeId(FormatIds.INTERVAL_YEAR_MONTH_ID, INTERVAL_MONTH_NAME);
    public static final TypeId INTERVAL_YEAR_MONTH_ID = new TypeId(FormatIds.INTERVAL_YEAR_MONTH_ID, INTERVAL_YEAR_MONTH_NAME);
    public static final TypeId INTERVAL_DAY_ID = new TypeId(FormatIds.INTERVAL_DAY_SECOND_ID, INTERVAL_DAY_NAME);
    public static final TypeId INTERVAL_HOUR_ID = new TypeId(FormatIds.INTERVAL_DAY_SECOND_ID, INTERVAL_HOUR_NAME);
    public static final TypeId INTERVAL_MINUTE_ID = new TypeId(FormatIds.INTERVAL_DAY_SECOND_ID, INTERVAL_MINUTE_NAME);
    public static final TypeId INTERVAL_SECOND_ID = new TypeId(FormatIds.INTERVAL_DAY_SECOND_ID, INTERVAL_SECOND_NAME);
    public static final TypeId INTERVAL_DAY_HOUR_ID = new TypeId(FormatIds.INTERVAL_DAY_SECOND_ID, INTERVAL_DAY_HOUR_NAME);
    public static final TypeId INTERVAL_DAY_MINUTE_ID = new TypeId(FormatIds.INTERVAL_DAY_SECOND_ID, INTERVAL_DAY_MINUTE_NAME);
    public static final TypeId INTERVAL_DAY_SECOND_ID = new TypeId(FormatIds.INTERVAL_DAY_SECOND_ID, INTERVAL_DAY_SECOND_NAME);
    public static final TypeId INTERVAL_HOUR_MINUTE_ID = new TypeId(FormatIds.INTERVAL_DAY_SECOND_ID, INTERVAL_HOUR_MINUTE_NAME);
    public static final TypeId INTERVAL_HOUR_SECOND_ID = new TypeId(FormatIds.INTERVAL_DAY_SECOND_ID, INTERVAL_HOUR_SECOND_NAME);
    public static final TypeId INTERVAL_MINUTE_SECOND_ID = new TypeId(FormatIds.INTERVAL_DAY_SECOND_ID, INTERVAL_MINUTE_SECOND_NAME);

    public static final TypeId SMALLINT_UNSIGNED_ID = new TypeId(FormatIds.SMALLINT_TYPE_ID, true);
    public static final TypeId MEDIUMINT_UNSIGNED_ID = new TypeId(FormatIds.MEDIUMINT_ID, true);
    public static final TypeId INTEGER_UNSIGNED_ID = new TypeId(FormatIds.INT_TYPE_ID, true);
    public static final TypeId TINYINT_UNSIGNED_ID = new TypeId(FormatIds.TINYINT_TYPE_ID, true);
    public static final TypeId BIGINT_UNSIGNED_ID = new TypeId(FormatIds.LONGINT_TYPE_ID, true);
    public static final TypeId REAL_UNSIGNED_ID = new TypeId(FormatIds.REAL_TYPE_ID, true);
    public static final TypeId DOUBLE_UNSIGNED_ID = new TypeId(FormatIds.DOUBLE_TYPE_ID, true);
    public static final TypeId DECIMAL_UNSIGNED_ID =    new TypeId(FormatIds.DECIMAL_TYPE_ID, true);
    public static final TypeId NUMERIC_UNSIGNED_ID =    new TypeId(FormatIds.NUMERIC_TYPE_ID, true);
    public static final TypeId DATETIME_ID = new TypeId(FormatIds.TIMESTAMP_TYPE_ID, DATETIME_NAME);
    public static final TypeId YEAR_ID = new TypeId(FormatIds.SMALLINT_TYPE_ID, YEAR_NAME);

    public static final TypeId TEXT_ID = new TypeId(FormatIds.CLOB_TYPE_ID, TEXT_NAME);
    public static final TypeId TINYBLOB_ID = new TypeId(FormatIds.BLOB_TYPE_ID, TINYBLOB_NAME);
    public static final TypeId TINYTEXT_ID = new TypeId(FormatIds.CLOB_TYPE_ID, TINYTEXT_NAME);
    public static final TypeId MEDIUMBLOB_ID = new TypeId(FormatIds.BLOB_TYPE_ID, MEDIUMBLOB_NAME);
    public static final TypeId MEDIUMTEXT_ID = new TypeId(FormatIds.CLOB_TYPE_ID, MEDIUMTEXT_NAME);
    public static final TypeId LONGBLOB_ID = new TypeId(FormatIds.BLOB_TYPE_ID, LONGBLOB_NAME);
    public static final TypeId LONGTEXT_ID = new TypeId(FormatIds.CLOB_TYPE_ID, LONGTEXT_NAME);

    private static final TypeId[] ALL_BUILTIN_TYPE_IDS = {
        BOOLEAN_ID,
        SMALLINT_ID,
        MEDIUMINT_ID,
        INTEGER_ID,
        CHAR_ID,
        TINYINT_ID,
        BIGINT_ID,
        REAL_ID,
        DOUBLE_ID,
        DECIMAL_ID,
        NUMERIC_ID,
        VARCHAR_ID,
        DATE_ID,
        TIME_ID,
        TIMESTAMP_ID,
        BIT_ID,
        VARBIT_ID,
        REF_ID,
        LONGVARCHAR_ID,
        LONGVARBIT_ID,
        BLOB_ID,
        CLOB_ID,
        XML_ID,
        INTERVAL_YEAR_ID,
        INTERVAL_MONTH_ID,
        INTERVAL_YEAR_MONTH_ID,
        INTERVAL_DAY_ID,
        INTERVAL_HOUR_ID,
        INTERVAL_MINUTE_ID,
        INTERVAL_SECOND_ID,
        INTERVAL_DAY_HOUR_ID,
        INTERVAL_DAY_MINUTE_ID,
        INTERVAL_DAY_SECOND_ID,
        INTERVAL_HOUR_MINUTE_ID,
        INTERVAL_HOUR_SECOND_ID,
        INTERVAL_MINUTE_SECOND_ID,

        SMALLINT_UNSIGNED_ID,
        INTEGER_UNSIGNED_ID,
        TINYINT_UNSIGNED_ID,
        BIGINT_UNSIGNED_ID,
        REAL_UNSIGNED_ID,
        DOUBLE_UNSIGNED_ID,
        DECIMAL_UNSIGNED_ID,
        NUMERIC_UNSIGNED_ID,
        DATETIME_ID,
        YEAR_ID,

        TEXT_ID,
        TINYBLOB_ID,
        TINYTEXT_ID,
        MEDIUMBLOB_ID,
        MEDIUMTEXT_ID,
        LONGBLOB_ID,
        LONGTEXT_ID,
    };

    /*
    ** Static methods to obtain TypeIds
    */

    /**
     * Return all of the builtin type ids.
     */
    public static TypeId[] getAllBuiltinTypeIds() {
        int count = ALL_BUILTIN_TYPE_IDS.length;

        TypeId[] retval = new TypeId[count];

        for (int i = 0; i < count; i++) { 
            retval[i] = ALL_BUILTIN_TYPE_IDS[i];
        }

        return retval;
    }
                
    /**
     * Get a TypeId of the given JDBC type.  This factory method is
     * intended to be used for built-in types.  For user-defined types,
     * we will need a factory method that takes a Java type name.
     *
     * @param JDBCTypeId The JDBC Id of the type, as listed in
     *                                   java.sql.Types
     *
     * @return The appropriate TypeId, or null if there is no such
     *               TypeId.
     */

    public static TypeId getBuiltInTypeId(int JDBCTypeId) {

        switch (JDBCTypeId) {
        case Types.TINYINT:
            return TINYINT_ID;

        case Types.SMALLINT:
            return SMALLINT_ID;

        case Types.INTEGER:
            return INTEGER_ID;

        case Types.BIGINT:
            return BIGINT_ID;

        case Types.FLOAT:
        case Types.REAL:
            return REAL_ID;

        case Types.DOUBLE:
            return DOUBLE_ID;

        case Types.DECIMAL:
            return DECIMAL_ID;

        case Types.NUMERIC:
            return NUMERIC_ID;

        case Types.CHAR:
            return CHAR_ID;

        case Types.VARCHAR:
            return VARCHAR_ID;

        case Types.DATE:
            return DATE_ID;

        case Types.TIME:
            return TIME_ID;

        case Types.TIMESTAMP:
            return TIMESTAMP_ID;

        case Types.BIT:
        case Types.BOOLEAN:
            return BOOLEAN_ID;

        case Types.BINARY:
            return BIT_ID;

        case Types.VARBINARY:
            return VARBIT_ID;

        case Types.LONGVARBINARY:
            return LONGVARBIT_ID;

        case Types.LONGVARCHAR:
            return LONGVARCHAR_ID;

        case Types.BLOB:
            return BLOB_ID;

        case Types.CLOB:
            return CLOB_ID;

        case Types.SQLXML: // 2009
            return XML_ID;
                        
        default:
            return null;
        }
    }

    public static TypeId getUserDefinedTypeId(String className, 
                                              boolean delimitedIdentifier)
            throws StandardException {
        return new TypeId(className, delimitedIdentifier);
    }

    /**
     * This factory  method is used for ANSI UDTs. If the className argument is null,
     * then this TypeId will have to be bound.
     *
     * @param schemaName Schema that the type definition lives in.
     * @param unqualifiedName The second part of the ANSI dot-separated name for the type.
     * @param className The Java class which is bound to the schema-qualified name by the CREATE TYPE statement.
     *
     * @return A bound type TypeId describing this ANSI UDT.
     */
    public static TypeId getUserDefinedTypeId(String schemaName, String unqualifiedName,
                                              String className)
            throws StandardException {
        return new TypeId(schemaName, unqualifiedName, className);
    }

    /** Return true if this is this type id describes an ANSI UDT */
    public boolean isAnsiUDT() { 
        return (schemaName != null); 
    }

    /**
     * Get a TypeId for the class that corresponds to the given Java type
     * name.
     * 
     * @param javaTypeName The name of the Java type
     * 
     * @return A TypeId for the SQL type that corresponds to the Java type,
     *               null if there is no corresponding type.
     */
    public static TypeId getSQLTypeForJavaType(String javaTypeName)
            throws StandardException {
        if (javaTypeName.equals("java.lang.Boolean") ||
            javaTypeName.equals("boolean")) {
            return BOOLEAN_ID;
        }
        else if (javaTypeName.equals("byte[]")) {
            return VARBIT_ID;
        }
        else if (javaTypeName.equals("java.lang.String")) {
            return VARCHAR_ID;
        }
        else if (javaTypeName.equals("java.lang.Integer") ||
                 javaTypeName.equals("int")) {
            return INTEGER_ID;
        }
        else if (javaTypeName.equals("byte")) {
            return TINYINT_ID;
        }
        else if (javaTypeName.equals("short")) {
            return SMALLINT_ID;
        }
        else if (javaTypeName.equals("java.lang.Long") ||
                 javaTypeName.equals("long")) {
            return BIGINT_ID;
        }
        else if (javaTypeName.equals("java.lang.Float") ||
                 javaTypeName.equals("float")) {
            return REAL_ID;
        }
        else if (javaTypeName.equals("java.lang.Double") ||
                 javaTypeName.equals("double")) {
            return DOUBLE_ID;
        }
        else if (javaTypeName.equals("java.math.BigDecimal")) {
            return DECIMAL_ID;
        }
        else if (javaTypeName.equals("java.sql.Date")) {
            return DATE_ID;
        }
        else if (javaTypeName.equals("java.sql.Time")) {
            return TIME_ID;
        }
        else if (javaTypeName.equals("java.sql.Timestamp")) {
            return TIMESTAMP_ID;
        }
        else if (javaTypeName.equals("java.sql.Blob")) {
            return BLOB_ID;
        }
        else if (javaTypeName.equals("java.sql.Clob")) {
            return CLOB_ID;
        }
        else if (javaTypeName.equals("org.cratedb.sql.parser.types.XML")) {
            return XML_ID;
        }
        else {
            /*
            ** If it's a Java primitive type, return null to indicate that
            ** there is no corresponding SQL type (all the Java primitive
            ** types that have corresponding SQL types are handled above).
            **
            ** There is only one primitive type not mentioned above, char.
            */
            if (javaTypeName.equals("char")) {
                return null;
            }

            /*
            ** It's a non-primitive type (a class) that does not correspond
            ** to a SQL built-in type, so treat it as a user-defined type.
            */
            return TypeId.getUserDefinedTypeId(javaTypeName, false);
        }
    }

    /**
     * Given a SQL type name return the corresponding TypeId.
     * @param SQLTypeName Name of SQL type
     * @return TypeId or null if there is no corresponding SQL type.
     */
    public static TypeId getBuiltInTypeId(String SQLTypeName) {

        if (SQLTypeName.equals(BOOLEAN_NAME)) {
            return BOOLEAN_ID;
        }
        if (SQLTypeName.equals(CHAR_NAME)) {
            return CHAR_ID;
        }
        if (SQLTypeName.equals(DATE_NAME)) {
            return DATE_ID;
        }
        if (SQLTypeName.equals(DOUBLE_NAME)) {
            return DOUBLE_ID;
        }
        if (SQLTypeName.equals(FLOAT_NAME)) {
            return REAL_ID;
        }
        if (SQLTypeName.equals(MEDIUMINT_NAME))
            return MEDIUMINT_ID;
        if (SQLTypeName.equals(INTEGER_NAME) ||
            SQLTypeName.equals(INT_NAME)) {
            return INTEGER_ID;
        }
        if (SQLTypeName.equals(LONGINT_NAME)) {
            return BIGINT_ID;
        }
        if (SQLTypeName.equals(REAL_NAME)) {
            return REAL_ID;
        }
        if (SQLTypeName.equals(SMALLINT_NAME)) {
            return SMALLINT_ID;
        }
        if (SQLTypeName.equals(TIME_NAME)) {
            return TIME_ID;
        }
        if (SQLTypeName.equals(TIMESTAMP_NAME)) {
            return TIMESTAMP_ID;
        }
        if (SQLTypeName.equals(VARCHAR_NAME)) {
            return VARCHAR_ID;
        }
        if (SQLTypeName.equals(BIT_NAME)) {
            return BIT_ID;
        }
        if (SQLTypeName.equals(VARBIT_NAME)) {
            return VARBIT_ID;
        }
        if (SQLTypeName.equals(TINYINT_NAME)) {
            return TINYINT_ID;
        }
        if (SQLTypeName.equals(DECIMAL_NAME)) {
            return DECIMAL_ID;
        }
        if (SQLTypeName.equals(NUMERIC_NAME)) {
            return NUMERIC_ID;
        }
        if (SQLTypeName.equals(LONGVARCHAR_NAME)) {
            return LONGVARCHAR_ID;
        }
        if (SQLTypeName.equals(LONGVARBIT_NAME)) {
            return LONGVARBIT_ID;
        }
        if (SQLTypeName.equals(BLOB_NAME)) {
            return BLOB_ID;
        }
        if (SQLTypeName.equals(CLOB_NAME)) {
            return CLOB_ID;
        }
        if (SQLTypeName.equals(TEXT_NAME)) {
            return TEXT_ID;
        }
        if (SQLTypeName.equals(TINYBLOB_NAME)) {
            return TINYBLOB_ID;
        }
        if (SQLTypeName.equals(TINYTEXT_NAME)) {
            return TINYTEXT_ID;
        }
        if (SQLTypeName.equals(MEDIUMBLOB_NAME)) {
            return MEDIUMBLOB_ID;
        }
        if (SQLTypeName.equals(MEDIUMTEXT_NAME)) {
            return MEDIUMTEXT_ID;
        }
        if (SQLTypeName.equals(LONGBLOB_NAME)) {
            return LONGBLOB_ID;
        }
        if (SQLTypeName.equals(LONGTEXT_NAME)) {
            return LONGTEXT_ID;
        }
        if (SQLTypeName.equals(XML_NAME)) {
            return XML_ID;
        }
        if (SQLTypeName.equals(INTERVAL_YEAR_NAME)) {
            return INTERVAL_YEAR_ID;
        }
        if (SQLTypeName.equals(INTERVAL_MONTH_NAME)) {
            return INTERVAL_MONTH_ID;
        }
        if (SQLTypeName.equals(INTERVAL_YEAR_MONTH_NAME)) {
            return INTERVAL_YEAR_MONTH_ID;
        }
        if (SQLTypeName.equals(INTERVAL_DAY_NAME)) {
            return INTERVAL_DAY_ID;
        }
        if (SQLTypeName.equals(INTERVAL_HOUR_NAME)) {
            return INTERVAL_HOUR_ID;
        }
        if (SQLTypeName.equals(INTERVAL_MINUTE_NAME)) {
            return INTERVAL_MINUTE_ID;
        }
        if (SQLTypeName.equals(INTERVAL_SECOND_NAME)) {
            return INTERVAL_SECOND_ID;
        }
        if (SQLTypeName.equals(INTERVAL_DAY_HOUR_NAME)) {
            return INTERVAL_DAY_HOUR_ID;
        }
        if (SQLTypeName.equals(INTERVAL_DAY_MINUTE_NAME)) {
            return INTERVAL_DAY_MINUTE_ID;
        }
        if (SQLTypeName.equals(INTERVAL_DAY_SECOND_NAME)) {
            return INTERVAL_DAY_SECOND_ID;
        }
        if (SQLTypeName.equals(INTERVAL_HOUR_MINUTE_NAME)) {
            return INTERVAL_HOUR_MINUTE_ID;
        }
        if (SQLTypeName.equals(INTERVAL_HOUR_SECOND_NAME)) {
            return INTERVAL_HOUR_SECOND_ID;
        }
        if (SQLTypeName.equals(INTERVAL_MINUTE_SECOND_NAME)) {
            return INTERVAL_MINUTE_SECOND_ID;
        }
        if (SQLTypeName.equals(TINYINT_UNSIGNED_NAME)) {
            return TINYINT_UNSIGNED_ID;
        }
        if (SQLTypeName.equals(SMALLINT_UNSIGNED_NAME)) {
            return SMALLINT_UNSIGNED_ID;
        }
        if (SQLTypeName.equals(INTEGER_UNSIGNED_NAME) ||
            SQLTypeName.equals(INT_UNSIGNED_NAME)) {
            return INTEGER_UNSIGNED_ID;
        }
        if (SQLTypeName.equals(LONGINT_UNSIGNED_NAME)) {
            return BIGINT_UNSIGNED_ID;
        }
        if (SQLTypeName.equals(FLOAT_UNSIGNED_NAME)) {
            return REAL_UNSIGNED_ID;
        }
        if (SQLTypeName.equals(REAL_UNSIGNED_NAME)) {
            return REAL_UNSIGNED_ID;
        }
        if (SQLTypeName.equals(DOUBLE_UNSIGNED_NAME)) {
            return DOUBLE_UNSIGNED_ID;
        }
        if (SQLTypeName.equals(NUMERIC_UNSIGNED_NAME)) {
            return NUMERIC_UNSIGNED_ID;
        }
        if (SQLTypeName.equals(DECIMAL_UNSIGNED_NAME)) {
            return DECIMAL_UNSIGNED_ID;
        }
        if (SQLTypeName.equals(DATETIME_NAME)) {
            return DATETIME_ID;
        }
        if (SQLTypeName.equals(YEAR_NAME)) {
            return YEAR_ID;
        }

        // Types defined below here are SQL types and non-JDBC types that are
        // supported by Derby
        if (SQLTypeName.equals(REF_NAME)) {
            return REF_ID;
        }
        return null;
    }

    /*
     * * Instance fields and methods
     */

    /* Set in setTypeIdSpecificInstanceVariables() as needed */
    private int formatId;
    private String schemaName;
    private String unqualifiedName;
    private int JDBCTypeId;
    private String javaTypeName;
    private boolean classNameWasDelimitedIdentifier;
    private boolean isBitTypeId;
    private boolean isLOBTypeId;
    private boolean isBooleanTypeId;
    private boolean isConcatableTypeId;
    private boolean isDecimalTypeId;
    private boolean isLongConcatableTypeId;
    private boolean isNumericTypeId;
    private boolean isRefTypeId;
    private boolean isStringTypeId;
    private boolean isFloatingPointTypeId;
    private boolean isRealTypeId;
    private boolean isDateTimeTimeStampTypeId;
    private boolean isIntervalTypeId;
    private boolean isUserDefinedTypeId;
    private boolean isComparable;
    private int maxPrecision;
    private int maxScale;
    private int maxMaxWidth;
    private int typePrecedence;

    /**
     * Constructor for a TypeId
     *
     * @param formatId Internal format id
     */
    private TypeId(int formatId) {
        this.formatId = formatId;
        // most types are comparable to themselves (with a few exceptions)
        isComparable = true;
        
        switch (formatId) {
        case FormatIds.BIT_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.BIT_NAME;
            JDBCTypeId = Types.BINARY;
            typePrecedence = BIT_PRECEDENCE;
            javaTypeName = "byte[]";
            maxMaxWidth = TypeId.BIT_MAXWIDTH;
            isBitTypeId = true;
            isConcatableTypeId = true;
            break;

        case FormatIds.BOOLEAN_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.BOOLEAN_NAME;
            JDBCTypeId = Types.BOOLEAN;
            maxPrecision = TypeId.BOOLEAN_MAXWIDTH;
            typePrecedence = BOOLEAN_PRECEDENCE;
            javaTypeName = "java.lang.Boolean";
            maxMaxWidth = TypeId.BOOLEAN_MAXWIDTH;
            isBooleanTypeId = true;
            break;

        case FormatIds.CHAR_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.CHAR_NAME;
            JDBCTypeId = Types.CHAR;
            typePrecedence = CHAR_PRECEDENCE;
            javaTypeName = "java.lang.String";
            maxMaxWidth = TypeId.CHAR_MAXWIDTH;
            isStringTypeId = true;
            isConcatableTypeId = true;
            break;

        case FormatIds.DATE_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.DATE_NAME;
            JDBCTypeId = Types.DATE;
            typePrecedence = DATE_PRECEDENCE;
            javaTypeName = "java.sql.Date";
            maxMaxWidth = TypeId.DATE_MAXWIDTH;
            maxPrecision = TypeId.DATE_MAXWIDTH;
            isDateTimeTimeStampTypeId = true;
            break;

        case FormatIds.DECIMAL_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.DECIMAL_NAME;
            JDBCTypeId = Types.DECIMAL;
            maxPrecision = TypeId.DECIMAL_PRECISION;
            maxScale = TypeId.DECIMAL_SCALE;
            typePrecedence = DECIMAL_PRECEDENCE;
            javaTypeName = "java.math.BigDecimal";
            maxMaxWidth = TypeId.DECIMAL_MAXWIDTH;
            isDecimalTypeId = true;
            isNumericTypeId = true;
            break;

        case FormatIds.NUMERIC_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.NUMERIC_NAME;
            JDBCTypeId = Types.NUMERIC;
            maxPrecision = TypeId.DECIMAL_PRECISION;
            maxScale = TypeId.DECIMAL_SCALE;
            typePrecedence = DECIMAL_PRECEDENCE;
            javaTypeName = "java.math.BigDecimal";
            maxMaxWidth = TypeId.DECIMAL_MAXWIDTH;
            isDecimalTypeId = true;
            isNumericTypeId = true;
            break;

        case FormatIds.DOUBLE_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.DOUBLE_NAME;
            JDBCTypeId = Types.DOUBLE;
            maxPrecision = TypeId.DOUBLE_PRECISION;
            maxScale = TypeId.DOUBLE_SCALE;
            typePrecedence = DOUBLE_PRECEDENCE;
            javaTypeName = "java.lang.Double";
            maxMaxWidth = TypeId.DOUBLE_MAXWIDTH;
            isNumericTypeId = true;
            isFloatingPointTypeId = true;
            break;

        case FormatIds.MEDIUMINT_ID:
            schemaName = null;
            unqualifiedName = TypeId.MEDIUMINT_NAME;
            JDBCTypeId = Types.OTHER;
            maxPrecision = TypeId.INT_PRECISION;
            maxScale = TypeId.INT_SCALE;
            typePrecedence = INT_PRECEDENCE;
            javaTypeName = "java.lang.Integer";
            maxMaxWidth = TypeId.INT_MAXWIDTH;
            isNumericTypeId = true;
            break;
            
        case FormatIds.INT_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.INTEGER_NAME;
            JDBCTypeId = Types.INTEGER;
            maxPrecision = TypeId.INT_PRECISION;
            maxScale = TypeId.INT_SCALE;
            typePrecedence = INT_PRECEDENCE;
            javaTypeName = "java.lang.Integer";
            maxMaxWidth = TypeId.INT_MAXWIDTH;
            isNumericTypeId = true;
            break;

        case FormatIds.LONGINT_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.LONGINT_NAME;
            JDBCTypeId = Types.BIGINT;
            maxPrecision = TypeId.LONGINT_PRECISION;
            maxScale = TypeId.LONGINT_SCALE;
            typePrecedence = LONGINT_PRECEDENCE;
            javaTypeName = "java.lang.Long";
            maxMaxWidth = TypeId.LONGINT_MAXWIDTH;
            isNumericTypeId = true;
            break;

        case FormatIds.LONGVARBIT_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.LONGVARBIT_NAME;
            JDBCTypeId = Types.LONGVARBINARY;
            typePrecedence = LONGVARBIT_PRECEDENCE;
            javaTypeName = "byte[]";
            maxMaxWidth = TypeId.LONGVARBIT_MAXWIDTH;
            isBitTypeId = true;
            isConcatableTypeId = true;
            isLongConcatableTypeId = true;
            break;

        case FormatIds.LONGVARCHAR_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.LONGVARCHAR_NAME;
            JDBCTypeId = Types.LONGVARCHAR;
            typePrecedence = LONGVARCHAR_PRECEDENCE;
            javaTypeName = "java.lang.String";
            maxMaxWidth = TypeId.LONGVARCHAR_MAXWIDTH;
            isStringTypeId = true;
            isConcatableTypeId = true;
            isLongConcatableTypeId = true;
            isComparable = false;
            break;

        case FormatIds.REAL_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.REAL_NAME;
            JDBCTypeId = Types.REAL;
            maxPrecision = TypeId.REAL_PRECISION;
            maxScale = TypeId.REAL_SCALE;
            typePrecedence = REAL_PRECEDENCE;
            javaTypeName = "java.lang.Float";
            maxMaxWidth = TypeId.REAL_MAXWIDTH;
            isNumericTypeId = true;
            isRealTypeId = true;
            isFloatingPointTypeId = true;
            break;

        case FormatIds.REF_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.REF_NAME;
            JDBCTypeId = Types.OTHER;
            typePrecedence = REF_PRECEDENCE;
            javaTypeName = "java.sql.Ref";
            isRefTypeId = true;
            isComparable = false;
            break;

        case FormatIds.SMALLINT_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.SMALLINT_NAME;
            JDBCTypeId = Types.SMALLINT;
            maxPrecision = TypeId.SMALLINT_PRECISION;
            maxScale = TypeId.SMALLINT_SCALE;
            typePrecedence = SMALLINT_PRECEDENCE;
            javaTypeName = "java.lang.Integer";
            maxMaxWidth = TypeId.SMALLINT_MAXWIDTH;
            isNumericTypeId = true;
            break;

        case FormatIds.TIME_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.TIME_NAME;
            JDBCTypeId = Types.TIME;
            typePrecedence = TIME_PRECEDENCE;
            javaTypeName = "java.sql.Time";
            maxScale = TypeId.TIME_SCALE;
            maxMaxWidth = TypeId.TIME_MAXWIDTH;
            maxPrecision = TypeId.TIME_MAXWIDTH;
            isDateTimeTimeStampTypeId = true;
            break;

        case FormatIds.TIMESTAMP_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.TIMESTAMP_NAME;
            JDBCTypeId = Types.TIMESTAMP;
            typePrecedence = TIMESTAMP_PRECEDENCE;
            javaTypeName = "java.sql.Timestamp";
            maxScale = TypeId.TIMESTAMP_SCALE;
            maxMaxWidth = TypeId.TIMESTAMP_MAXWIDTH;
            maxPrecision = TypeId.TIMESTAMP_MAXWIDTH;
            isDateTimeTimeStampTypeId = true;
            break;

        case FormatIds.TINYINT_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.TINYINT_NAME;
            JDBCTypeId = Types.TINYINT;
            maxPrecision = TypeId.TINYINT_PRECISION;
            maxScale = TypeId.TINYINT_SCALE;
            typePrecedence = TINYINT_PRECEDENCE;
            javaTypeName = "java.lang.Integer";
            maxMaxWidth = TypeId.TINYINT_MAXWIDTH;
            isNumericTypeId = true;
            break;

        case FormatIds.VARBIT_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.VARBIT_NAME;
            JDBCTypeId = Types.VARBINARY;
            typePrecedence = VARBIT_PRECEDENCE;
            javaTypeName = "byte[]";
            maxMaxWidth = TypeId.VARBIT_MAXWIDTH;
            isBitTypeId = true;
            isConcatableTypeId = true;
            break;

        case FormatIds.BLOB_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.BLOB_NAME;
            JDBCTypeId = Types.BLOB;
            typePrecedence = BLOB_PRECEDENCE;
            javaTypeName = "java.sql.Blob";
            maxMaxWidth = TypeId.BLOB_MAXWIDTH;
            isBitTypeId = true;
            isConcatableTypeId = true;
            isComparable = false;
            isLOBTypeId = true;
            break;

        case FormatIds.VARCHAR_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.VARCHAR_NAME;
            JDBCTypeId = Types.VARCHAR;
            typePrecedence = VARCHAR_PRECEDENCE;
            javaTypeName = "java.lang.String";
            maxMaxWidth = TypeId.VARCHAR_MAXWIDTH;
            isStringTypeId = true;
            isConcatableTypeId = true;
            break;

        case FormatIds.CLOB_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.CLOB_NAME;
            JDBCTypeId = Types.CLOB;
            typePrecedence = CLOB_PRECEDENCE;
            javaTypeName = "java.sql.Clob";
            maxMaxWidth = TypeId.CLOB_MAXWIDTH;
            isStringTypeId = true;
            isConcatableTypeId = true;
            isComparable = false;
            isLOBTypeId = true;
            break;

        case FormatIds.XML_TYPE_ID:
            schemaName = null;
            unqualifiedName = TypeId.XML_NAME;
            JDBCTypeId = Types.SQLXML;
            typePrecedence = XML_PRECEDENCE;
            javaTypeName = "org.cratedb.sql.parser.types.XML";
            maxMaxWidth = TypeId.XML_MAXWIDTH;
            isComparable = false;
            break;
            
        case FormatIds.INTERVAL_YEAR_MONTH_ID:
            schemaName = null;
            typePrecedence = INTERVAL_PRECEDENCE;
            JDBCTypeId = Types.OTHER;
            maxPrecision = TypeId.INTERVAL_YEAR_MONTH_PRECISION;
            maxScale = TypeId.INTERVAL_YEAR_MONTH_SCALE;
            maxMaxWidth = TypeId.INTERVAL_YEAR_MONTH_MAXWIDTH;
            isIntervalTypeId = true;
            break;
            
        case FormatIds.INTERVAL_DAY_SECOND_ID:
            schemaName = null;
            typePrecedence = INTERVAL_PRECEDENCE;
            JDBCTypeId = Types.OTHER;
            maxPrecision = TypeId.INTERVAL_DAY_SECOND_PRECISION;
            maxScale = TypeId.INTERVAL_DAY_SECOND_SCALE;
            maxMaxWidth = TypeId.INTERVAL_DAY_SECOND_MAXWIDTH;
            isIntervalTypeId = true;
            break;
            
        case FormatIds.USERDEFINED_TYPE_ID:
            JDBCTypeId = java.sql.Types.JAVA_OBJECT;
            maxMaxWidth = -1;
            isUserDefinedTypeId = true;
            typePrecedence = USER_PRECEDENCE;
            break;

        case FormatIds.ROW_MULTISET_TYPE_ID_IMPL:
            schemaName = null;
            JDBCTypeId = Types.OTHER;
            javaTypeName = "java.sql.ResultSet";
            maxMaxWidth = -1;
            break;

        default:
            assert false;
            break;
        }
    }

    public int getTypeFormatId() {
        return formatId;
    }

    private boolean unsigned;
    private TypeId(int formatId, boolean unsigned) {
        this(formatId);
        if (unsigned) {
            this.unsigned = true;
            switch (formatId) {
            case FormatIds.DECIMAL_TYPE_ID:
                unqualifiedName = TypeId.DECIMAL_UNSIGNED_NAME;
                break;
            case FormatIds.NUMERIC_TYPE_ID:
                unqualifiedName = TypeId.NUMERIC_UNSIGNED_NAME;
                break;
            case FormatIds.DOUBLE_TYPE_ID:
                unqualifiedName = TypeId.DOUBLE_UNSIGNED_NAME;
                break;
            case FormatIds.INT_TYPE_ID:
                unqualifiedName = TypeId.INTEGER_UNSIGNED_NAME;
                break;
            case FormatIds.LONGINT_TYPE_ID:
                unqualifiedName = TypeId.LONGINT_UNSIGNED_NAME;
                break;
            case FormatIds.REAL_TYPE_ID:
                unqualifiedName = TypeId.REAL_UNSIGNED_NAME;
                break;
            case FormatIds.SMALLINT_TYPE_ID:
                unqualifiedName = TypeId.SMALLINT_UNSIGNED_NAME;
                break;
            case FormatIds.TINYINT_TYPE_ID:
                unqualifiedName = TypeId.TINYINT_UNSIGNED_NAME;
                break;
            case FormatIds.MEDIUMINT_ID:
                unqualifiedName = TypeId.MEDIUMINT_UNSIGNED_NAME;
                break;
            default:
                assert false : "unknown formatId: " + formatId;
            }
        }
    }

    private TypeId(int formatId, String name) {
        this(formatId);
        unqualifiedName = name;
    }

    /**
     * Constructor for a TypeId for user defined types
     *
     * @param className The class name / delimited identifier.
     * @param classNameWasDelimitedIdentifier Whether or not the class name
     *                      was a delimited identifier.
     */
    private TypeId(String className, boolean classNameWasDelimitedIdentifier) {
        this(FormatIds.USERDEFINED_TYPE_ID);
        if (classNameWasDelimitedIdentifier) {
            // TODO: Need to split?
        }
        else {
            schemaName = null;
            unqualifiedName = className;
        }
        javaTypeName = className;
        this.classNameWasDelimitedIdentifier = classNameWasDelimitedIdentifier;
    }

    private TypeId(String schemaName, String unqualifiedName, String className) {
        this(FormatIds.USERDEFINED_TYPE_ID);
        this.schemaName = schemaName;
        this.unqualifiedName = unqualifiedName;
        this.javaTypeName = className;
    }

    /**
     * we want equals to say if these are the same type id or not.
     */
    public boolean equals(Object that) {
        if (that instanceof TypeId)
            return this.getSQLTypeName().equals(((TypeId)that).getSQLTypeName());
        else
            return false;
    }

    /*
      Hashcode which works with equals.
    */
    public int hashCode() {
        return this.getSQLTypeName().hashCode();
    }

    /**
     * JDBC has its own idea of type identifiers which is different from
     * the Derby internal type ids.  The JDBC type ids are defined
     * as public final static ints in java.sql.Types.    This method translates
     * a Derby internal TypeId to a JDBC type id. For java objects this
     * returns JAVA_OBJECT in Java2 and OTHER in JDK 1.1. For Boolean datatypes,
     * this returns Type.BOOLEAN in JDK1.4 and Type.BIT for jdks prior to 1.4
     *
     * @return The JDBC type Id for this type
     */
    public final int getJDBCTypeId() {
        return JDBCTypeId;
    }

    /**
     * Returns the SQL name of the datatype. If it is a user-defined type,
     * it returns the full Java path name for the datatype, meaning the
     * dot-separated path including the package names.
     *
     * @return A String containing the SQL name of this type.
     */
    public String getSQLTypeName() {
        if (schemaName == null) { 
            return unqualifiedName; 
        }
        else {
            // TODO: Need some quotes?
            return schemaName + "." + unqualifiedName; 
        }
    }

    /**
     * Tell whether this is a built-in type.
     * NOTE: There are 3 "classes" of types:
     *      built-in - system provided types which are implemented internally
     *                 (int, smallint, etc.)
     *      system built-in - system provided types, independent of implementation
     *                        (date, time, etc.)
     *      user types - types implemented outside of the system
     *                   (java.lang.Integer, asdf.asdf.asdf, etc.)
     *
     * @return false for built-in types, true for user-defined types.
     */
    public final boolean userType() {
        return isUserDefinedTypeId;
    }

    /**
     * Get the maximum precision of the type.    For types with variable
     * precision, this is an arbitrary high precision.
     *
     * @return The maximum precision of the type
     */
    public int getMaximumPrecision() {
        return maxPrecision;
    }

    /**
     * Get the maximum scale of the type.    For types with variable scale,
     * this is an arbitrary high scale.
     *
     * @return The maximum scale of the type
     */
    public int getMaximumScale() {
        return maxScale;
    }

    /**
     * For user types, tell whether or not the class name was a
     * delimited identifier. For all other types, return false.
     *
     * @return Whether or not the class name was a delimited identifier.
     */
    public boolean getClassNameWasDelimitedIdentifier() {
        return classNameWasDelimitedIdentifier;
    }

    /**
     * Does this TypeId represent a TypeId for a StringDataType.
     *
     * @return Whether or not this TypeId represents a TypeId for a StringDataType.
     */
    public boolean isStringTypeId() {
        return isStringTypeId;
    }

    /**
     * Is this a TypeId for DATE/TIME/TIMESTAMP
     *
     * @return true if this is a DATE/TIME/TIMESTAMP
     */
    public boolean isDateTimeTimeStampTypeId() {
        return isDateTimeTimeStampTypeId;
    }

    /**
     * Is this a TypeId for REAL
     *
     * @return true if this is a REAL
     */
    public boolean isRealTypeId() {
        return isRealTypeId;
    }

    /**
     * Is this a TypeId for floating point (REAL/DOUBLE)
     *
     * @return true if this is a REAL or DOUBLE
     */
    public boolean isFloatingPointTypeId() {
        return isFloatingPointTypeId;
    }

    /**
     * Is this a TypeId for DOUBLE
     *
     * @return true if this is a DOUBLE
     */
    public boolean isDoubleTypeId() {
        return isFloatingPointTypeId && (!isRealTypeId);
    }

    /**
     * Is this a fixed string type?
     * @return true if this is CHAR
     */
    public boolean isFixedStringTypeId() {
        return (formatId == FormatIds.CHAR_TYPE_ID);
    }

    /** 
     *Is this a Clob?
     * @return true if this is CLOB
     */
    public boolean isClobTypeId()
    {
        return (formatId == FormatIds.CLOB_TYPE_ID);
    }

    /** 
     *Is this a Blob?
     * @return true if this is BLOB
     */
    public boolean isBlobTypeId()
    {
        return (formatId == FormatIds.BLOB_TYPE_ID);
    }

    /** 
     *Is this a LongVarchar?
     * @return true if this is LongVarchar
     */
    public boolean isLongVarcharTypeId()
    {
        return (formatId == FormatIds.LONGVARCHAR_TYPE_ID);
    }

    /** 
     *Is this a LongVarbinary?
     * @return true if this is LongVarbinary
     */
    public boolean isLongVarbinaryTypeId()
    {
        return (formatId == FormatIds.LONGVARBIT_TYPE_ID);
    }


    /** 
     * Is this DATE/TIME or TIMESTAMP?
     *
     * @return true if this DATE/TIME or TIMESTAMP
     */
    public boolean isDateTimeTimeStampTypeID()
    {
        return ((formatId == FormatIds.DATE_TYPE_ID) ||
                (formatId == FormatIds.TIME_TYPE_ID) ||
                (formatId == FormatIds.TIMESTAMP_TYPE_ID));
    }

    /** 
     *Is this an XML doc?
     * @return true if this is XML
     */
    public boolean isXMLTypeId()
    {
        return (formatId == FormatIds.XML_TYPE_ID);
    }

    /**
     * @return <code>false</code> if this type is not comparable to any other types or even to itself
     *         <code>true</code> otherwise.
     */
    public boolean isComparable()
    {
        return isComparable;
    }
    
    /**
     * Each built-in type in JSQL has a precedence.  This precedence determines
     * how to do type promotion when using binary operators.    For example, float
     * has a higher precedence than int, so when adding an int to a float, the
     * result type is float.
     *
     * The precedence for some types is arbitrary.  For example, it doesn't
     * matter what the precedence of the boolean type is, since it can't be
     * mixed with other types.  But the precedence for the number types is
     * critical.    The SQL standard requires that exact numeric types be
     * promoted to approximate numeric when one operator uses both.  Also,
     * the precedence is arranged so that one will not lose precision when
     * promoting a type.
     * NOTE: char, varchar, and longvarchar must appear at the bottom of
     * the hierarchy, but above USER_PRECEDENCE, since we allow the implicit
     * conversion of those types to any other built-in system type.
     *
     * @return The precedence of this type.
     */
    public int typePrecedence()
    {
        return typePrecedence;
    }

    /**
     * Get the name of the corresponding Java type.
     *
     * Each SQL type has a corresponding Java type.  When a SQL value is
     * passed to a Java method, it is translated to its corresponding Java
     * type.    For example, when a SQL date column is passed to a method,
     * it is translated to a java.sql.Date.
     *
     * @return          The name of the corresponding Java type.
     */
    public String getCorrespondingJavaTypeName() {
        return javaTypeName;
    }

    /**
     * Get the name of the corresponding Java type.
     *
     * This method is used directly from EmbedResultSetMetaData (jdbc)
     * to return the corresponding type (as choosen by getObject).
     * It solves a specific problem for BLOB types where the 
     * getCorrespondingJavaTypeName() is used internally for casting
     * which doesn't work if changed from byte[] to java.sql.Blob.
     * So we do it here instead, to avoid unexpected side effects.
     *
     * @return          The name of the corresponding Java type.
     */
    public String getResultSetMetaDataTypeName() {
        if (BLOB_ID.equals(this))
            return "java.sql.Blob";
        if (CLOB_ID.equals(this))
            return "java.sql.Clob";
        return getCorrespondingJavaTypeName();
    }

    /**
     * Get the maximum maximum width of the type (that's not a typo).    For
     * types with variable length, this is the absolute maximum for the type.
     *
     * @return          The maximum maximum width of the type
     */
    public int getMaximumMaximumWidth() {
        return maxMaxWidth;
    }

    /**
     * Converts this TypeId, given a data type descriptor (including length/precision),
     * to a string. E.g.
     *
     *                                          VARCHAR(30)
     *
     *
     *          For most data types, we just return the SQL type name.
     *
     *          @param  dts Data type descriptor that holds the length/precision etc. as necessary
     *
     *           @return String version of datatype, suitable for running through
     *                          the Parser.
     */
    // TODO: Consider consolitation with DataTypeDescriptor.getFullSQLTypeName().
    public String toParsableString(DataTypeDescriptor dts) {
        String retval = getSQLTypeName();

        switch (formatId) {
        case FormatIds.BIT_TYPE_ID:
        case FormatIds.VARBIT_TYPE_ID:
            int rparen = retval.indexOf(')');
            String lead = retval.substring(0, rparen);
            retval = lead + dts.getMaximumWidth() + retval.substring(rparen);
            break;

        case FormatIds.CHAR_TYPE_ID:
        case FormatIds.VARCHAR_TYPE_ID:
        case FormatIds.BLOB_TYPE_ID:
        case FormatIds.CLOB_TYPE_ID:
            retval += "(" + dts.getMaximumWidth() + ")";
            break;

        case FormatIds.DECIMAL_TYPE_ID:
            if (unsigned) {
                retval = retval.substring(0, retval.length() - 9) +
                    "(" + dts.getPrecision() + "," + dts.getScale() + ")" +
                    retval.substring(retval.length() - 9);
            }
            else
                retval += "(" + dts.getPrecision() + "," + dts.getScale() + ")";
            break;

        case FormatIds.INTERVAL_YEAR_MONTH_ID:
        case FormatIds.INTERVAL_DAY_SECOND_ID:
            if (this == INTERVAL_SECOND_ID) {
                if (dts.getPrecision() > 0) {
                    retval += "(" + dts.getPrecision();
                    if (dts.getScale() > 0)
                        retval += ", " + dts.getScale();
                    retval += ")";
                }
            }
            else {
                if (dts.getPrecision() > 0) {
                    int idx = retval.indexOf(" ", 9);
                    if (idx < 0) idx = retval.length();
                    retval = retval.substring(0, idx) +
                        "(" + dts.getPrecision() + ")" +
                        retval.substring(idx);
                }
                if (dts.getScale() > 0)
                    retval += "(" + dts.getScale() +")";
            }
            break;
        }

        return retval;
    }

    /**
     * Is this a type id for a numeric type?
     *
     * @return Whether or not this a type id for a numeric type.
     */
    public boolean isNumericTypeId() {
        return isNumericTypeId;
    }

    /**
     * Is this a type id for a decimal type?
     *
     * @return Whether or not this a type id for a decimal type.
     */
    public boolean isDecimalTypeId() {
        return isDecimalTypeId;
    }

    /**
     * Is this a type id for an integer type?
     *
     * @return Whether or not this a type id for a integer type.
     */
    public boolean isIntegerTypeId() {
        return isNumericTypeId && !isDecimalTypeId && !isFloatingPointTypeId;
    }
    
    /**
     * Is this a type id for a boolean type?
     *
     * @return Whether or not this a type id for a boolean type.
     */
    public boolean isBooleanTypeId() {
        return isBooleanTypeId;
    }

    /**
     * Is this a type id for a ref type?
     *
     * @return Whether or not this a type id for a ref type.
     */
    public boolean isRefTypeId() {
        return isRefTypeId;
    }

    /**
     * Is this a type id for a concatable type?
     *
     * @return Whether or not this a type id for a concatable type.
     */
    public boolean isConcatableTypeId() {
        return isConcatableTypeId;
    }

    /**
     * Is this a type id for a bit type?
     *
     * @return Whether or not this a type id for a bit type.
     */
    public boolean isBitTypeId() {
        return isBitTypeId;
    }

    /**
     * Is this a type id for a LOB type?
     *
     * @return Whether or not this a type id for a LOB type.
     */
    public boolean isLOBTypeId() {
        return isLOBTypeId;
    }

    /**
     * Is this a type id for a long concatable type?
     *
     * @return Whether or not this a type id for a long concatable type.
     */
    public boolean isLongConcatableTypeId() {
        return isLongConcatableTypeId;
    }

    /**
     * Is this a type id for a user defined type?
     *
     * @return Whether or not this a type id for a user defined type.
     */
    public boolean isUserDefinedTypeId() {
        return isUserDefinedTypeId;
    }

    /**
     * Get the precision of the merge of two Decimals
     *
     * @param leftType the left type
     * @param rightType the left type
     *
     * @return the resultant precision
     */
    public int getPrecision(DataTypeDescriptor leftType,
                            DataTypeDescriptor rightType) {
        long lscale = (long)leftType.getScale();
        long rscale = (long)rightType.getScale();
        long lprec = (long)leftType.getPrecision();
        long rprec = (long)rightType.getPrecision();
        long val;

        assert (formatId == FormatIds.DECIMAL_TYPE_ID) : formatId;

        /*
        ** Take the maximum left of decimal digits plus the scale.
        */
        val = this.getScale(leftType, rightType) + Math.max(lprec - lscale, rprec - rscale);

        if (val > Integer.MAX_VALUE) {
            val = Integer.MAX_VALUE;
        }
        return (int)val;
    }

    /**
     * Get the scale of the merge of two decimals
     *
     * @param leftType the left type
     * @param rightType the left type
     *
     * @return the resultant precision
     */
    public int getScale(DataTypeDescriptor leftType,
                        DataTypeDescriptor rightType) {
        assert (formatId == FormatIds.DECIMAL_TYPE_ID) : formatId;

        /*
        ** Retain greatest scale
        */
        return Math.max(leftType.getScale(), rightType.getScale());
    }

    /**
     * Does type hava a declared variable length (defined by the application).
     * Examples are CHAR(10), CLOB(1M).
     * Unbounded long types, like LONG VARCHAR return false here.
     * @return boolean true if type is variable length false if not.    
     */
    public boolean variableLength() {
        switch (formatId) {
        case FormatIds.BIT_TYPE_ID:
        case FormatIds.VARBIT_TYPE_ID:
        case FormatIds.DECIMAL_TYPE_ID:
        case FormatIds.CHAR_TYPE_ID:
        case FormatIds.VARCHAR_TYPE_ID:
        case FormatIds.BLOB_TYPE_ID:
        case FormatIds.CLOB_TYPE_ID:
            return true;

        default:
            return false;
        }
    }

    public static class RowMultiSetTypeId extends TypeId {
        String[] columnNames;
        DataTypeDescriptor[] columnTypes;

        public RowMultiSetTypeId(String[] columnNames, DataTypeDescriptor[] columnTypes) {
            super(FormatIds.ROW_MULTISET_TYPE_ID_IMPL);
            this.columnNames = columnNames;
            this.columnTypes = columnTypes;
        }

        /**
         * <p>
         * Get the SQL name of this multi set. This is the name suitable for
         * replaying the DDL to create a Table Function.
         * </p>
         */
        public String getSQLTypeName() {
            StringBuffer buffer = new StringBuffer();
            int count = columnNames.length;

            buffer.append("TABLE ( ");

            for (int i = 0; i < count; i++) {
                if (i > 0) { 
                    buffer.append( ", " ); 
                }
                buffer.append('\"');
                buffer.append(columnNames[i]);
                buffer.append('\"');
                buffer.append(' ');
                buffer.append(columnTypes[i].getSQLstring());
            }

            buffer.append( " )" );

            return buffer.toString();
        }

        public boolean isRowMultiSet() {
            return true;
        }

        public String[] getColumnNames() {
            return columnNames;
        }

        public DataTypeDescriptor[] getColumnTypes() {
            return columnTypes;
        }
    }

    public static TypeId getRowMultiSet(String[] columnNames,
                                        DataTypeDescriptor[] columnTypes) {
        return new RowMultiSetTypeId(columnNames, columnTypes);
    }

    public boolean isRowMultiSet() {
        return false;
    }

    /** Is this one of the unsigned numeric types? */
    public boolean isUnsigned() {
        return unsigned;
    }

    public static TypeId intervalTypeId(TypeId startField, TypeId endField) 
            throws StandardException {
        if (startField == INTERVAL_YEAR_ID) {
            if (endField == INTERVAL_MONTH_ID)
                return INTERVAL_YEAR_MONTH_ID;
        }
        if (startField == INTERVAL_DAY_ID) {
            if (endField == INTERVAL_HOUR_ID)
                return INTERVAL_DAY_HOUR_ID;
            if (endField == INTERVAL_MINUTE_ID)
                return INTERVAL_DAY_MINUTE_ID;
            if (endField == INTERVAL_SECOND_ID)
                return INTERVAL_DAY_SECOND_ID;
        }
        if (startField == INTERVAL_HOUR_ID) {
            if (endField == INTERVAL_MINUTE_ID)
                return INTERVAL_HOUR_MINUTE_ID;
            if (endField == INTERVAL_SECOND_ID)
                return INTERVAL_HOUR_SECOND_ID;
        }
        if (startField == INTERVAL_MINUTE_ID) {
            if (endField == INTERVAL_SECOND_ID)
                return INTERVAL_MINUTE_SECOND_ID;
        }
        throw new StandardException("Illegal " + startField.unqualifiedName +
                                    " TO " + endField.unqualifiedName.substring("INTERVAL ".length()));
    }

    public boolean isIntervalTypeId() {
        return isIntervalTypeId;
    }

}
