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

import java.sql.ParameterMetaData;
import org.cratedb.sql.parser.types.DataTypeDescriptor;

/**
 * Describe a routine (procedure or function) alias.
 *
 * @see AliasInfo
 */
public class RoutineAliasInfo extends MethodAliasInfo
{
    public static enum SQLAllowed {
        MODIFIES_SQL_DATA("MODIFIES SQL DATA"),
        READS_SQL_DATA("READS SQL DATA"), 
        CONTAINS_SQL("CONTAINS SQL"), 
        NO_SQL("NO SQL");

        private String sql;

        private SQLAllowed(String sql) {
            this.sql = sql;
        }

        public String getSQL() {
            return sql;
        }
    }

    private int parameterCount;

    /**
     * Types of the parameters. If there are no parameters
     * then this may be null (or a zero length array).
     */
    private DataTypeDescriptor[] parameterTypes;

    /**
     * Name of each parameter. As of DERBY 10.3, parameter names
     * are optional. If the parameter is unnamed, parameterNames[i]
     * is a string of length 0
     */
    private String[] parameterNames;

    /**
     * ParameterMetaData.parameterModeXxx: IN, OUT, INOUT
     */
    private int[] parameterModes;

    private int dynamicResultSets;

    /**
     * Return type for functions. Null for procedures.
     */
    private DataTypeDescriptor returnType;

    private String language;

    private String parameterStyle;

    private SQLAllowed sqlAllowed;

    private boolean deterministic;

    private boolean definersRights;

    /**
     * SQL Specific name (future)
     */
    private String specificName;

    /**
     * True if the routine is called on null input.
     * (always true for procedures).
     */
    private boolean calledOnNullInput;

    /**
     * Create a RoutineAliasInfo for a PROCEDURE or FUNCTION
     */
    public RoutineAliasInfo(String methodName,
                            int parameterCount,
                            String[] parameterNames,
                            DataTypeDescriptor[] parameterTypes,
                            int[] parameterModes,
                            int dynamicResultSets,
                            String language,
                            String parameterStyle,
                            SQLAllowed sqlAllowed,
                            boolean deterministic,
                            boolean definersRights,
                            boolean calledOnNullInput,
                            DataTypeDescriptor returnType) {

        super(methodName);
        this.parameterCount = parameterCount;
        this.parameterNames = parameterNames;
        this.parameterTypes = parameterTypes;
        this.parameterModes = parameterModes;
        this.dynamicResultSets = dynamicResultSets;
        this.language = language;
        this.parameterStyle = parameterStyle;
        this.sqlAllowed = sqlAllowed;
        this.deterministic = deterministic;
        this.definersRights = definersRights;
        this.calledOnNullInput = calledOnNullInput;
        this.returnType = returnType;
    }

    public int getParameterCount() {
        return parameterCount;
    }

    /**
     * Types of the parameters. If there are no parameters
     * then this may return null (or a zero length array).
     */
    public DataTypeDescriptor[] getParameterTypes() {
        return parameterTypes;
    }

    public int[] getParameterModes() {
        return parameterModes;
    }

    /**
     * Returns an array containing the names of the parameters.
     * As of DERBY 10.3, parameter names are optional (see DERBY-183
     * for more information). If the i-th parameter was unnamed,
     * parameterNames[i] will contain a string of length 0.
     */
    public String[] getParameterNames() {
        return parameterNames;
    }

    public int getMaxDynamicResultSets() {
        return dynamicResultSets;
    }

    public String getLanguage() {
        return language;
    }

    public String getParameterStyle() {
        return parameterStyle;
    }

    public SQLAllowed getSQLAllowed() {
        return sqlAllowed;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }

    public boolean hasDefinersRights()
    {
        return definersRights;
    }

    public boolean calledOnNullInput() {
        return calledOnNullInput;
    }

    public DataTypeDescriptor getReturnType() {
        return returnType;
    }

    public boolean isFunction() {
        return (returnType != null);
    }

    public boolean isTableFunction() {
        if (returnType == null) { 
            return false;
        }
        else { 
            return returnType.isRowMultiSet(); 
        }
    }

    /**
     * Get this alias info as a string.  
     * This method must return a string that is syntactically valid.
     */
    public String toString() {

        StringBuffer sb = new StringBuffer();
        sb.append('(');
        for (int i = 0; i < parameterCount; i++) {
            if (i != 0)
                sb.append(", ");

            if (returnType == null) {
                // This is a PROCEDURE.  We only want to print the
                // parameter mode (ex. "IN", "OUT", "INOUT") for procedures--
                // we don't do it for functions since use of the "IN" keyword
                // is not part of the FUNCTION syntax.
                sb.append(parameterMode(parameterModes[i]));
                sb.append(' ');
            }
            if (parameterNames[i] != null) {
                sb.append(parameterNames[i]);
                sb.append(' ');
            }
            sb.append(parameterTypes[i].getSQLstring());
        }
        sb.append(')');

        if (returnType != null) {
            // this a FUNCTION, so syntax requires us to append the return type.
            sb.append(" RETURNS " + returnType.getSQLstring());
        }

        sb.append(" LANGUAGE ");
        sb.append(language);

        if (parameterStyle != null) {
            sb.append(" PARAMETER STYLE " );
            sb.append(parameterStyle);
        }
                
        if (deterministic) { 
            sb.append(" DETERMINISTIC "); 
        }

        if (definersRights) { 
            sb.append(" EXTERNAL SECURITY DEFINER"); 
        }

        if (sqlAllowed != null) {
            sb.append(" ");
            sb.append(sqlAllowed.getSQL());
        }
        if ((returnType == null) &&
            (dynamicResultSets != 0)) {
            // Only print dynamic result sets if this is a PROCEDURE
            // because it's not valid syntax for FUNCTIONs.
            sb.append(" DYNAMIC RESULT SETS ");
            sb.append(dynamicResultSets);
        }

        if (returnType != null) {
            // this a FUNCTION, so append the syntax telling what to
            // do with a null parameter.
            sb.append(calledOnNullInput ? " CALLED " : " RETURNS NULL ");
            sb.append("ON NULL INPUT");
        }
        
        return sb.toString();
    }

    public static String parameterMode(int parameterMode) {
        switch (parameterMode) {
        case ParameterMetaData.parameterModeIn:
            return "IN";
        case ParameterMetaData.parameterModeOut:
            return "OUT";
        case ParameterMetaData.parameterModeInOut:
            return "INOUT";
        default:
            return "UNKNOWN";
        }
    }
        
}
