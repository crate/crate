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

public class UserDefinedTypeCompiler extends TypeCompiler
{
    protected UserDefinedTypeCompiler(TypeId typeId) {
        super(typeId);
    }

    /**
     * Right now, casting is not allowed from one user defined type
     * to another.
     *
     * @param otherType 
     * @param forDataTypeFunction
     * @return true if otherType is convertible to this type, else false.
     * 
     *@see TypeCompiler#convertible
     */
    public boolean convertible(TypeId otherType, boolean forDataTypeFunction) {
        if (getTypeId().isAnsiUDT()) {
            if (!otherType.isAnsiUDT() ) { 
                return false; 
            }
            return getTypeId().getSQLTypeName().equals(otherType.getSQLTypeName());
        }
                
        /*
        ** We are a non-ANSI user defined type, we are
        ** going to have to let the client find out
        ** the hard way.
        */
        return true;
    }

    /** @see TypeCompiler#compatible */
    public boolean compatible(TypeId otherType) {
        return convertible(otherType, false);
    }
            
    /**
     * @see TypeCompiler#getCorrespondingPrimitiveTypeName
     */

    public String getCorrespondingPrimitiveTypeName() {
        return getTypeId().getCorrespondingJavaTypeName();
    }

    /**
     * Get the method name for getting out the corresponding primitive
     * Java type.
     *
     * @return String The method call name for getting the
     *                              corresponding primitive Java type.
     */
    public String getPrimitiveMethodName() {
        return "getObject";
    }

    /**
     * @see TypeCompiler#getCastToCharWidth
     */
    public int getCastToCharWidth(DataTypeDescriptor dts) {
        // This is the maximum maximum width for user types
        return -1;
    }

}
