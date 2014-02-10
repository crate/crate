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

import java.sql.Types;

/**
 * This class implements the timestamp(x) and date(x) functions.
 *
 * These two functions implement a few special cases of string conversions beyond the normal string to
 * date/timestamp casts.
 */
public class UnaryDateTimestampOperatorNode extends UnaryOperatorNode
{
    private static final String DATE_METHOD_NAME = "date";
    private static final String TIME_METHOD_NAME = "time";
    private static final String TIMESTAMP_METHOD_NAME = "timestamp";
        
    /**
     * @param operand The operand of the function
     * @param targetType The type of the result. Timestamp or Date.
     *
     * @exception StandardException Thrown on error
     */

    public void init(Object operand, Object targetType) throws StandardException {
        setType((DataTypeDescriptor)targetType);
        switch(getType().getJDBCTypeId()) {
        case Types.DATE:
            super.init(operand, "date", DATE_METHOD_NAME);
            break;

        case Types.TIME:
            super.init(operand, "time", TIME_METHOD_NAME);
            break;

        case Types.TIMESTAMP:
            super.init(operand, "timestamp", TIMESTAMP_METHOD_NAME);
            break;

        default:
            assert false;
            super.init(operand);
        }
    }
        
}
