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

package org.cratedb.sql.types;

public class BooleanSQLType extends SQLType {

    public static final String NAME = "boolean";

    @Override
    public String typeName() {
        return NAME;
    }

    @Override
    protected Object doMapValue(Object value) throws ConvertException {
        try {
            return (Boolean)value;
        } catch (ClassCastException e) {
            if (value instanceof String) {
                // boolean gets returned as "T" or "F" from lucene DocLookUp
                if ("T".equalsIgnoreCase((String)value)) { return true; }
                else if ("F".equalsIgnoreCase((String)value)) { return false; }
            }
            throw new ConvertException(typeName());
        }
    }
}
