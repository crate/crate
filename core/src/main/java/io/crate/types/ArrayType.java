/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.types;

import java.util.Collection;

public class ArrayType extends CollectionType {

    public static final int ID = 100;

    public ArrayType(DataType<?> innerType) {
        super(innerType);
    }

    public ArrayType() {
        super();
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return super.getName() + "_array";
    }

    @Override
    public Object[] value(Object value) {
        // We can pass in Collections and Arrays here but we always
        // have to return arrays since a lot of code makes assumptions
        // about this.
        if (value == null) {
            return null;
        }
        Object[] result;
        if (value instanceof Collection) {
            Collection values = (Collection) value;
            result = new Object[values.size()];
            int idx = 0;
            for (Object o : values) {
                result[idx] = innerType.value(o);
                idx++;
            }
        } else {
            Object[] values = (Object[]) value;
            result = new Object[values.length];
            int idx = 0;
            for (Object o : values) {
                result[idx] = innerType.value(o);
                idx++;
            }
        }
        return result;
    }
}
