/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.types;

/**
 * A type which represents a single column table. The type's
 * value represents a collection of all values of the first
 * column.
 */
public class SingleColumnTableType extends CollectionType {

    public static final int ID = 102;

    public SingleColumnTableType(DataType<?> innerType) {
        super(innerType);
    }

    public SingleColumnTableType() {
        super();
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.TableType;
    }

    @Override
    public String getName() {
        return super.getName() + "_table";
    }

    /**
     * Expects an array of values of the column.
     */
    @Override
    public Object[] value(Object value) {
        if (value == null) {
            return null;
        }
        Object[] array = (Object[]) value;
        for (int i = 0; i < array.length; i++) {
            array[i] = innerType.value(array[i]);
        }
        return array;
    }

    @Override
    public CollectionType newInstance(DataType innerType) {
        return new SingleColumnTableType(innerType);
    }
}
