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

import io.crate.Streamer;

/**
 * Type implementation which {@link #equals(Object)} to any implementation of {@link DataType}.
 *
 * <p><strong>Note that this class should only be used for matching types (e.g. signatures),
 * it does NOT support streaming, value evaluation or comparisons.</strong></p>
 */
public class AnyType extends DataType<Object> {

    public static final AnyType INSTANCE = new AnyType();
    public static final int ID = -1;

    private AnyType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return "any";
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof DataType;
    }

    @Override
    public Streamer<?> streamer() {
        throw new UnsupportedOperationException("streaming not supported");
    }

    @Override
    public Object value(Object value) throws IllegalArgumentException, ClassCastException {
        throw new UnsupportedOperationException("value() not supported");
    }

    @Override
    public int compareValueTo(Object val1, Object val2) {
        throw new UnsupportedOperationException("compareValueTo() not supported");
    }
}
