/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.types;

import io.crate.Streamer;
import io.crate.common.collections.MapComparator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Object type that makes no assumptions about neither the keys or values, treating them like generic values and lifting
 * the restriction of having the keys be Strings.
 */
public class UncheckedObjectType extends DataType<Map<Object, Object>> implements Streamer<Map<Object, Object>> {

    public static final UncheckedObjectType INSTANCE = new UncheckedObjectType();
    public static final int ID = 16;

    public static final String NAME = "unchecked_object";
    private static final StorageSupport<Map<Object, Object>> STORAGE = new StorageSupport<>(false, false, null);

    public static UncheckedObjectType untyped() {
        return new UncheckedObjectType();
    }

    UncheckedObjectType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.UNCHECKED_OBJECT;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Streamer<Map<Object, Object>> streamer() {
        return this;
    }

    @Override
    public Map<Object, Object> implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        //noinspection unchecked
        return (Map<Object, Object>) value;
    }

    @Override
    public Map<Object, Object> sanitizeValue(Object value) {
        //noinspection unchecked
        return (Map<Object, Object>) value;
    }

    @Override
    public int compare(Map<Object, Object> val1, Map<Object, Object> val2) {
        return MapComparator.compareMaps(val1, val2);
    }

    @Override
    public Map<Object, Object> readValueFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            int size = in.readInt();
            HashMap<Object, Object> m = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                Object key = in.readGenericValue();
                Object val = in.readGenericValue();
                m.put(key, val);
            }
            return m;
        }
        return null;
    }

    @Override
    public void writeValueTo(StreamOutput out, Map<Object, Object> v) throws IOException {
        if (v == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(v.size());
            for (Map.Entry<Object, Object> entry : v.entrySet()) {
                out.writeGenericValue(entry.getKey());
                out.writeGenericValue(entry.getValue());
            }
        }
    }

    @Override
    public StorageSupport<Map<Object, Object>> storageSupport() {
        return STORAGE;
    }
}
