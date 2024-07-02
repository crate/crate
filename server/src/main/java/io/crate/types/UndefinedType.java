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

import java.io.IOException;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;

public class UndefinedType extends DataType<Object> implements Streamer<Object> {

    public static final int ID = 0;

    public static final UndefinedType INSTANCE = new UndefinedType();

    private UndefinedType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.UNDEFINED;
    }

    @Override
    public String getName() {
        return "undefined";
    }

    @Override
    public Streamer<Object> streamer() {
        return this;
    }

    @Override
    public Object implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        return value;
    }

    @Override
    public boolean isConvertableTo(DataType<?> other, boolean explicitCast) {
        return true;
    }

    public Object sanitizeValue(Object value) {
        return value;
    }

    @Override
    public int compare(Object val1, Object val2) {
        return 0;
    }

    @Override
    public Object readValueFrom(StreamInput in) throws IOException {
        return in.readGenericValue();
    }

    @Override
    public void writeValueTo(StreamOutput out, Object v) throws IOException {
        out.writeGenericValue(v);
    }

    @Override
    public long valueBytes(Object value) {
        if (value == null) {
            return RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        }
        return RamUsageEstimator.sizeOfObject(value);
    }
}
