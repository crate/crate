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

public class RegprocType extends DataType<Regproc> implements Streamer<Regproc> {

    public static final RegprocType INSTANCE = new RegprocType();
    public static final int ID = 19;

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.REGPROC;
    }

    @Override
    public String getName() {
        return "regproc";
    }

    @Override
    public Streamer<Regproc> streamer() {
        return this;
    }

    @Override
    public Regproc implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof Integer num) {
            return Regproc.of(num, value.toString());
        } else if (value instanceof Long num) {
            if (num > Integer.MAX_VALUE || num < Integer.MIN_VALUE) {
                throw new IllegalArgumentException(
                    value + " is outside of `int` range and cannot be cast to the regproc type");
            }
            return Regproc.of(num.intValue(), value.toString());
        } else if (value instanceof String str) {
            return Regproc.of(str);
        } else if (value instanceof Regproc regproc) {
            return regproc;
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
    }

    @Override
    public Regproc valueForInsert(Regproc value) {
        throw new UnsupportedOperationException(
            getName() + " cannot be used in insert statements.");
    }

    @Override
    public Regproc sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else {
            return (Regproc) value;
        }
    }

    @Override
    public int compare(Regproc val1, Regproc val2) {
        return Integer.compare(val1.oid(), val2.oid());
    }

    @Override
    public Regproc readValueFrom(StreamInput in) throws IOException {
        return Regproc.of(in.readInt(), in.readString());
    }

    @Override
    public void writeValueTo(StreamOutput out, Regproc v) throws IOException {
        out.writeInt(v.oid());
        out.writeString(v.name());
    }

    @Override
    public long valueBytes(Regproc value) {
        if (value == null) {
            return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
        }
        return RamUsageEstimator.sizeOf(value.name()) + RamUsageEstimator.sizeOf(value.oid());
    }
}
