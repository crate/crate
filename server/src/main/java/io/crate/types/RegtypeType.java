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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import org.apache.lucene.util.RamUsageEstimator;

import io.crate.Streamer;

public final class RegtypeType extends DataType<Regtype> implements Streamer<Regtype>, FixedWidthType {
    public static final RegtypeType INSTANCE = new RegtypeType();
    public static final int ID = 30;

    private RegtypeType() {
    }

    @Override
    public int fixedSize() {
        return (int) RamUsageEstimator.shallowSizeOfInstance(Integer.class);
    }

    @Override
    public int compare(Regtype o1, Regtype o2) {
        return o1.compareTo(o2);
    }

    @Override
    public Regtype readValueFrom(StreamInput in) throws IOException {
        return in.readOptionalWriteable(Regtype::new);
    }

    @Override
    public void writeValueTo(StreamOutput out, Regtype v) throws IOException {
        out.writeOptionalWriteable(v);
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.REGTYPE;
    }

    @Override
    public String getName() {
        return "regtype";
    }

    @Override
    public Streamer<Regtype> streamer() {
        return this;
    }

    @Override
    public Regtype sanitizeValue(Object value) {
        if (value == null) {
            return null;
        }
        return (Regtype) value;
    }

    @Override
    public Regtype implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        }
        if (value instanceof Integer num) {
            return new Regtype(num.intValue());
        }
        if (value instanceof Long num) {
            if (num > Integer.MAX_VALUE || num < Integer.MIN_VALUE) {
                throw new IllegalArgumentException(
                        value + " is outside of `int` range and cannot be cast to the regtype type");
            }
            return new Regtype(num.intValue());
        }
        if (value instanceof String str) {
            return Regtype.fromName(str);
        }
        if (value instanceof Regtype regtype) {
            return regtype;
        }
        throw new ClassCastException("Can't cast '" + value + "' to " + getName());
    }

    @Override
    public long valueBytes(Regtype value) {
        return fixedSize();
    }

    @Override
    public Sort sortSupport() {
        return Sort.COMPARATOR;
    }

}
