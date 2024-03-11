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

package io.crate.execution.engine.aggregation.impl;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;
import io.crate.execution.engine.aggregation.impl.CmpByAggregation.CompareBy;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class CompareByType extends DataType<CompareBy> implements Streamer<CompareBy> {

    public static final int ID = 1027;
    private final DataType<?> retValType;
    private final DataType<?> cmpType;

    public CompareByType(DataType<?> retValType, DataType<?> cmpType) {
        this.retValType = retValType;
        this.cmpType = cmpType;
    }

    public CompareByType(StreamInput in) throws IOException {
        this.retValType = DataTypes.fromStream(in);
        this.cmpType = DataTypes.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataTypes.toStream(this.retValType, out);
        DataTypes.toStream(this.cmpType, out);
    }

    @Override
    public int compare(CompareBy o1, CompareBy o2) {
        return o1.cmpValue.compareTo(o2.cmpValue);
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
        return "_cmp_by_type";
    }

    @Override
    public Streamer<CompareBy> streamer() {
        return this;
    }

    @Override
    public CompareBy sanitizeValue(Object value) {
        return (CompareBy) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompareBy readValueFrom(StreamInput in) throws IOException {
        Object retVal = retValType.streamer().readValueFrom(in);
        Object cmpVal = cmpType.streamer().readValueFrom(in);
        CompareBy compareBy = new CompareBy();
        compareBy.cmpValue = (Comparable<Object>) cmpVal;
        compareBy.resultValue = retVal;
        return compareBy;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void writeValueTo(StreamOutput out, CompareBy v) throws IOException {
        ((Streamer) retValType.streamer()).writeValueTo(out, v.resultValue);
        ((Streamer) cmpType.streamer()).writeValueTo(out, v.cmpValue);
    }

    @Override
    public long valueBytes(CompareBy value) {
        throw new UnsupportedOperationException("valueSize is not implemented for CompareByType");
    }
}
