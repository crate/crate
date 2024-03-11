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
import io.crate.types.DataType;

class TDigestStateType extends DataType<TDigestState> implements Streamer<TDigestState> {

    static final int ID = 5120;
    static final TDigestStateType INSTANCE = new TDigestStateType();

    private TDigestStateType() {
    }

    @Override
    public TDigestState readValueFrom(StreamInput in) throws IOException {
        return TDigestState.read(in);
    }

    @Override
    public void writeValueTo(StreamOutput out, TDigestState v) throws IOException {
        TDigestState.write(v, out);
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.CUSTOM;
    }

    @Override
    public String getName() {
        return "percentile_state";
    }

    @Override
    public Streamer<TDigestState> streamer() {
        return this;
    }

    @Override
    public TDigestState sanitizeValue(Object value) {
        return (TDigestState) value;
    }

    @Override
    public int compare(TDigestState val1, TDigestState val2) {
        return 0;
    }

    @Override
    public long valueBytes(TDigestState value) {
        throw new UnsupportedOperationException("valueSize is not implemented for TDigestStateType");
    }
}
