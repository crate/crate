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

package io.crate.protocols.postgres.types;

import org.jetbrains.annotations.NotNull;

import io.netty.buffer.ByteBuf;

abstract class BaseTimestampType extends PGType<Long> {

    protected static final int TYPE_LEN = 8;
    protected static final int TYPE_MOD = -1;

    // 1st msec where BC date becomes AD date
    protected static final long FIRST_MSEC_AFTER_CHRIST = -62135596800000L;

    // amount of seconds between 1970-01-01 and 2000-01-01
    private static final long EPOCH_DIFF_IN_MS = 946_684_800_000L;


    BaseTimestampType(int oid, int typeLen, int typeMod, @NotNull String typeName) {
        super(oid, typeLen, typeMod, typeName);
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @NotNull Long value) {
        buffer.writeInt(TYPE_LEN);
        buffer.writeLong(toPgTimestamp((long) value));
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    public String typeCategory() {
        return TypeCategory.DATETIME.code();
    }

    @Override
    public String type() {
        return Type.BASE.code();
    }

    /**
     * Convert a crate timestamp (unix timestamp in ms) into a postgres timestamp
     * (long microseconds since 2000-01-01)
     */
    private static long toPgTimestamp(long unixTsInMs) {
        return (unixTsInMs - EPOCH_DIFF_IN_MS) * 1000;
    }

    @Override
    public Long readBinaryValue(ByteBuf buffer, int valueLength) {
        assert valueLength == TYPE_LEN : "valueLength must be " + TYPE_LEN +
            " because timestamp is a 64 bit long. Actual length: " + valueLength;
        long microSecondsSince2K = buffer.readLong();
        return toCrateTimestamp(microSecondsSince2K);
    }

    /**
     * Convert a postgres timestamp (seconds since 2000-01-01) into a crate
     * timestamp (unix timestamp in ms).
     */
    private static long toCrateTimestamp(long microSecondsSince2k) {
        return (microSecondsSince2k / 1000) + EPOCH_DIFF_IN_MS;
    }
}
