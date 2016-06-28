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

package io.crate.protocols.postgres.types;

import java.nio.ByteBuffer;

class DoubleType extends NumericPGType<Double> {

    static final int OID = 701;

    private static final int TYPE_LEN = 8;
    private static final int TYPE_MOD = -1;
    private static final short DEFAULT_FORMAT_CODE = FormatCode.BINARY;

    DoubleType() {
        super(OID, TYPE_LEN, TYPE_MOD, DEFAULT_FORMAT_CODE);
    }

    @Override
    void writeTo(ByteBuffer byteBuffer, Object value) {
        byteBuffer.putDouble((double) value);
    }

    @Override
    Double readFrom(ByteBuffer byteBuffer) {
        return byteBuffer.getDouble();
    }

    @Override
    Double fromString(String s) {
        return Double.parseDouble(s);
    }
}
