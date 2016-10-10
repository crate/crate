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

import com.google.common.collect.ImmutableList;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.*;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class PGTypesTest extends CrateUnitTest {

    @Test
    public void testCrate2PGType() throws Exception {
        assertThat(PGTypes.get(DataTypes.STRING), instanceOf(VarCharType.class));
        assertThat(PGTypes.get(DataTypes.OBJECT), instanceOf(JsonType.class));
        assertThat(PGTypes.get(DataTypes.BOOLEAN), instanceOf(BooleanType.class));
        assertThat(PGTypes.get(DataTypes.SHORT), instanceOf(SmallIntType.class));
        assertThat(PGTypes.get(DataTypes.INTEGER), instanceOf(IntegerType.class));
        assertThat(PGTypes.get(DataTypes.LONG), instanceOf(BigIntType.class));
        assertThat(PGTypes.get(DataTypes.FLOAT), instanceOf(RealType.class));
        assertThat(PGTypes.get(DataTypes.DOUBLE), instanceOf(DoubleType.class));
    }

    @Test
    public void testPG2CrateType() throws Exception {
        assertThat(PGTypes.fromOID(VarCharType.OID), instanceOf(StringType.class));
        assertThat(PGTypes.fromOID(JsonType.OID), instanceOf(ObjectType.class));
        assertThat(PGTypes.fromOID(BooleanType.OID), instanceOf(io.crate.types.BooleanType.class));
        assertThat(PGTypes.fromOID(SmallIntType.OID), instanceOf(ShortType.class));
        assertThat(PGTypes.fromOID(IntegerType.OID), instanceOf(io.crate.types.IntegerType.class));
        assertThat(PGTypes.fromOID(BigIntType.OID), instanceOf(LongType.class));
        assertThat(PGTypes.fromOID(RealType.OID), instanceOf(FloatType.class));
        assertThat(PGTypes.fromOID(DoubleType.OID), instanceOf(io.crate.types.DoubleType.class));
    }

    private static class Entry {
        final DataType type;
        final Object value;

        public Entry(DataType type, Object value) {
            this.type = type;
            this.value = value;
        }
    }

    @Test
    public void testByteReadWrite() throws Exception {
        for (Entry entry : ImmutableList.of(
            new Entry(DataTypes.STRING, TestingHelpers.bytesRef("foobar", random())),
            new Entry(DataTypes.LONG, 392873L),
            new Entry(DataTypes.INTEGER, 1234),
            new Entry(DataTypes.SHORT, (short) 42),
            new Entry(DataTypes.FLOAT, 42.3f),
            new Entry(DataTypes.DOUBLE, 42.00003),
            new Entry(DataTypes.BOOLEAN, true),
            new Entry(DataTypes.TIMESTAMP, DataTypes.TIMESTAMP.value("2014-05-08")),
            new Entry(DataTypes.TIMESTAMP, DataTypes.TIMESTAMP.value("2014-05-08T16:34:33.123")),
            new Entry(DataTypes.TIMESTAMP, DataTypes.TIMESTAMP.value(999999999999999L)),
            new Entry(DataTypes.TIMESTAMP, DataTypes.TIMESTAMP.value(-999999999999999L)),
            new Entry(DataTypes.IP, TestingHelpers.bytesRef("192.168.1.1", random())),
            new Entry(DataTypes.BYTE, (byte) 20),
            new Entry(new ArrayType(DataTypes.INTEGER), new Integer[] {10, null, 20}),
            new Entry(new ArrayType(DataTypes.INTEGER), new Integer[0]),
            new Entry(new ArrayType(DataTypes.INTEGER), new Integer[] {null, null}),
            new Entry(new ArrayType(DataTypes.INTEGER), new Integer[][] { new Integer[] {10, null, 20}, new Integer[] { 1, 2, 3}})
        )) {

            PGType pgType = PGTypes.get(entry.type);

            Object streamedValue = writeAndReadBinary(entry, pgType);
            assertThat(streamedValue, is(entry.value));

            streamedValue = writeAndReadAsText(entry, pgType);
            assertThat(streamedValue, is(entry.value));
        }
    }

    private Object writeAndReadBinary(Entry entry, PGType pgType) {
        ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
        pgType.writeAsBinary(buffer, entry.value);
        int length = buffer.readInt();
        return pgType.readBinaryValue(buffer, length);
    }

    private Object writeAndReadAsText(Entry entry, PGType pgType) {
        ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
        pgType.writeAsText(buffer, entry.value);
        int length = buffer.readInt();
        return pgType.readTextValue(buffer, length);
    }
}
