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

import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FloatType;
import io.crate.types.LongType;
import io.crate.types.ObjectType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.joda.time.Period;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.types.DataTypes.GEO_POINT;
import static io.crate.types.DataTypes.GEO_SHAPE;
import static io.crate.types.DataTypes.PRIMITIVE_TYPES;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class PGTypesTest extends CrateUnitTest {

    @Test
    public void testCrate2PGType() {
        assertThat(PGTypes.get(DataTypes.STRING), instanceOf(VarCharType.class));
        assertThat(PGTypes.get(ObjectType.untyped()), instanceOf(JsonType.class));
        assertThat(PGTypes.get(DataTypes.BOOLEAN), instanceOf(BooleanType.class));
        assertThat(PGTypes.get(DataTypes.SHORT), instanceOf(SmallIntType.class));
        assertThat(PGTypes.get(DataTypes.INTEGER), instanceOf(IntegerType.class));
        assertThat(PGTypes.get(DataTypes.LONG), instanceOf(BigIntType.class));
        assertThat(PGTypes.get(DataTypes.FLOAT), instanceOf(RealType.class));
        assertThat(PGTypes.get(DataTypes.DOUBLE), instanceOf(DoubleType.class));
        assertThat(PGTypes.get(DataTypes.DOUBLE), instanceOf(DoubleType.class));
        assertThat("Crate IP type is mapped to PG varchar", PGTypes.get(DataTypes.IP),
            instanceOf(VarCharType.class));
    }

    @Test
    public void testPG2CrateType() {
        assertThat(PGTypes.fromOID(VarCharType.OID), instanceOf(StringType.class));
        assertThat(PGTypes.fromOID(JsonType.OID), instanceOf(ObjectType.class));
        assertThat(PGTypes.fromOID(BooleanType.OID), instanceOf(io.crate.types.BooleanType.class));
        assertThat(PGTypes.fromOID(SmallIntType.OID), instanceOf(ShortType.class));
        assertThat(PGTypes.fromOID(IntegerType.OID), instanceOf(io.crate.types.IntegerType.class));
        assertThat(PGTypes.fromOID(BigIntType.OID), instanceOf(LongType.class));
        assertThat(PGTypes.fromOID(RealType.OID), instanceOf(FloatType.class));
        assertThat(PGTypes.fromOID(DoubleType.OID), instanceOf(io.crate.types.DoubleType.class));
    }

    @Test
    public void testTextOidIsMappedToString() {
        assertThat(PGTypes.fromOID(25), is(DataTypes.STRING));
        assertThat(PGTypes.fromOID(1009), is(new ArrayType(DataTypes.STRING)));
    }

    @Test
    public void testCrateCollection2PgType() {
        for (DataType type : PRIMITIVE_TYPES) {
            assertThat(PGTypes.get(new ArrayType(type)), instanceOf(PGArray.class));
        }

        assertThat(PGTypes.get(new ArrayType(GEO_POINT)), instanceOf(PGArray.class));

        assertThat(PGTypes.get(new ArrayType(GEO_SHAPE)), instanceOf(PGArray.class));
    }

    @Test
    public void testPgArray2CrateType() {
        assertThat(PGTypes.fromOID(PGArray.CHAR_ARRAY.oid()), instanceOf(ArrayType.class));
        assertThat(PGTypes.fromOID(PGArray.INT2_ARRAY.oid()), instanceOf(ArrayType.class));
        assertThat(PGTypes.fromOID(PGArray.INT4_ARRAY.oid()), instanceOf(ArrayType.class));
        assertThat(PGTypes.fromOID(PGArray.INT8_ARRAY.oid()), instanceOf(ArrayType.class));
        assertThat(PGTypes.fromOID(PGArray.FLOAT4_ARRAY.oid()), instanceOf(ArrayType.class));
        assertThat(PGTypes.fromOID(PGArray.FLOAT8_ARRAY.oid()), instanceOf(ArrayType.class));
        assertThat(PGTypes.fromOID(PGArray.BOOL_ARRAY.oid()), instanceOf(ArrayType.class));
        assertThat(PGTypes.fromOID(PGArray.TIMESTAMPZ_ARRAY.oid()), instanceOf(ArrayType.class));
        assertThat(PGTypes.fromOID(PGArray.TIMESTAMP_ARRAY.oid()), instanceOf(ArrayType.class));
        assertThat(PGTypes.fromOID(PGArray.VARCHAR_ARRAY.oid()), instanceOf(ArrayType.class));
        assertThat(PGTypes.fromOID(PGArray.JSON_ARRAY.oid()), instanceOf(ArrayType.class));
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
    public void testByteReadWrite() {
        for (Entry entry : List.of(
            new Entry(new ArrayType<>(DataTypes.INTEGER), Arrays.asList(10, null, 20)),
            new Entry(new ArrayType<>(DataTypes.INTEGER), List.of()),
            new Entry(new ArrayType<>(DataTypes.INTEGER), Arrays.asList(null, null)),
            new Entry(new ArrayType<>(DataTypes.INTEGER), Arrays.asList(Arrays.asList(10, null, 20), Arrays.asList(1, 2, 3)))
        )) {
            PGType pgType = PGTypes.get(entry.type);
            assertEntryOfPgType(entry, pgType);
        }
    }


    @Test
    public void test_period_binary_round_trip_streaming() {
        Entry entry = new Entry(DataTypes.INTERVAL, new Period(1, 2, 3, 4, 5, 6, 7, 8));
        assertThat(
            writeAndReadBinary(entry, IntervalType.INSTANCE),
            is(entry.value)
        );
    }

    @Test
    public void test_period_text_round_trip_streaming() {
        Entry entry = new Entry(DataTypes.INTERVAL, new Period(1, 2, 3, 4, 5, 6, 7, 8));
        assertThat(
            writeAndReadAsText(entry, IntervalType.INSTANCE),
            is(entry.value)
        );
    }

    @Test
    public void testReadWriteVarCharType() {
        assertEntryOfPgType(new Entry(DataTypes.STRING, "test"), VarCharType.INSTANCE);
    }

    private void assertEntryOfPgType(Entry entry, PGType pgType) {
        assertThat(
            "Binary write/read round-trip for `" + pgType.typName() + "` must not change value",
            writeAndReadBinary(entry, pgType),
            is(entry.value)
        );
        var streamedValue = writeAndReadAsText(entry, pgType);
        assertThat(
            "Text write/read round-trip for `" + pgType.typName() + "` must not change value",
            streamedValue,
            is(entry.value)
        );
    }

    private Object writeAndReadBinary(Entry entry, PGType pgType) {
        ByteBuf buffer = Unpooled.buffer();
        try {
            pgType.writeAsBinary(buffer, entry.value);
            int length = buffer.readInt();
            return pgType.readBinaryValue(buffer, length);
        } finally {
            buffer.release();
        }
    }

    private Object writeAndReadAsText(Entry entry, PGType pgType) {
        ByteBuf buffer = Unpooled.buffer();
        try {
            pgType.writeAsText(buffer, entry.value);
            int length = buffer.readInt();
            return pgType.readTextValue(buffer, length);
        } finally {
            buffer.release();
        }
    }
}
