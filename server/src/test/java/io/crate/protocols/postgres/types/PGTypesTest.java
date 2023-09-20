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

import static io.crate.testing.Asserts.assertThat;
import static io.crate.types.DataTypes.GEO_POINT;
import static io.crate.types.DataTypes.GEO_SHAPE;
import static io.crate.types.DataTypes.PRIMITIVE_TYPES;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.elasticsearch.test.ESTestCase;
import org.joda.time.Period;
import org.junit.Test;

import io.crate.data.Row1;
import io.crate.sql.tree.BitString;
import io.crate.testing.DataTypeTesting;
import io.crate.types.ArrayType;
import io.crate.types.BitStringType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FloatType;
import io.crate.types.LongType;
import io.crate.types.ObjectType;
import io.crate.types.RowType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class PGTypesTest extends ESTestCase {

    @Test
    public void testCrate2PGType() {
        assertThat(PGTypes.get(DataTypes.STRING)).isExactlyInstanceOf(VarCharType.class);
        assertThat(PGTypes.get(DataTypes.UNTYPED_OBJECT)).isExactlyInstanceOf(JsonType.class);
        assertThat(PGTypes.get(DataTypes.BOOLEAN)).isExactlyInstanceOf(BooleanType.class);
        assertThat(PGTypes.get(DataTypes.SHORT)).isExactlyInstanceOf(SmallIntType.class);
        assertThat(PGTypes.get(DataTypes.INTEGER)).isExactlyInstanceOf(IntegerType.class);
        assertThat(PGTypes.get(DataTypes.LONG)).isExactlyInstanceOf(BigIntType.class);
        assertThat(PGTypes.get(DataTypes.FLOAT)).isExactlyInstanceOf(RealType.class);
        assertThat(PGTypes.get(DataTypes.DOUBLE)).isExactlyInstanceOf(DoubleType.class);
        assertThat(PGTypes.get(DataTypes.DATE)).isExactlyInstanceOf(DateType.class);
        assertThat(PGTypes.get(DataTypes.IP))
            .as("Crate IP type is mapped to PG varchar")
            .isExactlyInstanceOf(VarCharType.class);
        assertThat(PGTypes.get(DataTypes.NUMERIC)).isExactlyInstanceOf(NumericType.class);
        assertThat(PGTypes.get(io.crate.types.JsonType.INSTANCE)).isExactlyInstanceOf(JsonType.class);

    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void test_undefined_type_can_stream_non_string_values() {
        PGType pgType = PGTypes.get(DataTypes.UNDEFINED);
        pgType.writeAsBinary(Unpooled.buffer(), 30);
    }

    @Test
    public void testPG2CrateType() {
        assertThat(PGTypes.fromOID(VarCharType.OID)).isExactlyInstanceOf(StringType.class);
        assertThat(PGTypes.fromOID(JsonType.OID)).isExactlyInstanceOf(ObjectType.class);
        assertThat(PGTypes.fromOID(BooleanType.OID)).isExactlyInstanceOf(io.crate.types.BooleanType.class);
        assertThat(PGTypes.fromOID(SmallIntType.OID)).isExactlyInstanceOf(ShortType.class);
        assertThat(PGTypes.fromOID(IntegerType.OID)).isExactlyInstanceOf(io.crate.types.IntegerType.class);
        assertThat(PGTypes.fromOID(BigIntType.OID)).isExactlyInstanceOf(LongType.class);
        assertThat(PGTypes.fromOID(RealType.OID)).isExactlyInstanceOf(FloatType.class);
        assertThat(PGTypes.fromOID(DoubleType.OID)).isExactlyInstanceOf(io.crate.types.DoubleType.class);
        assertThat(PGTypes.fromOID(NumericType.OID)).isExactlyInstanceOf(io.crate.types.NumericType.class);
    }

    @Test
    public void testTextOidIsMappedToString() {
        assertThat(PGTypes.fromOID(25)).isEqualTo(DataTypes.STRING);
        assertThat(PGTypes.fromOID(1009)).isEqualTo(new ArrayType<>(DataTypes.STRING));
    }

    @Test
    public void testCrateCollection2PgType() {
        for (DataType<?> type : PRIMITIVE_TYPES) {
            assertThat(PGTypes.get(new ArrayType<>(type))).isExactlyInstanceOf(PGArray.class);
        }
        assertThat(PGTypes.get(new ArrayType<>(GEO_POINT))).isExactlyInstanceOf(PGArray.class);
        assertThat(PGTypes.get(new ArrayType<>(GEO_SHAPE))).isExactlyInstanceOf(PGArray.class);
    }

    @Test
    public void testPgArray2CrateType() {
        assertThat(PGTypes.fromOID(PGArray.CHAR_ARRAY.oid())).isExactlyInstanceOf(ArrayType.class);
        assertThat(PGTypes.fromOID(PGArray.INT2_ARRAY.oid())).isExactlyInstanceOf(ArrayType.class);
        assertThat(PGTypes.fromOID(PGArray.INT4_ARRAY.oid())).isExactlyInstanceOf(ArrayType.class);
        assertThat(PGTypes.fromOID(PGArray.INT8_ARRAY.oid())).isExactlyInstanceOf(ArrayType.class);
        assertThat(PGTypes.fromOID(PGArray.FLOAT4_ARRAY.oid())).isExactlyInstanceOf(ArrayType.class);
        assertThat(PGTypes.fromOID(PGArray.FLOAT8_ARRAY.oid())).isExactlyInstanceOf(ArrayType.class);
        assertThat(PGTypes.fromOID(PGArray.BOOL_ARRAY.oid())).isExactlyInstanceOf(ArrayType.class);
        assertThat(PGTypes.fromOID(PGArray.TIMESTAMPZ_ARRAY.oid())).isExactlyInstanceOf(ArrayType.class);
        assertThat(PGTypes.fromOID(PGArray.TIMESTAMP_ARRAY.oid())).isExactlyInstanceOf(ArrayType.class);
        assertThat(PGTypes.fromOID(PGArray.DATE_ARRAY.oid())).isExactlyInstanceOf(ArrayType.class);
        assertThat(PGTypes.fromOID(PGArray.VARCHAR_ARRAY.oid())).isExactlyInstanceOf(ArrayType.class);
        assertThat(PGTypes.fromOID(PGArray.JSON_ARRAY.oid())).isExactlyInstanceOf(ArrayType.class);
    }

    private static class Entry<T> {
        final DataType<T> type;
        final Object value;

        public Entry(DataType<T> type, Object value) {
            this.type = type;
            this.value = value;
        }
    }

    @Test
    public void testByteReadWrite() {
        for (var entry : List.of(
            new Entry<>(new ArrayType<>(DataTypes.INTEGER), Arrays.asList(10, null, 20)),
            new Entry<>(new ArrayType<>(DataTypes.INTEGER), List.of()),
            new Entry<>(new ArrayType<>(DataTypes.INTEGER), Arrays.asList(null, null)),
            new Entry<>(new ArrayType<>(DataTypes.INTEGER), Arrays.asList(Arrays.asList(10, null, 20), Arrays.asList(1, 2, 3)))
        )) {
            var pgType = PGTypes.get(entry.type);
            assertEntryOfPgType(entry, pgType);
        }
    }

    @Test
    public void test_pgtypes_has_en_entry_for_each_typelem() throws Exception {
        Map<Integer, PGType<?>> typeByOid = StreamSupport.stream(PGTypes.pgTypes().spliterator(), false)
            .collect(Collectors.toMap(PGType::oid, x -> x));
        for (var type : PGTypes.pgTypes()) {
            if (type.typElem() == 0) {
                continue;
            }
            assertThat(typeByOid.get(type.typElem()))
                .as("The element type with oid " + type.typElem() + " must exist for " + type.typName())
                .isNotNull();
        }
    }


    @Test
    public void test_period_binary_round_trip_streaming() {
        var entry = new Entry<>(DataTypes.INTERVAL, new Period(1, 2, 3, 4, 5, 6, 7, 8));
        assertThat(writeAndReadBinary(entry, IntervalType.INSTANCE)).isEqualTo(entry.value);
    }

    @Test
    public void test_period_text_round_trip_streaming() {
        var entry = new Entry<>(DataTypes.INTERVAL, new Period(1, 2, 3, 4, 5, 6, 7, 8));
        assertThat(writeAndReadAsText(entry, IntervalType.INSTANCE)).isEqualTo(entry.value);
    }

    @Test
    public void testReadWriteVarCharType() {
        assertEntryOfPgType(new Entry<>(DataTypes.STRING, "test"), VarCharType.INSTANCE);
    }

    @Test
    public void test_binary_oidvector_streaming_roundtrip() throws Exception {
        var entry = new Entry<>(DataTypes.OIDVECTOR, List.of(1, 2, 3, 4));
        assertThat(writeAndReadBinary(entry, PGTypes.get(entry.type))).isEqualTo(entry.value);
    }

    @Test
    public void test_text_oidvector_streaming_roundtrip() throws Exception {
        var entry = new Entry<>(DataTypes.OIDVECTOR, List.of(1, 2, 3, 4));
        assertThat(writeAndReadAsText(entry, PGTypes.get(entry.type))).isEqualTo(entry.value);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void test_can_retrieve_pg_type_for_record_array() throws Exception {
        var pgType = PGTypes.get(new ArrayType<>(new RowType(List.of(DataTypes.STRING))));
        assertThat(pgType.oid()).isEqualTo(PGArray.EMPTY_RECORD_ARRAY.oid());

        byte[] bytes = ((PGType) pgType).encodeAsUTF8Text(List.of(new Row1("foobar")));
        assertThat(new String(bytes, StandardCharsets.UTF_8)).isEqualTo("{\"(foobar)\"}");
    }

    private <V> void assertEntryOfPgType(Entry<?> entry, PGType<V> pgType) {
        assertThat(writeAndReadBinary(entry, pgType))
            .as("Binary write/read round-trip for `" + pgType.typName() + "` must not change value")
            .isEqualTo(entry.value);
        var streamedValue = writeAndReadAsText(entry, pgType);
        assertThat(streamedValue)
            .as("Text write/read round-trip for `" + pgType.typName() + "` must not change value")
            .isEqualTo(entry.value);
    }

    @Test
    public void test_all_types_exposed_via_pg_type_table_can_be_resolved_via_oid() throws Exception {
        for (var type : PGTypes.pgTypes()) {
            assertThat(PGTypes.fromOID(type.oid()))
                .as("Must be possible to retrieve " + type.typName() + "/" + type.oid() + " via PGTypes.fromOID")
                .isNotNull();
        }
    }

    @Test
    public void test_bit_binary_round_trip_streaming() {
        int bitLength = randomIntBetween(1, 40);
        BitStringType type = new BitStringType(bitLength);
        Supplier<BitString> dataGenerator = DataTypeTesting.getDataGenerator(type);
        PGType<?> bitType = PGTypes.get(type);
        Entry<?> entry = new Entry<>(type, dataGenerator.get());
        assertThat(writeAndReadBinary(entry, bitType)).isEqualTo(entry.value);
    }


    @SuppressWarnings("unchecked")
    @Test
    public void test_typsend_and_receive_names_match_postgresql() throws Exception {
        // Some types have `<name>send`, others `<name>_send`
        // Needs to match PostgreSQL because clients may depend on this (concrete example is Postgrex)
        Set<PGType<?>> withUnderscore = Set.of(
            TimestampType.INSTANCE,
            BitType.INSTANCE,
            DateType.INSTANCE,
            IntervalType.INSTANCE,
            JsonType.INSTANCE,
            NumericType.INSTANCE,
            PointType.INSTANCE,
            TimestampZType.INSTANCE,
            TimeTZType.INSTANCE,
            RecordType.EMPTY_RECORD
        );

        for (var type : PGTypes.pgTypes()) {
            if (type.oid() == 2277) {
                assertThat(type.typSend().name()).isEqualTo("anyarray_send");
                assertThat(type.typReceive().name()).isEqualTo("anyarray_recv");
            } else if (type.oid() == 2276) {
                assertThat(type.typSend().name()).isEqualTo("-");
            } else if (type.typArray() == 0) {
                assertThat(type.typSend().name()).isEqualTo("array_send");
            } else {
                if (withUnderscore.contains(type)) {
                    assertThat(type.typSend().name()).isEqualTo(type.typName() + "_send");
                    assertThat(type.typReceive().name()).isEqualTo(type.typName() + "_recv");
                } else {
                    assertThat(type.typSend().name()).isEqualTo(type.typName() + "send");
                    assertThat(type.typReceive().name()).isEqualTo(type.typName() + "recv");
                }
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object writeAndReadBinary(Entry<?> entry, PGType pgType) {
        ByteBuf buffer = Unpooled.buffer();
        try {
            pgType.writeAsBinary(buffer, entry.value);
            int length = buffer.readInt();
            Object result = pgType.readBinaryValue(buffer, length);
            assertThat(buffer.readableBytes()).isEqualTo(0);
            return result;
        } finally {
            buffer.release();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Object writeAndReadAsText(Entry<?> entry, PGType pgType) {
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
