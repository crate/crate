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

package io.crate.operation.aggregation;

import static java.lang.Double.doubleToLongBits;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.annotation.Nullable;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.DocValueFormat;

import io.crate.Streamer;
import io.crate.breaker.RamAccounting;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.execution.engine.aggregation.impl.HyperLogLogPlusPlus;
import io.crate.execution.engine.aggregation.impl.templates.SortedNumericDocValueAggregator;
import io.crate.expression.symbol.Literal;
import io.crate.memory.MemoryManager;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.Signature;
import io.crate.module.ExtraFunctionsModule;
import io.crate.types.BooleanType;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;

public class HyperLogLogDistinctAggregation extends AggregationFunction<HyperLogLogDistinctAggregation.HllState, Long> {

    static final String NAME = "hyperloglog_distinct";

    static {
        DataTypes.register(HllStateType.ID, in -> HllStateType.INSTANCE);
    }

    public static void register(ExtraFunctionsModule mod) {
        for (var supportedType : DataTypes.PRIMITIVE_TYPES) {
            mod.register(
                Signature.aggregate(
                    NAME,
                    supportedType.getTypeSignature(),
                    DataTypes.LONG.getTypeSignature()),
                (signature, boundSignature) ->
                    new HyperLogLogDistinctAggregation(signature, boundSignature, supportedType)
            );
            mod.register(
                Signature.aggregate(
                    NAME,
                    supportedType.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.LONG.getTypeSignature()
                ),
                (signature, boundSignature) ->
                    new HyperLogLogDistinctAggregation(signature, boundSignature, supportedType)
            );
        }
    }

    private final Signature signature;
    private final Signature boundSignature;
    private final DataType<?> dataType;

    private HyperLogLogDistinctAggregation(Signature signature,
                                           Signature boundSignature,
                                           DataType<?> dataType) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.dataType = dataType;
    }

    @Nullable
    @Override
    public HllState newState(RamAccounting ramAccounting,
                             Version indexVersionCreated,
                             Version minNodeInCluster,
                             MemoryManager memoryManager) {
        return new HllState(dataType, minNodeInCluster.onOrAfter(Version.V_4_1_0));
    }

    @Override
    public HllState iterate(RamAccounting ramAccounting,
                            MemoryManager memoryManager,
                            HllState state,
                            Input... args) throws CircuitBreakingException {
        if (state.isInitialized() == false) {
            int precision = HyperLogLogPlusPlus.DEFAULT_PRECISION;
            if (args.length > 1) {
                precision = DataTypes.INTEGER.sanitizeValue(args[1].value());
            }
            state.init(memoryManager, precision);
        }
        Object value = args[0].value();
        if (value != null) {
            state.add(value);
        }
        return state;
    }

    @Override
    public HllState reduce(RamAccounting ramAccounting, HllState state1, HllState state2) {
        if (state1.isInitialized() == false) {
            return state2;
        }
        if (state2.isInitialized()) {
            state1.merge(state2);
        }
        return state1;
    }

    @Override
    public Long terminatePartial(RamAccounting ramAccounting, HllState state) {
        if (state.isInitialized()) {
            return state.value();
        }
        return null;
    }

    @Nullable
    @Override
    public DocValueAggregator<?> getDocValueAggregator(List<Reference> aggregationReferences,
                                                       DocTableInfo table,
                                                       List<Literal<?>> optionalParams) {
        if (aggregationReferences.stream().anyMatch(x -> !x.hasDocValues())) {
            return null;
        }
        Reference reference = aggregationReferences.get(0);
        var dataType = reference.valueType();
        switch (dataType.id()) {
            case ByteType.ID:
            case ShortType.ID:
            case IntegerType.ID:
            case LongType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
                return new SortedNumericDocValueAggregator<>(
                    reference.column().fqn(),
                    (ramAccounting, memoryManager, minNodeVersion) -> {
                        var state = new HllState(dataType, minNodeVersion.onOrAfter(Version.V_4_1_0));
                        var precision = optionalParams.size() == 1 ? (Integer) optionalParams.get(0).value() : HyperLogLogPlusPlus.DEFAULT_PRECISION;
                        return initIfNeeded(state, memoryManager, precision);
                    },
                    (values, state) -> {
                        var hash = BitMixer.mix64(values.nextValue());
                        state.addHash(hash);
                    }
                );
            case DoubleType.ID:
                return new SortedNumericDocValueAggregator<>(
                    reference.column().fqn(),
                    (ramAccounting, memoryManager, minNodeVersion) -> {
                        var state = new HllState(dataType, minNodeVersion.onOrAfter(Version.V_4_1_0));
                        var precision = optionalParams.size() == 1 ? (Integer) optionalParams.get(0).value() : HyperLogLogPlusPlus.DEFAULT_PRECISION;
                        return initIfNeeded(state, memoryManager, precision);
                    },
                    (values, state) -> {
                        // Murmur3Hash.Double in regular aggregation calls mix64 of doubleToLongBits(double value).
                        // Equivalent of that in context of double DocValue is
                        // mix64(doubleToLongBits(decoded))
                        // => mix64(doubleToLongBits(NumericUtils.sortableLongToDouble(values.nextValue())))
                        // => mix64(doubleToLongBits(Double.longBitsToDouble(sortableDoubleBits(values.nextValue()))))
                        // => mix64(sortableDoubleBits(values.nextValue()))
                        var hash = BitMixer.mix64(NumericUtils.sortableDoubleBits(values.nextValue()));
                        state.addHash(hash);
                    }
                );
            case FloatType.ID:
                return new SortedNumericDocValueAggregator<>(
                    reference.column().fqn(),
                    (ramAccounting, memoryManager, minNodeVersion) -> {
                        var state = new HllState(dataType, minNodeVersion.onOrAfter(Version.V_4_1_0));
                        var precision = optionalParams.size() == 1 ? (Integer) optionalParams.get(0).value() : HyperLogLogPlusPlus.DEFAULT_PRECISION;
                        return initIfNeeded(state, memoryManager, precision);
                    },
                    (values, state) -> {
                        // Murmur3Hash.Float in regular aggregation calls mix64 of doubleToLongBits(double value).
                        // Equivalent of that in context of float DocValue is
                        // mix64(doubleToLongBits(decoded))
                        // => mix64(doubleToLongBits((double)NumericUtils.sortableIntToFloat((int)values.nextValue())))
                        // => mix64(doubleToLongBits(NumericUtils.sortableIntToFloat((int)values.nextValue()))) - no need to double cast, auto widening
                        var hash = BitMixer.mix64(doubleToLongBits(NumericUtils.sortableIntToFloat((int) values.nextValue())));
                        state.addHash(hash);
                    }
                );
            case StringType.ID:
                var precision = optionalParams.size() == 1 ? (Integer) optionalParams.get(0).value() : HyperLogLogPlusPlus.DEFAULT_PRECISION;
                return new HllAggregator(reference.column().fqn(), dataType, precision) {
                    @Override
                    public void apply(RamAccounting ramAccounting, int doc, HllState state) throws IOException {
                        if (super.values.advanceExact(doc) && super.values.docValueCount() == 1) {
                            BytesRef ref = super.values.nextValue();
                            var hash = state.isAllOn4_1() ?
                                MurmurHash3.hash64(ref.bytes, ref.offset, ref.length)
                                : MurmurHash3.hash128(ref.bytes, ref.offset, ref.length, 0, super.hash128).h1;

                            state.addHash(hash);
                        }
                    }
                };
            case IpType.ID:
                var ipPrecision = optionalParams.size() == 1 ? (Integer) optionalParams.get(0).value() : HyperLogLogPlusPlus.DEFAULT_PRECISION;
                return new HllAggregator(reference.column().fqn(), dataType, ipPrecision) {
                    @Override
                    public void apply(RamAccounting ramAccounting, int doc, HllState state) throws IOException {
                        if (super.values.advanceExact(doc) && super.values.docValueCount() == 1) {
                            BytesRef ref = super.values.nextValue();
                            byte[] bytes = ((String) DocValueFormat.IP.format(ref)).getBytes(StandardCharsets.UTF_8);

                            var hash = state.isAllOn4_1() ?
                                MurmurHash3.hash64(bytes, 0, bytes.length)
                                : MurmurHash3.hash128(bytes, 0, bytes.length, 0, super.hash128).h1;
                            state.addHash(hash);
                        }
                    }
                };
            default:
                return null;
        }
    }

    private abstract static class HllAggregator implements DocValueAggregator<HllState> {

        private final String columnName;
        private final DataType<?> dataType;
        private final Integer precision;
        private final MurmurHash3.Hash128 hash128 = new MurmurHash3.Hash128();
        private SortedBinaryDocValues values;

        public HllAggregator(String columnName, DataType<?> dataType, Integer precision) {
            this.columnName = columnName;
            this.dataType = dataType;
            this.precision = precision;
        }

        @Override
        public HllState initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion) {
            var state = new HllState(dataType, minNodeVersion.onOrAfter(Version.V_4_1_0));
            return initIfNeeded(state, memoryManager, precision);
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = FieldData.toString(DocValues.getSortedSet(reader, columnName));
        }

        @Override
        public abstract void apply(RamAccounting ramAccounting, int doc, HllState state) throws IOException;

        @Override
        public Object partialResult(RamAccounting ramAccounting, HllState state) {
            return state;
        }
    }

    @Override
    public DataType<?> partialType() {
        return HllStateType.INSTANCE;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    public static class HllState implements Comparable<HllState>, Writeable {

        private final DataType<?> dataType;
        private final Murmur3Hash murmur3Hash;
        private final boolean allOn4_1;

        // Although HyperLogLogPlus implements Releasable we do not close instances.
        // We're using BigArrays.NON_RECYCLING_INSTANCE and instances created using it are not accounted / recycled
        private HyperLogLogPlusPlus hyperLogLogPlusPlus;

        HllState(DataType<?> dataType, boolean allOn4_1) {
            this.dataType = dataType;
            this.allOn4_1 = allOn4_1;
            murmur3Hash = Murmur3Hash.getForType(dataType, allOn4_1);
        }

        HllState(StreamInput in) throws IOException {
            if (in.getVersion().onOrAfter(Version.V_4_1_0)) {
                this.allOn4_1 = in.readBoolean();
            } else {
                this.allOn4_1 = false;
            }
            dataType = DataTypes.fromStream(in);
            murmur3Hash = Murmur3Hash.getForType(dataType, allOn4_1);
            if (in.readBoolean()) {
                hyperLogLogPlusPlus = HyperLogLogPlusPlus.readFrom(in);
            }
        }

        void init(MemoryManager memoryManager, int precision) {
            assert hyperLogLogPlusPlus == null : "hyperLogLog algorithm was already initialized";
            try {
                hyperLogLogPlusPlus = new HyperLogLogPlusPlus(precision, memoryManager::allocate);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("precision must be >= 4 and <= 18");
            }
        }

        boolean isAllOn4_1() {
            return allOn4_1;
        }

        boolean isInitialized() {
            return hyperLogLogPlusPlus != null;
        }

        void add(Object value) {
            hyperLogLogPlusPlus.collect(murmur3Hash.hash(value));
        }

        void addHash(long hash) {
            hyperLogLogPlusPlus.collect(hash);
        }

        void merge(HllState state) {
            hyperLogLogPlusPlus.merge(state.hyperLogLogPlusPlus);
        }

        long value() {
            return hyperLogLogPlusPlus.cardinality();
        }

        @Override
        public int compareTo(HllState o) {
            return java.lang.Long.compare(hyperLogLogPlusPlus.cardinality(), o.hyperLogLogPlusPlus.cardinality());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            HllState that = (HllState) o;
            return this.compareTo(that) == 0;
        }

        @Override
        public String toString() {
            return String.valueOf(value());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().onOrAfter(Version.V_4_1_0)) {
                out.writeBoolean(allOn4_1);
            }
            DataTypes.toStream(dataType, out);
            if (isInitialized()) {
                out.writeBoolean(true);
                hyperLogLogPlusPlus.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    public static class HllStateType extends DataType<HyperLogLogDistinctAggregation.HllState>
        implements Streamer<HyperLogLogDistinctAggregation.HllState> {

        static final int ID = 17000;
        static final HllStateType INSTANCE = new HllStateType();

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
            return "hll_state";
        }

        @Override
        public Streamer<HllState> streamer() {
            return this;
        }

        @Override
        public HllState sanitizeValue(Object value) {
            return (HyperLogLogDistinctAggregation.HllState) value;
        }

        @Override
        public int compare(HyperLogLogDistinctAggregation.HllState val1, HyperLogLogDistinctAggregation.HllState val2) {
            if (val1 == null) {
                return -1;
            } else {
                return val1.compareTo(val2);
            }
        }

        @Override
        public HyperLogLogDistinctAggregation.HllState readValueFrom(StreamInput in) throws IOException {
            return new HllState(in);
        }

        @Override
        public void writeValueTo(StreamOutput out, HllState v) throws IOException {
            v.writeTo(out);
        }
    }

    private static HllState initIfNeeded(HllState state, MemoryManager memoryManager, Integer precision) {
        if (!state.isInitialized()) {
            state.init(memoryManager, precision);
        }
        return state;
    }

    @VisibleForTesting
    abstract static class Murmur3Hash {

        static Murmur3Hash getForType(DataType<?> dataType, boolean allOn4_1) {
            switch (dataType.id()) {
                case DoubleType.ID:
                case FloatType.ID:
                    return Double.INSTANCE;
                case LongType.ID:
                case IntegerType.ID:
                case ShortType.ID:
                case ByteType.ID:
                case TimestampType.ID_WITH_TZ:
                case TimestampType.ID_WITHOUT_TZ:
                    return Long.INSTANCE;
                case StringType.ID:
                case BooleanType.ID:
                case IpType.ID:
                    if (allOn4_1) {
                        return Bytes64.INSTANCE;
                    } else {
                        return new Bytes();
                    }
                default:
                    throw new IllegalArgumentException("data type \"" + dataType + "\" is not supported");
            }
        }

        abstract long hash(Object val);

        private static class Long extends Murmur3Hash {

            private static final Long INSTANCE = new Long();

            @Override
            long hash(Object val) {
                return BitMixer.mix64(DataTypes.LONG.sanitizeValue(val));
            }
        }

        private static class Double extends Murmur3Hash {

            private static final Double INSTANCE = new Double();

            @Override
            long hash(Object val) {
                return BitMixer.mix64(
                    doubleToLongBits(DataTypes.DOUBLE.sanitizeValue(val)));
            }
        }

        static class Bytes64 extends Murmur3Hash {

            static final Bytes64 INSTANCE = new Bytes64();

            @Override
            long hash(Object val) {
                byte[] bytes = DataTypes.STRING.implicitCast(val).getBytes(StandardCharsets.UTF_8);
                return MurmurHash3.hash64(bytes,0, bytes.length);
            }
        }

        static class Bytes extends Murmur3Hash {

            private final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();

            @Override
            long hash(Object val) {
                byte[] bytes = DataTypes.STRING.implicitCast(val).getBytes(StandardCharsets.UTF_8);
                MurmurHash3.hash128(bytes, 0, bytes.length, 0, hash);
                return hash.h1;
            }
        }
    }
}
