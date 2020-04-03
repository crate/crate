/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.operation.aggregation;

import com.carrotsearch.hppc.BitMixer;
import com.google.common.annotations.VisibleForTesting;
import io.crate.Streamer;
import io.crate.breaker.RamAccounting;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.impl.HyperLogLogPlusPlus;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.module.EnterpriseFunctionsModule;
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
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

public class HyperLogLogDistinctAggregation extends AggregationFunction<HyperLogLogDistinctAggregation.HllState, Long> {

    static final String NAME = "hyperloglog_distinct";

    static {
        DataTypes.register(HllStateType.ID, in -> HllStateType.INSTANCE);
    }

    public static void register(EnterpriseFunctionsModule mod) {
        for (DataType<?> t : DataTypes.PRIMITIVE_TYPES) {
            mod.register(new HyperLogLogDistinctAggregation(new FunctionInfo(
                new FunctionIdent(NAME, Collections.singletonList(t)), DataTypes.LONG,
                FunctionInfo.Type.AGGREGATE),
                t));
            mod.register(new HyperLogLogDistinctAggregation(new FunctionInfo(
                new FunctionIdent(NAME, Arrays.asList(t, DataTypes.INTEGER)), DataTypes.LONG,
                FunctionInfo.Type.AGGREGATE),
                t));
        }
    }

    private final FunctionInfo info;
    private final DataType dataType;

    private HyperLogLogDistinctAggregation(FunctionInfo info, DataType dataType) {
        this.info = info;
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
                precision = DataTypes.INTEGER.value(args[1].value());
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

    @Override
    public DataType partialType() {
        return HllStateType.INSTANCE;
    }

    @Override
    public FunctionInfo info() {
        return info;
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

        boolean isInitialized() {
            return hyperLogLogPlusPlus != null;
        }

        void add(Object value) {
            hyperLogLogPlusPlus.collect(murmur3Hash.hash(value));
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
        public HyperLogLogDistinctAggregation.HllState value(Object value) throws IllegalArgumentException, ClassCastException {
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
                return BitMixer.mix64(DataTypes.LONG.value(val));
            }
        }

        private static class Double extends Murmur3Hash {

            private static final Double INSTANCE = new Double();

            @Override
            long hash(Object val) {
                return BitMixer.mix64(java.lang.Double.doubleToLongBits(DataTypes.DOUBLE.value(val)));
            }
        }

        static class Bytes64 extends Murmur3Hash {

            static final Bytes64 INSTANCE = new Bytes64();

            @Override
            long hash(Object val) {
                byte[] bytes = DataTypes.STRING.value(val).getBytes(StandardCharsets.UTF_8);
                return MurmurHash3.hash64(bytes, bytes.length);
            }
        }

        static class Bytes extends Murmur3Hash {

            private final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();

            @Override
            long hash(Object val) {
                byte[] bytes = DataTypes.STRING.value(val).getBytes(StandardCharsets.UTF_8);
                MurmurHash3.hash128(bytes, 0, bytes.length, 0, hash);
                return hash.h1;
            }
        }
    }
}
