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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.Streamer;
import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;

public final class LTTBAggregation
        extends
        AggregationFunction<LTTBAggregation.LttbState, Map<String, List<? extends Number>>> {
    static final String NAME = "lttb";

    static {
        DataTypes.register(LttbStateType.ID, _ -> LttbStateType.INSTANCE);
    }

    static final List<DataType<?>> SUPPORTED_X_TYPES = List.of(DataTypes.TIMESTAMP,
            DataTypes.TIMESTAMPZ);

    private final Signature signature;
    private final BoundSignature boundSignature;

    record Point(long timestamp, double value) {
        public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Point.class);
    }

    private LTTBAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    public static void register(Functions.Builder builder) {
        for (var supportedXType : SUPPORTED_X_TYPES) {
            builder.add(
                    Signature.builder(NAME, FunctionType.AGGREGATE)
                            .argumentTypes(
                                    supportedXType.getTypeSignature(),
                                    DataTypes.DOUBLE.getTypeSignature(),
                                    // Third argument is threshold (the number of points to be returned)
                                    DataTypes.INTEGER.getTypeSignature())
                            .returnType(DataTypes.UNTYPED_OBJECT.getTypeSignature())
                            .features(Scalar.Feature.DETERMINISTIC)
                            .build(),
                    LTTBAggregation::new);
        }
    }

    @Override
    public LttbState reduce(RamAccounting ramAccounting, LttbState state1, LttbState state2) {
        if (state1.isInitialized() == false) {
            return state2;
        }

        if (state2.isInitialized()) {
            state1.merge(ramAccounting, state2);
        }
        return state1;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public DataType<?> partialType() {
        return LttbStateType.INSTANCE;
    }

    @Override
    public LttbState newState(RamAccounting ramAccounting,
            Version minNodeInCluster,
            MemoryManager memoryManager) {
        ramAccounting.addBytes(LttbState.SHALLOW_SIZE);
        return new LttbState();
    }

    @Override
    public LttbState iterate(RamAccounting ramAccounting, MemoryManager memoryManager, LttbState state,
            Input<?>... args) throws CircuitBreakingException {
        if (args.length != 3) {
            throw new IllegalArgumentException("lttb expects 3 arguments: timestamp, value, and threshold");
        }

        Long timestamp = (Long) args[0].value();
        Double value = (Double) args[1].value();
        Integer threshold = (Integer) args[2].value();

        if (threshold == null) {
            throw new IllegalArgumentException("lttb expects threshold not to be a null");
        }

        if (state.isInitialized() == false) {
            state.init(ramAccounting, threshold);
        }

        if (timestamp != null && value != null) {
            state.addPoint(ramAccounting, timestamp, value);
        }
        return state;
    }

    @Override
    public Map<String, List<? extends Number>> terminatePartial(RamAccounting ramAccounting, LttbState state) {
        List<Point> sampled = state.downsample();
        List<Long> sampledX = new ArrayList<>(sampled.size());
        List<Double> sampledY = new ArrayList<>(sampled.size());

        for (Point p : sampled) {
            sampledX.add(p.timestamp);
            sampledY.add(p.value);
        }
        return Map.of("x", sampledX, "y", sampledY);
    }

    public static class LttbState implements Comparable<LttbState>, Writeable {
        public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(LttbState.class);

        private List<Point> points;
        private boolean initialized;
        // The number of data points to be returned
        private int threshold;

        LttbState() {
        }

        LttbState(StreamInput in) throws IOException {
            boolean isInitialized = in.readBoolean();
            if (isInitialized) {
                this.threshold = in.readVInt();
                int size = in.readVInt();
                this.points = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    long timestamp = in.readLong();
                    double value = in.readDouble();
                    this.points.add(new Point(timestamp, value));
                }
            }
            this.initialized = isInitialized;

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(initialized);
            if (initialized) {
                out.writeVInt(threshold);
                out.writeVInt(points.size());
                for (Point point : points) {
                    out.writeLong(point.timestamp);
                    out.writeDouble(point.value);
                }
            }
        }

        public List<Point> getPoints() {
            return points;
        }

        public int getThreshold() {
            return threshold;
        }

        void init(RamAccounting ramAccounting, Integer threshold) {
            assert initialized == false : "LttbState was already initialized";
            points = new ArrayList<>();
            ramAccounting.addBytes(RamUsageEstimator.shallowSizeOfInstance(ArrayList.class));
            this.threshold = threshold;
            initialized = true;
        }

        boolean isInitialized() {
            return initialized;
        }

        public void addPoint(RamAccounting ramAccounting, long timestamp, double value) {
            assert initialized == true
                    : "LttbState has not yet been initialized. It needs to be initialized before adding a point";
            points.add(new Point(timestamp, value));
            ramAccounting.addBytes(Point.SHALLOW_SIZE);
        }

        public LttbState merge(RamAccounting ramAccounting, LttbState other) {
            if (other.threshold != this.threshold) {
                throw new IllegalStateException("The thresholds between the two LttbStates do not match");
            }
            for (Point p : other.points) {
                addPoint(ramAccounting, p.timestamp, p.value);
            }
            return this;
        }

        @Override
        public int compareTo(LttbState o) {
            // not necessary to compare one state to another in LTTB; we gather all data
            // points
            return 0;
        }

        public List<Point> downsample() {
            points.sort(Comparator.comparingLong(p -> p.timestamp));

            List<Point> sampled = new ArrayList<>();
            int dataLength = points.size();

            if (threshold >= dataLength || threshold == 0) {
                return points;
            }

            // -2 to account for start and end data points
            int bucketSize = (dataLength - 2) / (threshold - 2);

            // There are three buckets: a, b, and c
            //
            // "b" is the bucket where we search each point
            // to find the point that gives the largest area with points in buckets a and c
            //
            // "a" refers to the point in the previous bucket that was
            // sampled.
            //
            // "c" refers to the bucket after bucket "b"
            int a = 0;
            sampled.add(points.get(a));

            for (int cur = 0; cur < threshold - 2; cur++) {
                // find the average point in the next bucket
                long avgX = 0L; // NOTE: the current implementation expects a timestamp in the x axis
                double avgY = 0.0D;

                int cStart = (int) Math.floor((cur + 1) * bucketSize) + 1;
                int cEnd = (int) Math.floor((cur + 2) * bucketSize) + 1;
                cEnd = cEnd < dataLength ? cEnd : dataLength;

                int cSize = cEnd - cStart;

                for (int c = cStart; c < cEnd; c++) {
                    Point point = points.get(c);
                    avgX += point.timestamp;
                    avgY += point.value;
                }
                avgX /= cSize;
                avgY /= cSize;

                // search the current bucket "b" to find the point that gives the largest
                // triangle with the previous point in bucket "a" and the average point of
                // bucket "c"
                int bStart = (int) Math.floor((cur + 0) * bucketSize) + 1;
                int bEnd = (int) Math.floor((cur + 1) * bucketSize) + 1;

                long aX = points.get(a).timestamp;
                double aY = points.get(a).value;
                double maxArea = 0.0D;
                int maxAreaPoint = a;

                for (int b = bStart; b < bEnd; b++) {
                    Point point = points.get(b);
                    double area = Math.abs(
                            (aX - avgX) * (point.value - aY) - (aX - point.timestamp) * (avgY - aY)) * 0.5;
                    if (area > maxArea) {
                        maxArea = area;
                        maxAreaPoint = b;
                    }
                }
                sampled.add(points.get(maxAreaPoint));
                // this index will be the next bucket's "previous" value
                a = maxAreaPoint;

            }

            // add the last point of the dataset
            sampled.add(points.get(dataLength - 1));
            return sampled;
        }

    }

    public static class LttbStateType extends DataType<LTTBAggregation.LttbState>
            implements Streamer<LTTBAggregation.LttbState> {
        static final int ID = 18000;
        static final LttbStateType INSTANCE = new LttbStateType();

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
            return "lttb_state";
        }

        @Override
        public Streamer<LttbState> streamer() {
            return this;
        }

        @Override
        public LttbState sanitizeValue(Object value) {
            return (LTTBAggregation.LttbState) value;
        }

        @Override
        public int compare(LttbState val1, LttbState val2) {
            // not necessary to compare one state to another in LTTB; we gather all data
            // points
            return 0;
        }

        @Override
        public void writeValueTo(StreamOutput out, LttbState v) throws IOException {
            v.writeTo(out);
        }

        @Override
        public LttbState readValueFrom(StreamInput in) throws IOException {
            return new LttbState(in);
        }

        @Override
        public long valueBytes(LttbState value) {
            throw new UnsupportedOperationException("valueSize is not implemented for LttbStateType");
        }
    }

}
