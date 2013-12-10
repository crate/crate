package org.cratedb.action.groupby;

import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.distinct.TypedSeenSerializer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RowSerializationContext {

    public final int numDistinctColumns;
    public final List<ParameterInfo> distinctColumns;
    public final TypedSeenSerializer[] typedSeenSerializers;
    public final List<AggExpr> aggregateExpressions;
    public final List<Integer> seenIdxMapping;

    public RowSerializationContext(List<AggExpr> aggregateExpressions) {
        this.aggregateExpressions = aggregateExpressions;
        this.seenIdxMapping = new ArrayList<>();
        this.distinctColumns = new ArrayList<>();

        int seenIdx;
        for (AggExpr aggExpr : aggregateExpressions) {
            if (aggExpr.isDistinct) {
                if (!distinctColumns.contains(aggExpr.parameterInfo)) {
                    distinctColumns.add(aggExpr.parameterInfo);
                }
                seenIdx = distinctColumns.indexOf(aggExpr.parameterInfo);
                seenIdxMapping.add(seenIdx);
            }
        }
        numDistinctColumns = distinctColumns.size();
        typedSeenSerializers = new TypedSeenSerializer[numDistinctColumns];
        generateSeenSerializer();
    }

    private void generateSeenSerializer() {
        for (int i = 0; i < numDistinctColumns; i++) {
            ParameterInfo parameterInfo = distinctColumns.get(i);

            switch (parameterInfo.dataType) {
                case TIMESTAMP:
                    // timestamp is a long
                case BYTE:
                    // byte is stored as long in lucene
                case SHORT:
                    // short is stored as long in lucene
                case INTEGER:
                    // integer is also stored as long in lucene
                case LONG:
                    typedSeenSerializers[i] = new TypedSeenSerializer<Long>() {
                        @Override
                        public Long readFrom(StreamInput in) throws IOException {
                            return in.readLong();
                        }

                        @Override
                        public void writeTo(StreamOutput out, Long value) throws IOException {
                            out.writeLong(value);
                        }
                    };
                    break;
                case FLOAT:
                    // float is stored as double
                case DOUBLE:
                    typedSeenSerializers[i] = new TypedSeenSerializer<Double>() {
                        @Override
                        public Double readFrom(StreamInput in) throws IOException {
                            return in.readDouble();
                        }

                        @Override
                        public void writeTo(StreamOutput out, Double value) throws IOException {
                            out.writeDouble(value);
                        }
                    };
                    break;
                case BOOLEAN:
                    // lucene stores boolean as "T" or "F"
                case IP:
                case STRING:
                    typedSeenSerializers[i] = new TypedSeenSerializer<String>() {
                        @Override
                        public String readFrom(StreamInput in) throws IOException {
                            return in.readString();
                        }

                        @Override
                        public void writeTo(StreamOutput out, String value) throws IOException {
                            out.writeString(value);
                        }
                    };
                    break;
                case CRATY:
                    typedSeenSerializers[i] = new TypedSeenSerializer<Object>() {
                        @Override
                        public Object readFrom(StreamInput in) throws IOException {
                            return in.readGenericValue();
                        }

                        @Override
                        public void writeTo(StreamOutput out, Object value) throws IOException {
                            out.writeGenericValue(value);
                        }
                    };
                    break;
            }
        }
    }
}
