/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.action.groupby.aggregate;

import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.action.collect.Expression;
import org.cratedb.action.groupby.aggregate.any.AnyAggState;
import org.cratedb.action.groupby.aggregate.avg.AvgAggState;
import org.cratedb.action.groupby.aggregate.count.CountAggState;
import org.cratedb.action.groupby.aggregate.count.CountDistinctAggState;
import org.cratedb.action.groupby.aggregate.max.MaxAggState;
import org.cratedb.action.groupby.aggregate.min.MinAggState;
import org.cratedb.action.groupby.aggregate.sum.SumAggState;
import org.cratedb.action.parser.ColumnDescription;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AggExpr extends ColumnDescription {

    private AggStateCreator aggStateCreator;
    private DataType returnType;

    public Expression expression;
    public boolean isDistinct;
    public String functionName;

    public AggExpr() {
        super(Types.AGGREGATE_COLUMN);
    }

    public AggExpr(String functionName, boolean isDistinct, Expression expression) {
        super(Types.AGGREGATE_COLUMN);
        this.functionName = functionName;
        this.isDistinct = isDistinct;
        this.expression = expression;
        createAggStateCreator();
    }

    @Override
    public DataType returnType() {
        return this.returnType;
    }

    /**
     * this method is used to generate the aggStateCreator, this creator is then used in {@link #createAggState()}
     * to instantiate a concrete {@link AggState}
     * <p/>
     * this built aggStateCreator depends on the functionName and parameterInfo
     */
    private void createAggStateCreator() {
        switch (functionName) {
            case "AVG":
                createAvgAggState();
                break;
            case "COUNT":
            case "COUNT(*)":
                createCountAggState();
                break;
            case "COUNT_DISTINCT":
                createCountDistinctAggState();
                break;
            case "MAX":
                createMaxAggState();
                break;
            case "MIN":
                createMinAggState();
                break;
            case "SUM":
                createSumAggState();
                break;
            case "ANY":
                createAnyAggState();
                break;
            default:
                throw new IllegalArgumentException("Unknown function " + functionName);

        }
    }

    private void createMinAggState() {
        returnType = expression.returnType();
        switch (expression.returnType()) {
            case STRING:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new MinAggState<BytesRef>() {

                            @Override
                            @SuppressWarnings("unchecked")
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    setValue(in.readBytesRef());
                                } else {
                                    setValue(null);
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                if (value() == null) {
                                    out.writeBoolean(true);
                                } else {
                                    out.writeBoolean(false);
                                    out.writeBytesRef((BytesRef) value());
                                }
                            }
                        };
                    }
                };
                break;
            case DOUBLE:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new MinAggState<Double>() {

                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    setValue(in.readDouble());
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                Double value = (Double) value();
                                out.writeBoolean(value == null);
                                if (value != null) {
                                    out.writeDouble(value);
                                }
                            }
                        };
                    }
                };
                break;
            case FLOAT:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new MinAggState<Float>() {

                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    setValue(in.readFloat());
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                Float value = (Float) value();
                                out.writeBoolean(value == null);
                                if (value != null) {
                                    out.writeFloat(value);
                                }
                            }
                        };
                    }
                };
                break;
            case LONG:
            case TIMESTAMP:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new MinAggState<Long>() {
                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    setValue(in.readLong());
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                Long value = (Long) value();
                                out.writeBoolean(value == null);
                                if (value != null) {
                                    out.writeLong(value);
                                }
                            }
                        };
                    }
                };
                break;
            case SHORT:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new MinAggState<Short>() {
                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    setValue(in.readShort());
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                Short value = (Short) value();
                                out.writeBoolean(value == null);
                                if (value != null) {
                                    out.writeShort(value);
                                }
                            }
                        };
                    }
                };
                break;
            case INTEGER:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new MinAggState<Integer>() {
                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    setValue(in.readInt());
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                Integer value = (Integer) value();
                                out.writeBoolean(value == null);
                                if (value != null) {
                                    out.writeInt(value);
                                }
                            }
                        };
                    }
                };
                break;
            default:
                throw new IllegalArgumentException("Illegal ParameterInfo for MAX");
        }
    }

    private void createMaxAggState() {
        returnType = expression.returnType();
        switch (expression.returnType()) {
            case STRING:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new MaxAggState<BytesRef>() {
                            @Override
                            @SuppressWarnings("unchecked")
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    setValue(in.readBytesRef());
                                } else {
                                    setValue(null);
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                if (value() == null) {
                                    out.writeBoolean(true);
                                } else {
                                    out.writeBoolean(false);
                                    out.writeBytesRef((BytesRef) value());
                                }
                            }
                        };
                    }
                };
                break;
            case DOUBLE:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new MaxAggState<Double>() {

                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    setValue(in.readDouble());
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                Double value = (Double) value();
                                out.writeBoolean(value == null);
                                if (value != null) {
                                    out.writeDouble(value);
                                }
                            }
                        };
                    }
                };
                break;
            case FLOAT:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new MaxAggState<Float>() {

                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    setValue(in.readFloat());
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                Float value = (Float) value();
                                out.writeBoolean(value == null);
                                if (value != null) {
                                    out.writeFloat(value);
                                }
                            }
                        };
                    }
                };
                break;
            case LONG:
            case TIMESTAMP:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new MaxAggState<Long>() {
                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    setValue(in.readLong());
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                Long value = (Long) value();
                                out.writeBoolean(value == null);
                                if (value != null) {
                                    out.writeLong(value);
                                }
                            }
                        };
                    }
                };
                break;
            case SHORT:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new MaxAggState<Short>() {
                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    setValue(in.readShort());
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                Short value = (Short) value();
                                out.writeBoolean(value == null);
                                if (value != null) {
                                    out.writeShort(value);
                                }
                            }
                        };
                    }
                };
                break;
            case INTEGER:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new MaxAggState<Integer>() {
                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    setValue(in.readInt());
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                Integer value = (Integer) value();
                                out.writeBoolean(value == null);
                                if (value != null) {
                                    out.writeInt(value);
                                }
                            }
                        };
                    }
                };
                break;
            default:
                throw new IllegalArgumentException("Illegal ParameterInfo for MAX");
        }
    }

    private void createSumAggState() {
        returnType = DataType.DOUBLE;
        aggStateCreator = new AggStateCreator() {
            @Override
            AggState create() {
                return new SumAggState();
            }
        };
    }

    private void createCountAggState() {
        returnType = DataType.LONG;
        aggStateCreator = new AggStateCreator() {
            @Override
            AggState create() {
                return new CountAggState();
            }
        };
    }

    private void createCountDistinctAggState() {
        returnType = DataType.LONG;
        aggStateCreator = new AggStateCreator() {
            @Override
            AggState create() {
                return new CountDistinctAggState();
            }
        };
    }

    private void createAvgAggState() {
        returnType = DataType.DOUBLE;
        aggStateCreator = new AggStateCreator() {
            @Override
            AggState create() {
                return new AvgAggState();
            }
        };
    }

    /**
     * create AnyAggStates with concrete serialization methods
     * <p/>
     * These are defined here for performance reasons (no if checks and such)
     */
    private void createAnyAggState() {
        returnType = expression.returnType();
        switch (expression.returnType()) {
            case STRING:
            case IP:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new AnyAggState<BytesRef>() {

                            @Override
                            public void add(Object otherValue) {
                                this.value = (BytesRef) otherValue;
                            }

                            @Override
                            @SuppressWarnings("unchecked")
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    this.value = in.readBytesRef();
                                } else {
                                    this.value = null;
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                if (value() == null) {
                                    out.writeBoolean(true);
                                } else {
                                    out.writeBoolean(false);
                                    out.writeBytesRef((BytesRef) value());
                                }
                            }
                        };
                    }
                };
                break;
            case LONG:
            case TIMESTAMP:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new AnyAggState<Long>() {

                            @Override
                            public void add(Object otherValue) {
                                this.value = (Long) otherValue;
                            }

                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    this.value = in.readLong();
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                out.writeBoolean(this.value == null);
                                if (this.value != null) {
                                    out.writeLong(this.value);
                                }
                            }
                        };
                    }
                };
                break;
            case INTEGER:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new AnyAggState<Integer>() {

                            @Override
                            public void add(Object otherValue) {
                                this.value = (Integer) otherValue;
                            }

                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    this.value = in.readInt();
                                }

                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                out.writeBoolean(this.value == null);
                                if (this.value != null) {
                                    out.writeInt(this.value);
                                }
                            }
                        };
                    }
                };
                break;
            case SHORT:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new AnyAggState<Short>() {

                            @Override
                            public void add(Object otherValue) {
                                this.value = (Short) otherValue;
                            }

                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    this.value = in.readShort();
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                out.writeBoolean(this.value == null);
                                if (this.value != null) {
                                    out.writeShort(this.value);
                                }
                            }
                        };
                    }
                };
                break;
            case FLOAT:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new AnyAggState<Float>() {

                            @Override
                            public void add(Object otherValue) {
                                this.value = (Float) otherValue;
                            }

                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    this.value = in.readFloat();
                                }

                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                out.writeBoolean(this.value == null);
                                if (this.value != null) {
                                    out.writeFloat(this.value);
                                }
                            }
                        };
                    }
                };
                break;
            case DOUBLE:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new AnyAggState<Double>() {

                            @Override
                            public void add(Object otherValue) {
                                this.value = (Double) otherValue;
                            }

                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    this.value = in.readDouble();
                                }

                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                out.writeBoolean(this.value == null);
                                if (this.value != null) {
                                    out.writeDouble(this.value);
                                }
                            }
                        };
                    }
                };
                break;
            case BOOLEAN:
                aggStateCreator = new AggStateCreator() {
                    @Override
                    AggState create() {
                        return new AnyAggState<Boolean>() {

                            @Override
                            public void add(Object otherValue) {
                                if (otherValue instanceof String) {
                                    // this is how lucene stores the truth
                                    this.value = ((String) otherValue).charAt(0) == 'T';
                                } else {
                                    this.value = (Boolean) otherValue;
                                }
                            }

                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                this.value = in.readOptionalBoolean();
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                out.writeOptionalBoolean(this.value);
                            }
                        };
                    }
                };
                break;
            default:
                throw new IllegalArgumentException("Illegal ParameterInfo for ANY");
        }
    }

    /**
     * internally used to create the {@link #createAggState()} method.
     */
    abstract class AggStateCreator {
        abstract AggState create();
    }

    /**
     * can be used to create a concrete AggState that will fit to the functionName
     */
    public AggState createAggState() {
        return aggStateCreator.create();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AggExpr)) return false;

        AggExpr aggExpr = (AggExpr) o;

        if (!functionName.equals(aggExpr.functionName)) return false;
        if (!expression.equals(aggExpr.expression)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = functionName.hashCode();
        result = 31 * result + expression.hashCode();
        return result;
    }

    @Override
    public String toString() {
        if (expression == null) {
            return functionName;
        }
        return String.format("%s(%s%s)",
            functionName.split("_")[0], // COUNT_DISTINCT -> COUNT
            (isDistinct ? "DISTINCT " : ""),
            expression.toString()
        );
    }

    @Override
    public String name() {
        return this.toString();
    }
}
