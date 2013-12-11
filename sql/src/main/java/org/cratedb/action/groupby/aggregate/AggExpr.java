package org.cratedb.action.groupby.aggregate;

import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.action.collect.Expression;
import org.cratedb.action.groupby.ParameterInfo;
import org.cratedb.action.groupby.aggregate.any.AnyAggState;
import org.cratedb.action.groupby.aggregate.avg.AvgAggState;
import org.cratedb.action.groupby.aggregate.count.CountAggState;
import org.cratedb.action.groupby.aggregate.max.MaxAggState;
import org.cratedb.action.groupby.aggregate.min.MinAggState;
import org.cratedb.action.groupby.aggregate.sum.SumAggState;
import org.cratedb.action.parser.ColumnDescription;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AggExpr extends ColumnDescription {

    public Expression expression;
    private AggStateCreator aggStateCreator;
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

    /**
     * this method is used to generate the aggStateCreator, this creator is then used in {@link #createAggState()}
     * to instantiate a concrete {@link AggState}
     *
     * this built aggStateCreator depends on the functionName and parameterInfo
     */
    private void createAggStateCreator() {
        switch (functionName) {
            case "AVG":
                createAvgAggState();
                break;
            case "COUNT":
            case "COUNT(*)":
            case "COUNT_DISTINCT":
                createCountAggState();
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
        if (expression.returnType() == DataType.STRING) {
            aggStateCreator = new AggStateCreator() {
                @Override
                AggState create() {
                    return new MinAggState<BytesRef>() {

                        @Override
                        public void readFrom(StreamInput in) throws IOException {
                            this.setValue(in.readBytesRef());
                        }

                        @Override
                        public void writeTo(StreamOutput out) throws IOException {
                            out.writeBytesRef((BytesRef) value());
                        }
                    };
                }
            };
        } else if (DataType.DECIMAL_TYPES.contains(expression.returnType())) {
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
                            Double value = (Double)value();
                            out.writeBoolean(value == null);
                            if (value != null) {
                                out.writeDouble(value);
                            }
                        }
                    };
                }
            };
        } else if (DataType.INTEGER_TYPES.contains(expression.returnType())
            || DataType.TIMESTAMP == expression.returnType())
        {
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
                            Long value = (Long)value();
                            out.writeBoolean(value == null);
                            if (value != null) {
                                out.writeLong(value);
                            }
                        }
                    };
                }
            };
        } else {
            throw new IllegalArgumentException("Illegal ParameterInfo for MIN");
        }
    }

    private void createMaxAggState() {
        if (expression.returnType() == DataType.STRING) {
            aggStateCreator = new AggStateCreator() {
                @Override
                AggState create() {
                    return new MaxAggState<BytesRef>() {

                        @Override
                        @SuppressWarnings("unchecked")
                        public void readFrom(StreamInput in) throws IOException {
                            setValue(in.readBytesRef());

                        }

                        @Override
                        public void writeTo(StreamOutput out) throws IOException {
                            out.writeBytesRef((BytesRef) value());
                        }
                    };
                }
            };
        } else if (DataType.DECIMAL_TYPES.contains(expression.returnType())) {
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
                            Double value = (Double)value();
                            out.writeBoolean(value == null);
                            if (value != null) {
                                out.writeDouble(value);
                            }
                        }
                    };
                }
            };
        } else if (DataType.INTEGER_TYPES.contains(expression.returnType())
            || DataType.TIMESTAMP == expression.returnType())
        {
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
                            Long value = (Long)value();
                            out.writeBoolean(value == null);
                            if (value != null) {
                                out.writeLong(value);
                            }
                        }
                    };
                }
            };
        } else {
            throw new IllegalArgumentException("Illegal ParameterInfo for MAX");
        }
    }

    private void createSumAggState() {
        aggStateCreator = new AggStateCreator() {
            @Override
            AggState create() {
                return new SumAggState();
            }
        };
    }

    private void createCountAggState() {
        aggStateCreator = new AggStateCreator() {
            @Override
            AggState create() {
                return new CountAggState();
            }
        };
    }

    private void createAvgAggState() {
        aggStateCreator = new AggStateCreator() {
            @Override
            AggState create() {
                return new AvgAggState();
            }
        };
    }

    /**
     * create AnyAggStates with concrete serialization methods
     *
     * These are defined here for performance reasons (no if checks and such)
     */
    private void createAnyAggState() {
        if (expression.returnType() == DataType.STRING || expression.returnType() == DataType.IP) {
            aggStateCreator = new AggStateCreator() {
                @Override
                AggState create() {
                    return new AnyAggState<BytesRef>() {

                        @Override
                        public void add(Object otherValue) {
                            this.value = (BytesRef)otherValue;
                        }

                        @Override
                        public void readFrom(StreamInput in) throws IOException {
                            this.value = in.readBytesRef();
                        }

                        @Override
                        public void writeTo(StreamOutput out) throws IOException {
                            out.writeBytesRef(this.value);
                        }
                    };
                }
            };
        } else if (DataType.INTEGER_TYPES.contains(expression.returnType()) ||
                DataType.TIMESTAMP == expression.returnType()) {
            aggStateCreator = new AggStateCreator() {
                @Override
                AggState create() {
                    return new AnyAggState<Long>() {

                        @Override
                        public void add(Object otherValue) {
                            this.value = (Long)otherValue;
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
        } else if (DataType.DECIMAL_TYPES.contains(expression.returnType())) {
            aggStateCreator = new AggStateCreator() {
                @Override
                AggState create() {
                    return new AnyAggState<Double>() {

                        @Override
                        public void add(Object otherValue) {
                            this.value = (Double)otherValue;
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
        } else if (expression.returnType() == DataType.BOOLEAN) {
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
                                this.value = (Boolean)otherValue;
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
        } else {
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
        return String.format("%s(%s%s)",
                functionName.split("_")[0],
                (isDistinct ? "DISTINCT " : ""),
                (expression!=null ? expression.toString():""));
    }

}
