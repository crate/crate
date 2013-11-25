package org.cratedb.action.groupby.aggregate;

import org.cratedb.action.groupby.ParameterInfo;
import org.cratedb.action.parser.ColumnDescription;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AggExpr extends ColumnDescription {

    public String functionName;
    public ParameterInfo parameterInfo;

    public AggExpr(String functionName, ParameterInfo parameterInfo) {
        super(Types.AGGREGATE_COLUMN);
        this.functionName = functionName;
        this.parameterInfo = parameterInfo;
    }

    public AggExpr() {
        super(Types.AGGREGATE_COLUMN);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AggExpr)) return false;

        AggExpr aggExpr = (AggExpr) o;

        if (!functionName.equals(aggExpr.functionName)) return false;
        if (!parameterInfo.equals(aggExpr.parameterInfo)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = functionName.hashCode();
        result = 31 * result + parameterInfo.hashCode();
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        functionName = in.readString();
        parameterInfo = new ParameterInfo();
        parameterInfo.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(functionName);
        parameterInfo.writeTo(out);
    }

    public static AggExpr readFromStream(StreamInput in) throws IOException {
        AggExpr expr = new AggExpr();
        expr.readFrom(in);
        return expr;
    }

    @Override
    public String toString() {
        if (parameterInfo != null) {
            return String.format("%s(%s)", functionName,
                    (parameterInfo.isAllColumn ? "*" : parameterInfo.columnName));

        } else {
            return functionName;
        }
    }
}
