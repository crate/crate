package org.cratedb.action.groupby;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

/**
 * used to specify the parameters of the aggregateExpressions.
 *
 *  e.g. count(*) ->  ParameterInfo with isAllColumn true
 *
 *  intended to be extended to be used for example for avg(columnName)
 */
public class ParameterInfo implements Streamable {

    public boolean isAllColumn;
    public String columnName;

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ParameterInfo)) return false;

        ParameterInfo that = (ParameterInfo) o;

        if (isAllColumn != that.isAllColumn) return false;

        if (columnName != null && !columnName.equals(that.columnName)) return false;

        if (that.columnName != null && !that.columnName.equals(columnName)) return false;

        return true;
    }

    public int hashCode() {
        return (isAllColumn ? 1 : columnName.hashCode());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        isAllColumn = in.readBoolean();
        columnName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isAllColumn);
        out.writeString(columnName);
    }

}
