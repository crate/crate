package org.cratedb.action.groupby;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class ParameterInfo implements Streamable {

    public boolean isAllColumn;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ParameterInfo)) return false;

        ParameterInfo that = (ParameterInfo) o;

        if (isAllColumn != that.isAllColumn) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (isAllColumn ? 1 : 0);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        isAllColumn = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isAllColumn);
    }
}
