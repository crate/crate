package org.cratedb.action.groupby.aggregate;

import org.cratedb.action.groupby.aggregate.avg.AvgAggState;
import org.cratedb.action.groupby.aggregate.count.CountAggState;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class AggStateReader {

    public static AggState readFrom(StreamInput in) throws IOException {
        AggState result;
        switch (in.readByte()) {
            case 0:
                result = new CountAggState();
                result.readFrom(in);
                return result;
            case 1:
                result = new AvgAggState();
                result.readFrom(in);
                return result;
        }

        return null;
    }
}
