package org.cratedb.action;

import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Maps.newHashMap;

/**
 * Result of a group by operation.
 * Each key represents a row for the SQLResponse.
 *
 * See {@link TransportDistributedSQLAction} for an overview of the workflow how the SQLGroupByResult is used.
 */
public class SQLGroupByResult implements Streamable {

    public List<GroupByRow> result;

    public SQLGroupByResult() {
        // empty ctor - serialization
    }

    public SQLGroupByResult(Collection<GroupByRow> result) {
        this.result = new ArrayList<>(result);
    }

    public void merge(SQLGroupByResult otherResult) {
        merge(otherResult.result);
    }

    /**
     * merge the content of "mapperResult" into "result"
     *
     * a an entry (identified by key) in mapperResult that is missing in result is added to result
     * if the entry is in result the values are merged.
     * @param mapperResult
     */
    protected void merge(Collection<GroupByRow> mapperResult) {
        if (result == null || result.isEmpty()) {
            result = new ArrayList<>(mapperResult);
            return;
        }
        if (mapperResult == null || mapperResult.isEmpty()) {
            return;
        }

        Iterator<GroupByRow> thisIterator = result.iterator();
        Iterator<GroupByRow> otherIterator = mapperResult.iterator();

        GroupByRow otherRow = otherIterator.next();
        GroupByRow thisRow = thisIterator.next();
        boolean exit = false;
        while (!exit) {
            switch (thisRow.key.compareTo(otherRow.key)) {
                case 0:
                    thisRow.merge(otherRow);
                    if (otherIterator.hasNext()) {
                        otherRow = otherIterator.next();
                    } else {
                        exit = true;
                    }
                    break;
                case -1:
                case 1:
                    if (thisIterator.hasNext()) {
                        thisRow = thisIterator.next();
                    } else {
                        result.add(otherRow);
                        exit = true;
                    }
                    break;
            }
        }

        while (otherIterator.hasNext()) {
            otherRow =  otherIterator.next();
            result.add(otherRow);
        }
    }

    public int size() {
        return result.size();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int resultSize = in.readVInt();
        result = new ArrayList<>(resultSize);

        for (int i = 0; i < resultSize; i++) {
            result.add(GroupByRow.readGroupByRow(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(result.size());
        for (GroupByRow groupByRow : result) {
            groupByRow.writeTo(out);
        }
    }

    public static SQLGroupByResult readSQLGroupByResult(StreamInput in) throws IOException {
        SQLGroupByResult result = new SQLGroupByResult();
        result.readFrom(in);
        return result;
    }
}
