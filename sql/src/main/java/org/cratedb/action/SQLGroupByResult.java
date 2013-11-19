package org.cratedb.action;

import org.cratedb.action.groupby.GroupByRow;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.*;


/**
 * Result of a group by operation.
 * Each key represents a row for the SQLResponse.
 *
 * See {@link TransportDistributedSQLAction} for an overview of the workflow how the SQLGroupByResult is used.
 */
public class SQLGroupByResult implements Streamable {

    /**
     * optimization: the preSerializationResult is set on the mapper
     * after the SQLGroupByResult is sent from the Mapper to the Reducer
     * the result is filled
     *
     * the serialization is basically abused to convert the Collection into a List
     */
    public List<GroupByRow> result;
    private Collection<GroupByRow> preSerializationResult;

    public SQLGroupByResult() {
        // empty ctor - serialization
        result = new ArrayList<>(0);
    }

    public SQLGroupByResult(Collection<GroupByRow> result) {
        this.preSerializationResult = result;
    }

    /**
     * Only use this for testing, the result is not serialized!
     * @param result
     */
    public SQLGroupByResult(List<GroupByRow> result) {
       this.result = result;
    }

    public void merge(SQLGroupByResult otherResult) {
        merge(otherResult.result);
    }

    protected void merge(List<GroupByRow> mapperResult) {
        assert result != null;
        assert mapperResult != null;

        if (result.isEmpty()) {
            result = mapperResult;
            return;
        }
        if (mapperResult.isEmpty()) {
            return;
        }

        List<GroupByRow> newResult = new ArrayList<>();

        ListIterator<GroupByRow> thisIterator = result.listIterator();
        ListIterator<GroupByRow> otherIterator = mapperResult.listIterator();

        GroupByRow otherRow;
        GroupByRow thisRow;

        while(thisIterator.hasNext() || otherIterator.hasNext()) {
            if (!otherIterator.hasNext()) {
                newResult.add(thisIterator.next());
            } else if (!thisIterator.hasNext()) {
                newResult.add(otherIterator.next());
            } else {

                thisRow = result.get(thisIterator.nextIndex());
                otherRow = mapperResult.get(otherIterator.nextIndex());

                switch (thisRow.key.compareTo(otherRow.key)) {
                    case 0:
                        thisRow.merge(otherRow);
                        newResult.add(thisRow);
                        thisIterator.next();
                        otherIterator.next();
                        break;
                    case -1:
                        newResult.add(thisRow);
                        thisIterator.next();
                        break;
                    case 1:
                        newResult.add(otherRow);
                        otherIterator.next();
                        break;
                }
            }
        }

        result = newResult;
    }

    public int size() {
        if (preSerializationResult != null) {
            return preSerializationResult.size();
        }
        return result.size();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int resultSize = in.readVInt();
        result = new ArrayList<>();

        for (int i = 0; i < resultSize; i++) {
            result.add(GroupByRow.readGroupByRow(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(preSerializationResult.size());
        for (GroupByRow groupByRow : preSerializationResult) {
            groupByRow.writeTo(out);
        }
    }

    public static SQLGroupByResult readSQLGroupByResult(StreamInput in) throws IOException {
        SQLGroupByResult result = new SQLGroupByResult();
        result.readFrom(in);
        return result;
    }
}
