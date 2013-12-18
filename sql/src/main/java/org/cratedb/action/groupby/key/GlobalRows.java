package org.cratedb.action.groupby.key;

import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GlobalRows extends Rows<GlobalRows> {

    private static final GroupByKey GLOBAL_KEY = new GroupByKey(new Boolean[]{true});
    private final ParsedStatement stmt;
    private final GroupByRow[] buckets;
    private int currentBucket;
    private GroupByRow mergedRow = null;

    public GlobalRows(int numBuckets, ParsedStatement stmt) {
        assert numBuckets > 0: "requires at least one bucket";

        this.stmt = stmt;
        this.buckets = new GroupByRow[numBuckets];
        this.currentBucket = 0;
    }

    private void nextBucket() {
        if (currentBucket < buckets.length - 1) {
            currentBucket++;
        } else {
            currentBucket = 0;
        }
    }

    @Override
    public GroupByRow getRow() {
        if (buckets[currentBucket] == null) {
            buckets[currentBucket] = GroupByRow.createEmptyRow(GLOBAL_KEY, stmt);
        }
        GroupByRow row = buckets[currentBucket];
        nextBucket();
        return row;
    }

    @Override
    public void writeBucket(StreamOutput out, int idx) throws IOException {
        GroupByRow row = buckets[idx];
        if (row == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        row.writeStates(out, stmt);
    }

    @Override
    public void readBucket(StreamInput in, int idx) throws IOException {
        if (!in.readBoolean()) {
            return;
        }
        buckets[idx] = new GroupByRow();
        buckets[idx].readFrom(in, GLOBAL_KEY, stmt);
    }

    public List<GroupByRow> buckets() {
        List<GroupByRow> result = new ArrayList<>(buckets.length);
        for (GroupByRow bucket : buckets) {
            if (bucket != null) {
                result.add(bucket);
            }
        }

        return result;
    }

    @Override
    public synchronized void merge(GlobalRows other) {
        // put all buckets of other in this buckets regardless how many buckets are in other
        assert other.buckets().size() <= 1;

        if (mergedRow == null) {
            walk(new RowVisitor() {
                @Override
                public void visit(GroupByRow row) {
                    mergedRow = row;
                }
            });
        }

        other.walk(new RowVisitor() {
            @Override
            public void visit(GroupByRow row) {
                if (mergedRow == null) {
                    mergedRow = row;
                } else {
                    mergedRow.merge(row);
                }
            }
        });
    }

    @Override
    public void walk(RowVisitor visitor) {
        if (mergedRow != null) {
            visitor.visit(mergedRow);
            return;
        }

        for (GroupByRow row : buckets()) {
            visitor.visit(row);
        }
    }
}
