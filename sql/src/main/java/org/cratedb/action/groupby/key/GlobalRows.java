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
    private final List<GroupByRow>[] buckets;
    private int currentBucket;

    public GlobalRows(int numBuckets, ParsedStatement stmt) {
        this.stmt = stmt;
        this.buckets = new ArrayList[numBuckets];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new ArrayList<>();
        }
        this.currentBucket=0;
    }

    private void nextBucket(){
        if (currentBucket<buckets.length-1){
            currentBucket++;
        } else {
            currentBucket = 0;
        }
    }

    @Override
    public GroupByRow getRow() {
        GroupByRow row =  GroupByRow.createEmptyRow(GLOBAL_KEY, stmt);
        buckets[currentBucket].add(row);
        nextBucket();
        return row;
    }

    @Override
    public void writeBucket(StreamOutput out, int idx) throws IOException {
        // TODO: special serializer
        out.writeGenericValue(buckets[idx]);
    }

    @Override
    public void readBucket(StreamInput in, int idx) throws IOException {
        buckets[idx] = (List<GroupByRow>) in.readGenericValue();
    }

    public List<GroupByRow>[] buckets() {
        return buckets;
    }

    @Override
    public void merge(GlobalRows other) {
        // put all buckets of other in this buckets regardless how many buckets are in other
        for(List<GroupByRow> l: other.buckets()){
            buckets[currentBucket].addAll(l);
            nextBucket();
        }
    }

    @Override
    public void walk(RowVisitor visitor) {
        for (List<GroupByRow> l: buckets){
            for (GroupByRow row: l){
                visitor.visit(row);
            }
        }
    }

}
