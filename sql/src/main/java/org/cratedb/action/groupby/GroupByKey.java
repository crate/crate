package org.cratedb.action.groupby;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.Arrays;

public class GroupByKey implements Streamable, Comparable<GroupByKey> {

    private Object[] keyValue;

    public GroupByKey() {

    }

    public GroupByKey(Object[] keyValue) {
        this.keyValue = keyValue;
    }

    public Object get(int idx) {
        return keyValue[idx];
    }

    public int size() {
        return keyValue.length;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(keyValue);
    }

    @Override
    public int compareTo(GroupByKey other) {
        assert this.size() == other.size();

        Ordering<Comparable> ordering = Ordering.natural();
        ComparisonChain chain = ComparisonChain.start();

        for (int i = 0; i < this.size(); i++) {
            Object left = this.get(i);
            Object right = other.get(i);

            if (left == null) {
                return 1;
            }

            chain = chain.compare((Comparable)left, (Comparable)right, ordering);
        }

        return chain.result();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        keyValue = new Object[in.readVInt()];
        for (int i = 0; i < keyValue.length; i++) {
            keyValue[i] = in.readGenericValue() ;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(keyValue.length);
        for (Object o : keyValue) {
            out.writeGenericValue(o);
        }
    }

    public static GroupByKey readFromStreamInput(StreamInput in) throws IOException {
        GroupByKey key = new GroupByKey();
        key.readFrom(in);
        return key;
    }
}
