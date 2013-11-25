package org.cratedb.action.groupby;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.Arrays;

public class GroupByKey implements Streamable, Comparable<GroupByKey> {

    public Object[] keyValue;
    Ordering<Comparable> ordering = Ordering.natural();

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
    public boolean equals(Object obj) {
        if (!(obj instanceof GroupByKey)) {
            return super.equals(obj);
        }

        return Arrays.equals(keyValue, ((GroupByKey) obj).keyValue);
    }

    @Override
    public int compareTo(GroupByKey other) {
        assert this.size() == other.size();
        ComparisonChain chain = ComparisonChain.start();

        for (int i = 0; i < this.size(); i++) {
            Object left = this.get(i);
            Object right = other.get(i);

            if (left != null && right != null) {
                chain = chain.compare((Comparable)left, (Comparable)right, ordering);
            } else if (right != null) {
                chain = chain.compare(0, 1);
            } else if (left != null) {
                chain = chain.compare(1, 0);
            } else {
                chain = chain.compare(0, 0);
            }
        }

        return chain.result();
    }

    @Override
    public String toString() {
        return "GroupByKey{" + Arrays.toString(keyValue) + "}";
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
