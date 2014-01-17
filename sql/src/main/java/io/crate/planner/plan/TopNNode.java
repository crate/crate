package io.crate.planner.plan;

import com.google.common.base.Preconditions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class TopNNode extends PlanNode {


    int[] orderBy;
    boolean[] reverseFlags;
    private int limit;
    private int offset;

    public TopNNode(String id) {
        super(id);
    }

    public TopNNode(String id, int limit, int offset) {
        this(id);
        Preconditions.checkArgument(limit + offset < Integer.MAX_VALUE, "Range too big", limit, offset);
        this.limit = limit;
        this.offset = offset;
    }

    public TopNNode(String id, int limit, int offset, int[] orderBy, boolean[] reverseFlags) {
        this(id, limit, offset);
        this.orderBy = orderBy;
        this.reverseFlags = reverseFlags;
    }

    public boolean isOrdered() {
        return orderBy != null && orderBy.length > 0;
    }

    public int limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    public int[] orderBy() {
        return orderBy;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitTopNNode(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        offset = in.readVInt();
        limit = in.readVInt();

        orderBy = new int[in.readVInt()];
        for (int i = 0; i < orderBy.length; i++) {
            orderBy[i] = in.readVInt();
        }

        reverseFlags = new boolean[in.readVInt()];
        for (int i = 0; i < reverseFlags.length; i++) {
            reverseFlags[i] = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(offset);
        out.writeVInt(limit);
        out.writeVInt(orderBy.length);
        for (int i : orderBy) {
            out.writeVInt(i);
        }

        out.writeVInt(reverseFlags.length);
        for (boolean reverseFlag : reverseFlags) {
            out.writeBoolean(reverseFlag);
        }
    }

}
