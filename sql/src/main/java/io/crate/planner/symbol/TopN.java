package io.crate.planner.symbol;

import com.google.common.base.Preconditions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class TopN extends Symbol {

    public static final SymbolFactory<TopN> FACTORY = new SymbolFactory<TopN>() {
        @Override
        public TopN newInstance() {
            return new TopN();
        }
    };

    public TopN() {

    }

    int[] orderBy;
    boolean[] reverseFlags;
    private int limit;
    private int offset;

    public TopN(int limit, int offset) {
        Preconditions.checkArgument(limit + offset < Integer.MAX_VALUE, "Range too big", limit, offset);
        this.limit = limit;
        this.offset = offset;
    }

    public TopN(int limit, int offset, int[] orderBy, boolean[] reverseFlags) {
        this(limit, offset);
        this.orderBy = orderBy;
        this.reverseFlags = reverseFlags;
    }

    public boolean isOrdered(){
        return orderBy != null && orderBy.length>0;
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
    public SymbolType symbolType() {
        return SymbolType.TOPN;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitTopN(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
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
