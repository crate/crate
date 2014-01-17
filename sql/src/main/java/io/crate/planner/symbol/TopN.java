package io.crate.planner.symbol;

import com.google.common.base.Preconditions;

public class TopN implements Symbol {

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

}
