package io.crate.planner.symbol;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

public class TopN implements Symbol {

    public static final SymbolFactory<TopN> FACTORY = new SymbolFactory<TopN>() {
        @Override
        public TopN newInstance() {
            return new TopN();
        }
    };

    public TopN() {

    }

    private int limit;
    private int offset;

    public TopN(int limit, int offset) {
        Preconditions.checkArgument(limit + offset < Integer.MAX_VALUE, "Range too big", limit, offset);
        this.limit = limit;
        this.offset = offset;
    }

    public int limit() {
        return limit;
    }

    public int offset() {
        return offset;
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
