package io.crate.planner.symbol;

import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Set;

/**
 * Created by bd on 11.2.14.
 */
public class SetLiteral<ValueType, LiteralType> extends Literal<Set<ValueType>, Set<LiteralType>>{


    @Override
    public int compareTo(Set<LiteralType> o) {
        return 0;
    }

    @Override
    public Set<ValueType> value() {
        return null;
    }

    @Override
    public DataType valueType() {
        return null;
    }

    @Override
    public SymbolType symbolType() {
        return null;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }
}
