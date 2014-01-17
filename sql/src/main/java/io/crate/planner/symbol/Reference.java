package io.crate.planner.symbol;

import io.crate.metadata.ReferenceInfo;
import io.crate.planner.plan.Routing;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class Reference extends ValueSymbol {

    public static final SymbolFactory<Reference> FACTORY = new SymbolFactory<Reference>() {
        @Override
        public Reference newInstance() {
            return new Reference();
        }
    };

    private ReferenceInfo info;

    public Reference(ReferenceInfo info) {
        this.info = info;
    }

    public Reference() {

    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.REFERENCE;
    }

    @Override
    public DataType valueType() {
        return info.type();
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitReference(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        info = new ReferenceInfo();
        info.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        info.writeTo(out);
    }
}
