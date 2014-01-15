package io.crate.planner.symbol;

import io.crate.metadata.ReferenceInfo;
import org.cratedb.DataType;

public class Reference implements ValueSymbol {

    public static final SymbolFactory<Reference> FACTORY = new SymbolFactory<Reference>() {
        @Override
        public Reference newInstance() {
            return new Reference();
        }
    };

    private Routing routing;
    private ReferenceInfo info;

    public Reference(ReferenceInfo info, Routing routing) {
        this.info = info;
        this.routing = routing;
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


}
