package io.crate.planner.symbol;

import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.RowGranularity;
import org.cratedb.DataType;

public class Reference implements ValueSymbol {

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
}
