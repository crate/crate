package io.crate.planner.symbol;

import io.crate.planner.RowGranularity;
import org.cratedb.DataType;

public class Reference implements ValueSymbol {

    public static final SymbolFactory<Reference> FACTORY = new SymbolFactory<Reference>() {
        @Override
        public Reference newInstance() {
            return new Reference();
        }
    };

    private String schema;
    private String table;
    private String column;
    private String[] path;

    private RowGranularity granularity;
    private DataType type;


    public Reference(DataType type, RowGranularity granularity,
                     String schema, String table, String column, String... path) {
        this.type = type;
        this.granularity = granularity;
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.path = path;
    }

    public Reference() {
    }

    public String schema() {
        return schema;
    }

    public String table() {
        return table;
    }

    public String column() {
        return column;
    }

    public String[] path() {
        return path;
    }

    public RowGranularity granularity() {
        return granularity;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.REFERENCE;
    }

    @Override
    public DataType valueType() {
        return type;
    }
}
