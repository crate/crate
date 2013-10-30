package org.cratedb.information_schema;

import org.apache.lucene.search.SortField;

public class InformationSchemaIntegerColumn extends InformationSchemaNumericColumn {

    public InformationSchemaIntegerColumn(String name) {
        super(name);
        type = SortField.Type.INT;
    }
}
