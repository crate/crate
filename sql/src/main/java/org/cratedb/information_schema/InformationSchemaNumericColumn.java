package org.cratedb.information_schema;

import org.apache.lucene.index.IndexableField;

public abstract class InformationSchemaNumericColumn extends InformationSchemaColumn {

    public InformationSchemaNumericColumn(String name) {
        super(name);
    }

    @Override
    public Object getValue(IndexableField field) {
        return field.numericValue();
    }
}
