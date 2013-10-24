package org.cratedb.information_schema;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;

public class InformationSchemaStringColumn extends InformationSchemaColumn {

    public InformationSchemaStringColumn(String name) {
        super(name);
        type = SortField.Type.STRING;
    }

    @Override
    public Object getValue(IndexableField field) {
        return field.stringValue();
    }
}
