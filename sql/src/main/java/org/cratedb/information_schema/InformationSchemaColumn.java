package org.cratedb.information_schema;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;

public abstract class InformationSchemaColumn {

    public final String name;
    public SortField.Type type;

    public InformationSchemaColumn(String name) {
        this.name = name;
    }

    public abstract Object getValue(IndexableField field);
}
