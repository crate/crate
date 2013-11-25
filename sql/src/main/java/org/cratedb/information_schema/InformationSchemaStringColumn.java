package org.cratedb.information_schema;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.MultiTermQueryWrapperFilter;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.TermRangeQuery;
import org.cratedb.index.ColumnDefinition;
import org.elasticsearch.common.lucene.BytesRefs;

public class InformationSchemaStringColumn extends InformationSchemaColumn {

    public InformationSchemaStringColumn(String name) {
        this(name, false);
    }

    public InformationSchemaStringColumn(String name, boolean allowMultipleValues) {
        super(name, allowMultipleValues);
        type = SortField.Type.STRING;
    }

    @Override
    public Object getValue(IndexableField field) {
        return field.stringValue();
    }

    @Override
    public TermRangeQuery rangeQuery(Object from, Object to,
                                     boolean includeLower, boolean includeUpper) {
        return new TermRangeQuery(name, BytesRefs.toBytesRef(from), BytesRefs.toBytesRef(to),
            includeLower, includeUpper);
    }

    @Override
    public MultiTermQueryWrapperFilter rangeFilter(Object from, Object to,
                                                   boolean includeLower, boolean includeUpper) {
        return new TermRangeFilter(name, BytesRefs.toBytesRef(from), BytesRefs.toBytesRef(to),
            includeLower, includeUpper);
    }

    @Override
    public ColumnDefinition getColumnDefinition(String tableName, int ordinalPosition) {
        return new ColumnDefinition(tableName, name, "string", null, ordinalPosition, false, true);
    }
}
