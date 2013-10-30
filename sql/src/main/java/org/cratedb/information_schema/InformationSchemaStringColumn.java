package org.cratedb.information_schema;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.MultiTermQueryWrapperFilter;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.TermRangeQuery;
import org.elasticsearch.common.lucene.BytesRefs;

public class InformationSchemaStringColumn extends InformationSchemaColumn {

    public InformationSchemaStringColumn(String name) {
        super(name);
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
}
