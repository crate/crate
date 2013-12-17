package org.cratedb.lucene.fields;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.MultiTermQueryWrapperFilter;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.index.ColumnDefinition;
import org.elasticsearch.common.lucene.BytesRefs;

import java.io.IOException;

public class StringLuceneField extends LuceneField<BytesRef> {

    public StringLuceneField(String name) {
        this(name, false);
    }

    public StringLuceneField(String name, boolean allowMultipleValues) {
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
        return new ColumnDefinition(tableName, name, DataType.STRING, null, ordinalPosition, false, true);
    }

    @Override
    public Object mappedValue(Object value) {
        return (BytesRef)value;
    }

    @Override
    public StringField field(BytesRef value) {
        // TODO: avoid convertion to utf8 string
        return new StringField(name, value.utf8ToString(), Field.Store.YES);
    }

    @Override
    public TokenStream tokenStream(BytesRef value) throws IOException {
        return field(value).tokenStream(analyzer);
    }
}
