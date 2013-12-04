package org.cratedb.lucene.fields;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.MultiTermQueryWrapperFilter;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.TermRangeQuery;
import org.cratedb.DataType;
import org.cratedb.index.ColumnDefinition;
import org.elasticsearch.common.lucene.BytesRefs;

import java.io.IOException;

public class StringLuceneField extends LuceneField<String> {

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
        return (String)value;
    }

    public StringField field(String value) {
        value = value == null ? "NULL" : value;
        return new StringField(name, value, Field.Store.YES);
    }

    public TokenStream tokenStream(String value) throws IOException {
        return field(value).tokenStream(analyzer);
    }

}
