package org.cratedb.lucene.fields;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.search.MultiTermQueryWrapperFilter;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.SortField;
import org.cratedb.DataType;
import org.cratedb.index.ColumnDefinition;
import org.elasticsearch.index.analysis.NumericLongAnalyzer;

import java.io.IOException;

public class LongLuceneField extends NumericLuceneField<Long> {

    public Analyzer analyzer = new NumericLongAnalyzer();

    public LongLuceneField(String name) {
        this(name, false);
    }

    public LongLuceneField(String name, boolean allowMultipleValues) {
        super(name, allowMultipleValues);
        type = SortField.Type.LONG;
    }

    @Override
    public NumericRangeQuery<Long> rangeQuery(Object from, Object to,
                                                 boolean includeLower, boolean includeUpper) {
        return NumericRangeQuery.newLongRange(name,
                from == null ? null : ((Number)from).longValue(),
                to == null ? null : ((Number)to).longValue(),
                includeLower, includeUpper);
    }

    @Override
    public MultiTermQueryWrapperFilter rangeFilter(
            Object from, Object to, boolean includeLower, boolean includeUpper)
    {
        return NumericRangeFilter.newLongRange(name,
                ((Number)from).longValue(), ((Number)to).longValue(), includeLower, includeUpper);
    }

    @Override
    public ColumnDefinition getColumnDefinition(String tableName, int ordinalPosition) {
        return new ColumnDefinition(tableName, name, DataType.LONG, null, ordinalPosition, false, true);
    }

    @Override
    public Object mappedValue(Object value) {
        return (Long)value;
    }

    @Override
    public Field field(Long value) {
        return new LongField(name, value, Field.Store.YES);
    }

    public TokenStream tokenStream(Long value) throws IOException {
        return field(value).tokenStream(analyzer);
    }

}
