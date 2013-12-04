package org.cratedb.lucene.fields;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.search.MultiTermQueryWrapperFilter;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.SortField;
import org.cratedb.DataType;
import org.cratedb.index.ColumnDefinition;
import org.elasticsearch.index.analysis.NumericIntegerAnalyzer;

import java.io.IOException;

public class IntegerLuceneField extends NumericLuceneField<Integer> {

    public Analyzer analyzer = new NumericIntegerAnalyzer();

    public IntegerLuceneField(String name) {
        this(name, false);
    }

    public IntegerLuceneField(String name, boolean allowMultipleValues) {
        super(name, allowMultipleValues);
        type = SortField.Type.INT;
    }

    @Override
    public NumericRangeQuery<Integer> rangeQuery(Object from, Object to,
                                                 boolean includeLower, boolean includeUpper) {
        return NumericRangeQuery.newIntRange(name, (Integer)from, (Integer)to, includeLower, includeUpper);
    }

    @Override
    public MultiTermQueryWrapperFilter rangeFilter(
        Object from, Object to, boolean includeLower, boolean includeUpper)
    {
        return NumericRangeFilter.newIntRange(name, (Integer)from, (Integer)to, includeLower, includeUpper);
    }

    @Override
    public ColumnDefinition getColumnDefinition(String tableName, int ordinalPosition) {
        return new ColumnDefinition(tableName, name, DataType.INTEGER, null, ordinalPosition, false, true);
    }

    @Override
    public Object mappedValue(Object value) {
        return (Integer)value;
    }

    @Override
    public Field field(Integer value) {
        return new IntField(name, value, Field.Store.YES);
    }

    public TokenStream tokenStream(Integer value) throws IOException {
        return field(value).tokenStream(analyzer);
    }

}
