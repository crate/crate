package io.crate.operation.reference.doc.lucene;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.OrderBy;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import java.util.Map;

/**
 * A {@link LuceneCollectorExpression} is used to collect
 * sorting values from FieldDocs
 */
public class OrderByCollectorExpression extends LuceneCollectorExpression<Object> {

    private Object value;

    private final int orderIndex;

    private final Object missingValue;

    private final DataType valueType;

    public OrderByCollectorExpression(Symbol symbol, OrderBy orderBy) {
        assert orderBy.orderBySymbols().contains(symbol);
        orderIndex = orderBy.orderBySymbols().indexOf(symbol);
        valueType = symbol.valueType();
        this.missingValue = missingValue(
                orderBy.reverseFlags()[orderIndex],
                orderBy.nullsFirst()[orderIndex],
                luceneTypeMap.get(valueType)
        );
    }

    private void value(Object value) {
        if ( missingValue != null && missingValue.equals(value) ) {
            this.value = null;
        } else {
            this.value = valueType.value(value);
        }
    }

    public void setNextFieldDoc(FieldDoc fieldDoc) {
        value(fieldDoc.fields[orderIndex]);
    }

    @Override
    public Object value() {
        return value;
    }

    private static final BytesRef MAX_TERM;
    static {
        BytesRefBuilder builder = new BytesRefBuilder();
        final char[] chars = Character.toChars(Character.MAX_CODE_POINT);
        builder.copyChars(chars, 0, chars.length);
        MAX_TERM = builder.toBytesRef();
    }

    /** Calculates the missing Values as in {@link org.elasticsearch.index.fielddata.IndexFieldData}
     * The results in the {@link org.apache.lucene.search.ScoreDoc} contains this missingValues instead of nulls. Because we
     * need nulls in the result, it's necessary to check if a value is a missingValue.
     */
    private Object missingValue(boolean reverseFlag, Boolean nullFirst, SortField.Type type) {
        boolean min = reverseFlag ^ (nullFirst != null ? nullFirst : reverseFlag);
        switch (type) {
            case INT:
            case LONG:
                return min ? Long.MIN_VALUE : Long.MAX_VALUE;
            case FLOAT:
                return min ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
            case DOUBLE:
                return min ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
            case STRING:
            case STRING_VAL:
                return min ? null : MAX_TERM;
            default:
                throw new UnsupportedOperationException("Unsupported reduced type: " + type);
        }
    }

    /**
     * @link io.crate.action.sql.query.CrateSearchService.luceneTypeMap
     */
    private static final Map<DataType, SortField.Type> luceneTypeMap = ImmutableMap.<DataType, SortField.Type>builder()
            .put(DataTypes.STRING, SortField.Type.STRING)
            .put(DataTypes.LONG, SortField.Type.LONG)
            .put(DataTypes.INTEGER, SortField.Type.INT)
            .put(DataTypes.DOUBLE, SortField.Type.DOUBLE)
            .put(DataTypes.FLOAT, SortField.Type.FLOAT)
            .build();
}
