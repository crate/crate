package io.crate.operation.reference.doc.lucene;

import io.crate.action.sql.query.SortSymbolVisitor;
import io.crate.analyze.OrderBy;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * A {@link LuceneCollectorExpression} is used to collect
 * sorting values from FieldDocs
 */
public class OrderByCollectorExpression extends LuceneCollectorExpression<Object> {

    private static final BytesRef MAX_TERM;

    static {
        BytesRefBuilder builder = new BytesRefBuilder();
        final char[] chars = Character.toChars(Character.MAX_CODE_POINT);
        builder.copyChars(chars, 0, chars.length);
        MAX_TERM = builder.toBytesRef();
    }

    private final int orderIndex;
    private final DataType valueType;
    private Object value;
    private Object missingValue;

    public OrderByCollectorExpression(Symbol symbol, OrderBy orderBy) {
        assert orderBy.orderBySymbols().contains(symbol);
        orderIndex = orderBy.orderBySymbols().indexOf(symbol);
        valueType = symbol.valueType();
        this.missingValue = missingValue(
                orderBy.reverseFlags()[orderIndex],
                orderBy.nullsFirst()[orderIndex],
                SortSymbolVisitor.LUCENE_TYPE_MAP.get(valueType)
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
}
