package io.crate.operation.reference.doc.lucene;

import io.crate.action.sql.query.SortSymbolVisitor;
import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.Symbol;
import io.crate.types.DataType;
import org.apache.lucene.search.FieldDoc;

/**
 * A {@link LuceneCollectorExpression} is used to collect
 * sorting values from FieldDocs
 */
public class OrderByCollectorExpression extends LuceneCollectorExpression<Object> {

    private final int orderIndex;
    private final DataType valueType;
    private Object value;
    private Object missingValue;

    public OrderByCollectorExpression(Symbol symbol, OrderBy orderBy) {
        assert orderBy.orderBySymbols().contains(symbol);
        orderIndex = orderBy.orderBySymbols().indexOf(symbol);
        valueType = symbol.valueType();
        this.missingValue = LuceneMissingValue.missingValue(
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

}
