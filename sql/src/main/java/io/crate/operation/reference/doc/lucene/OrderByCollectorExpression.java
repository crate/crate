package io.crate.operation.reference.doc.lucene;

import io.crate.action.sql.query.SortSymbolVisitor;
import io.crate.analyze.OrderBy;
import io.crate.metadata.Reference;
import org.apache.lucene.search.FieldDoc;

import java.util.function.Function;

/**
 * A {@link LuceneCollectorExpression} is used to collect
 * sorting values from FieldDocs
 */
public class OrderByCollectorExpression extends LuceneCollectorExpression<Object> {

    private final int orderIndex;
    private final Function<Object, Object> valueConversion;
    private final Object missingValue;

    private Object value;

    public OrderByCollectorExpression(Reference ref, OrderBy orderBy, Function<Object, Object> valueConversion) {
        super(ref.column().fqn());
        this.valueConversion = valueConversion;
        assert orderBy.orderBySymbols().contains(ref) : "symbol must be part of orderBy symbols";
        orderIndex = orderBy.orderBySymbols().indexOf(ref);
        this.missingValue = LuceneMissingValue.missingValue(
            orderBy.reverseFlags()[orderIndex],
            orderBy.nullsFirst()[orderIndex],
            SortSymbolVisitor.LUCENE_TYPE_MAP.get(ref.valueType())
        );
    }

    private void value(Object value) {
        if (missingValue != null && missingValue.equals(value)) {
            this.value = null;
        } else {
            this.value = valueConversion.apply(value);
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
