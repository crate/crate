package io.crate.operation.reference.doc.lucene;

import io.crate.action.sql.query.SortSymbolVisitor;
import io.crate.analyze.OrderBy;
import io.crate.metadata.Reference;
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

    public OrderByCollectorExpression(Reference ref, OrderBy orderBy) {
        super(ref.ident().columnIdent().fqn());
        assert orderBy.orderBySymbols().contains(ref) : "symbol must be part of orderBy symbols";
        orderIndex = orderBy.orderBySymbols().indexOf(ref);
        valueType = ref.valueType();
        this.missingValue = LuceneMissingValue.missingValue(
            orderBy.reverseFlags()[orderIndex],
            orderBy.nullsFirst()[orderIndex],
            SortSymbolVisitor.LUCENE_TYPE_MAP.get(valueType)
        );
    }

    private void value(Object value) {
        if (missingValue != null && missingValue.equals(value)) {
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
