package org.cratedb.action.collect;

import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.DataType;
import org.cratedb.action.FieldLookup;
import org.cratedb.index.ColumnDefinition;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;

public class FieldLookupExpression<ReturnType> extends
        CollectorExpression<ReturnType> implements ColumnReferenceExpression{

    private ESLogger logger = Loggers.getLogger(getClass());

    private final ColumnDefinition columnDefinition;
    private FieldLookup fieldLookup;

    public FieldLookupExpression(ColumnDefinition columnDefinition) {
        this.columnDefinition = columnDefinition;
    }

    public void startCollect(CollectorContext context) {
        fieldLookup = context.fieldLookup();
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        fieldLookup.setNextReader(context);
    }

    @Override
    public void setNextDocId(int docId) {
        fieldLookup.setNextDocId(docId);
    }

    public ReturnType evaluate() {
        try {
            return (ReturnType) fieldLookup.lookupField(columnDefinition.columnName);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public DataType returnType() {
        return columnDefinition.dataType;
    }


    public static FieldLookupExpression create(ColumnDefinition columnDefinition) {
        switch (columnDefinition.dataType) {
            case STRING:
                return new FieldLookupExpression<String>(columnDefinition);
            case DOUBLE:
                return new FieldLookupExpression<Double>(columnDefinition);
            case BOOLEAN:
                return new FieldLookupExpression<Boolean>(columnDefinition);
            case LONG:
                return new FieldLookupExpression<Long>(columnDefinition);
            default:
                return new FieldLookupExpression(columnDefinition);
        }
    }

    @Override
    public String toString() {
        return columnName();
    }

    @Override
    public String columnName() {
        return columnDefinition.columnName;
    }
}
