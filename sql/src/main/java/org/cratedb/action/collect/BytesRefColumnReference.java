package org.cratedb.action.collect;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.GroupByOnArrayUnsupportedException;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;

public class BytesRefColumnReference extends FieldCacheExpression<IndexFieldData, BytesRef> {

    private BytesValues values;

    public BytesRefColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public BytesRef evaluate() throws CrateException {
        switch (values.setDocument(docId)) {
            case 0:
                return null;
            case 1:
                values.nextValue();
                return values.copyShared();
            default:
                throw new GroupByOnArrayUnsupportedException(columnName());
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        super.setNextReader(context);
        values = indexFieldData.load(context).getBytesValues(true);
    }

    @Override
    public DataType returnType() {
        return DataType.STRING;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof BytesRefColumnReference))
            return false;
        return columnName.equals(((BytesRefColumnReference) obj).columnName);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }
}

