package org.cratedb.action.collect;

import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.Constants;
import org.cratedb.sql.parser.parser.ValueNode;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.FieldMapper;

import java.io.IOException;

public abstract class FieldCacheExpression<IFD extends IndexFieldData, ReturnType> extends
        ColumnReferenceCollectorExpression<ReturnType> {

    private final static String[] DEFAULT_MAPPING_TYPES = new String[]{
            Constants.DEFAULT_MAPPING_TYPE};

    protected IFD indexFieldData;
    protected int docId;

    public FieldCacheExpression(String columnName) {
        super(columnName);
    }

    public void startCollect(CollectorContext context){
        FieldMapper mapper = context.searchContext().mapperService().smartNameFieldMapper
                (columnName, DEFAULT_MAPPING_TYPES);
        indexFieldData = (IFD) context.searchContext().fieldData().getForField(mapper);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        this.docId = -1;
    }

    @Override
    public void setNextDocId(int docId) {
        this.docId = docId;
    }

}
