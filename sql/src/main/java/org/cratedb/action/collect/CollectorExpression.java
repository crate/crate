package org.cratedb.action.collect;

import org.apache.lucene.index.AtomicReaderContext;

/**
 * An expression which gets evaluated in the collect phase
 */
public abstract class CollectorExpression<ReturnType> implements Expression<ReturnType> {

    public void startCollect(CollectorContext context){

    }

    public void setNextDocId(int doc){
    }

    public void setNextReader(AtomicReaderContext context){
    }

    public abstract ReturnType evaluate();

}
