
package io.crate.lucene;

import java.util.List;

import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.search.Query;

import io.crate.metadata.Reference;
import io.crate.types.StorageReader;


public class LuceneFloatReader implements StorageReader<Query, Float> {

    @Override
    public Query termQuery(Reference ref, Float value) {
        return FloatPoint.newExactQuery(ref.column().fqn(), value);
    }

    @Override
    public Query termsQuery(Reference ref, List<Float> values) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Query rangeQuery(Reference ref, Float lower, Float upper, boolean includeLower, boolean includeUpper) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Query existsQuery(Reference ref) {
        // TODO Auto-generated method stub
        return null;
    }
}
