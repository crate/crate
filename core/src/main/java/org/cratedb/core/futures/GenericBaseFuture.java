package org.cratedb.core.futures;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.concurrent.BaseFuture;

public class GenericBaseFuture<V> extends BaseFuture<V> {

    @Override
    public boolean set(@Nullable V value) {
        return super.set(value);
    }
}
