package org.cratedb.action.groupby.key;

import com.carrotsearch.hppc.ObjectObjectMap;

public abstract class MapFactory<K, V> {

    public abstract ObjectObjectMap<K, V> create();
}
