package org.cratedb.action.groupby.key;

import gnu.trove.map.hash.THashMap;

import java.util.Map;

public abstract class MapFactory<K, V> {

    public abstract Map<K, V> create();

    public static final MapFactory GENERIC = new MapFactory<Object, Object>() {

        @Override
        public Map<Object, Object> create() {
            return new THashMap<>(-1);
        }
    };

}
