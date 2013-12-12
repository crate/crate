package org.cratedb.action.groupby.key;

import com.carrotsearch.hppc.ObjectObjectMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.hppc.procedures.ObjectObjectProcedure;
import com.carrotsearch.hppc.procedures.ObjectProcedure;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.action.collect.Expression;
import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.CrateException;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class GroupTree extends Rows<GroupTree> {

    private final List<Expression> expressions;
    private final int depth;
    private final MapFactory[] mapFactories;
    private final ParsedStatement stmt;
    private final ObjectObjectMap[] maps;
    private final CacheRecycler cacheRecycler;

    private MapFactory getMapFactory(Expression expr) {
        switch (expr.returnType()) {
            case STRING:
                return new MapFactory<BytesRef, Object>() {
                    @Override
                    public ObjectObjectMap<BytesRef, Object> create() {
                        return cacheRecycler.<BytesRef, Object>hashMap(-1).v();
                    }
                };
            case LONG:
                return new MapFactory<Long, Object>() {
                    @Override
                    public ObjectObjectMap<Long, Object> create() {
                        return cacheRecycler.<Long, Object>hashMap(-1).v();
                    }
                };
        }
        return null;
    }

    public GroupTree(int numBuckets, ParsedStatement stmt, CacheRecycler cacheRecycler) {
        this.stmt = stmt;
        this.cacheRecycler = cacheRecycler;
        expressions = stmt.groupByExpressions();
        depth = expressions.size();
        assert (depth > 0);
        mapFactories = new MapFactory[depth];
        for (int i = 0; i < depth; i++) {
            mapFactories[i] = getMapFactory(expressions.get(i));
        }
        maps = new ObjectObjectOpenHashMap[numBuckets];
        for (int i = 0; i < maps.length; i++) {
            maps[i] = mapFactories[0].create();
        }
    }

    private int getRouting(Object o){
        if (o==null){
            return 0;
        }
        return Math.abs(o.hashCode()) % maps.length;
    }

    @SuppressWarnings("unchecked")
    public GroupByRow getRow() throws CrateException {
        ObjectObjectMap m = null;
        Object[] key = new Object[depth];
        for (int i = 0; i < depth; i++) {
            boolean last = (i == depth - 1);
            key[i] = expressions.get(i).evaluate();
            if (i == 0) {
                m = maps[getRouting(key[0])];
            }
            Object value = m.get(key[i]);
            if (value == null) {
                if (last) {
                    GroupByRow row = GroupByRow.createEmptyRow(
                            new GroupByKey(Arrays.copyOf(key, key.length)), stmt);
                    m.put(key[i], row);
                    return row;
                } else {
                    ObjectObjectMap subMap = mapFactories[i].create();
                    m.put(key[i], subMap);
                    m = subMap;
                }
            } else {
                if (last) {
                    return (GroupByRow) value;
                } else {
                    m = (ObjectObjectMap) value;
                }
            }
        }
        return null;
    }

    private synchronized void printlns(List<String> lines){
        String res = "";
        for (String l: lines){
            res += "\n" + stmt + ": " + l;
        }
        System.out.println(res);
    }


    private void writeMap(ObjectObjectMap m,
                          final StreamOutput out,
                          final int level,
                          final DataType.Streamer[] keyStreamers) throws IOException {

        final DataType.Streamer keyStreamer = keyStreamers[level];
        final AtomicReference<IOException> lastException = new AtomicReference<>();

        out.writeVInt(m.size());
        if (level==depth-1){
            // last

            m.forEach(new ObjectObjectProcedure() {
                @Override
                public void apply(Object key, Object value) {
                    try {
                        keyStreamer.writeTo(out, key);
                        ((GroupByRow)value).writeStates(out, stmt);
                    } catch (IOException ex) {
                        lastException.set(ex);
                    }
                }
            });
        } else {
            m.forEach(new ObjectObjectProcedure() {
                @Override
                public void apply(Object key, Object value) {
                    try {
                        keyStreamer.writeTo(out, key);
                        writeMap((ObjectObjectMap)value, out, level + 1, keyStreamers);
                    } catch (IOException ex) {
                        lastException.set(ex);
                    }
                }
            });
        }

        if (lastException.get() != null) {
            throw lastException.get();
        }
    }

    @SuppressWarnings("unchecked")
    private void readMap(ObjectObjectMap m, StreamInput in, int level,
            DataType.Streamer[] keyStreamers, Object[] key) throws IOException {
        DataType.Streamer keyStreamer = keyStreamers[level];
        int size = in.readVInt();
        if (level==depth-1){
            //last
            for (int i = 0; i < size ; i++) {
                GroupByRow row = new GroupByRow();
                key[level] = keyStreamer.readFrom(in);
                row.readFrom(in, new GroupByKey(Arrays.copyOf(key, key.length)), stmt);
                m.put(key[level], row);
            }
        } else {
            for (int i = 0; i < size ; i++) {
                ObjectObjectMap subMap = mapFactories[level+1].create();
                key[level] = keyStreamer.readFrom(in);
                m.put(key[level], subMap);
                readMap(subMap, in, level+1, keyStreamers, key);
            }
        }
    }


    public void writeBucket(StreamOutput out, int idx) throws IOException {
        writeMap(maps[idx], out, 0, getStreamers());

    }

    private DataType.Streamer[] getStreamers() {
        DataType.Streamer[] keyStreamers = new DataType.Streamer[depth];
        for (int i = 0; i < keyStreamers.length; i++) {
            keyStreamers[i] = expressions.get(i).returnType().streamer();
        }
        return keyStreamers;
    }

    @Override
    public void readBucket(StreamInput in, int idx) throws IOException {
        readMap(maps[idx], in, 0, getStreamers(), new Object[depth]);
    }

    @SuppressWarnings("unchecked")
    private void mergeMaps(final ObjectObjectMap m1, final ObjectObjectMap m2, final int level) {
        final boolean last = level == depth - 1;

        m2.forEach(new ObjectObjectProcedure() {
            @Override
            public void apply(Object key, Object value) {
                if (m1.containsKey(key)) {
                    if (last) {
                        ((GroupByRow)m1.get(key)).merge((GroupByRow)value);
                    } else {
                        mergeMaps((ObjectObjectMap)m1.get(key), (ObjectObjectMap)value, level + 1);
                    }
                } else {
                    m1.put(key, value);
                }
            }
        });
    }

    public ObjectObjectMap[] maps() {
        return maps;
    }

    public void merge(GroupTree other) {
        for (int i = 0; i < maps.length; i++) {
            mergeMaps(maps[i], other.maps()[i], 0);
        }
    }

    @Override
    public void walk(RowVisitor visitor) {
        for (ObjectObjectMap m : maps) {
            walk(visitor, m, 0);
        }
    }

    private void walk(final RowVisitor visitor, final ObjectObjectMap m, final int level) {
        if (level == depth - 1) {
            // last
            m.values().forEach(new ObjectProcedure() {
                @Override
                public void apply(Object value) {
                    visitor.visit((GroupByRow) value);
                }
            });
        } else {
            m.values().forEach(new ObjectProcedure() {
                @Override
                public void apply(Object value) {
                    walk(visitor, (ObjectObjectMap) value, level + 1);
                }
            });
        }
    }
}



