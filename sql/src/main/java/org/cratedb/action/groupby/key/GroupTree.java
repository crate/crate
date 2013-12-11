package org.cratedb.action.groupby.key;

import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.action.collect.Expression;
import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GroupTree extends Rows<GroupTree> {

    private final List<Expression> expressions;
    private final int depth;
    private final MapFactory[] mapFactories;
    private final ParsedStatement stmt;
    private final Map[] maps;
    private final CacheRecycler cacheRecycler;

    private MapFactory getMapFactory(Expression expr) {
        switch (expr.returnType()) {
            case STRING:
                return new MapFactory<BytesRef, Object>() {
                    @Override
                    public Map<BytesRef, Object> create() {
                        return cacheRecycler.<BytesRef, Object>hashMap(-1).v();
                    }
                };
            case LONG:
                return new MapFactory<Long, Object>() {
                    @Override
                    public Map<Long, Object> create() {
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
        maps = new Map[numBuckets];
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

    public GroupByRow getRow() {
        Map m = null;
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
                    Map subMap = mapFactories[i].create();
                    m.put(key[i], subMap);
                    m = subMap;
                }
            } else {
                if (last) {
                    return (GroupByRow) value;
                } else {
                    m = (Map) value;
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

    public void dump(int i){

        List<String> lines = new ArrayList<>();
        lines.add("dump: " + i);
        //for (int i = 0; i < maps.length; i++) {
            Map<Object, Object> m = maps[i];
            if (m.size()==0){
                return;
            }
            lines.add("------------- start map:" + i +  "-------");

            for (Map.Entry e: m.entrySet()){
                String k = ((GroupByRow) e.getValue()).key.toString();
                int h = getRouting(e.getKey());
                if (i!=h){
                    lines.add("FFFffffffffffFAIL-------------------------FFFFFFFFFFFFFFFF");
                }
                lines.add("key: " +  e.getKey() + " modulo: " + h + " groupkey: " +  k);
            }
            lines.add("------------- end map:" + i +  "-------");
        //}
        printlns(lines);
    }

    private void writeMap(Map<Object, Object> m, StreamOutput out, int level,
            DataType.Streamer[] keyStreamers) throws IOException {
        DataType.Streamer keyStreamer = keyStreamers[level];
        out.writeVInt(m.size());
        if (level==depth-1){
            // last
            for (Map.Entry entry: m.entrySet()){
                keyStreamer.writeTo(out, entry.getKey());
                ((GroupByRow) entry.getValue()).writeStates(out);
            }
        } else {
            for (Map.Entry entry: m.entrySet()){
                keyStreamer.writeTo(out, entry.getKey());
                writeMap((Map) entry.getValue(), out, level + 1, keyStreamers);
            }
        }
    }

    private void readMap(Map<Object, Object> m, StreamInput in, int level,
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
                Map subMap = mapFactories[level+1].create();
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

    private void mergeMaps(Map<Object, Object> m1, Map<Object, Object> m2, int level) {
        boolean last = level == depth - 1;
        for (Map.Entry entry : m2.entrySet()) {
            if (m1.containsKey(entry.getKey())) {
                if (last) {
                    ((GroupByRow) m1.get(entry.getKey())).merge((GroupByRow) entry.getValue());
                } else {
                    mergeMaps((Map) m1.get(entry.getKey()), (Map) entry.getValue(), level + 1);
                }
            } else {
                m1.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public Map[] maps() {
        return maps;
    }

    public void merge(GroupTree other) {
        for (int i = 0; i < maps.length; i++) {
            mergeMaps(maps[i], other.maps()[i], 0);
        }
    }

    @Override
    public void walk(RowVisitor visitor) {
        for (Map m : maps) {
            walk(visitor, m, 0);
        }
    }

    private void walk(RowVisitor visitor, Map<Object, Object> m, int level) {
        if (level == depth - 1) {
            // last
            for (Object row : m.values()) {
                visitor.visit((GroupByRow) row);
            }
        } else {
            for (Object row : m.values()) {
                walk(visitor, (Map) row, level + 1);
            }
        }
    }

}



