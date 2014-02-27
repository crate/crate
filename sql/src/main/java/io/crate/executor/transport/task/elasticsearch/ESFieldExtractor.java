package io.crate.executor.transport.task.elasticsearch;

import io.crate.metadata.ColumnIdent;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class ESFieldExtractor {

    private static final Object NOT_FOUND = new Object();

    public abstract Object extract(SearchHit hit);

    public static class Source extends ESFieldExtractor {

        private final ColumnIdent ident;

        public Source(ColumnIdent ident) {
            this.ident = ident;
        }

        @Override
        public Object extract(SearchHit hit) {
            return toValue(hit.getSource());
        }

        Object down(Object c, int idx) {
            if (idx == ident.path().size()) {
                return c == NOT_FOUND ? null : c;
            }
            if (c instanceof List) {
                List l = (List) c;
                ArrayList children = new ArrayList(l.size());
                for (Object child : l) {
                    Object sub = down(child, idx);
                    if (sub != NOT_FOUND) {
                        children.add(sub);
                    }
                }
                return children;
            } else if (c instanceof Map) {
                Map cm = ((Map) c);
                if (cm.containsKey(ident.path().get(idx))) {
                    return down(((Map) c).get(ident.path().get(idx)), idx + 1);
                } else {
                    return NOT_FOUND;
                }
            }
            throw new IndexOutOfBoundsException("Failed to get path");
        }

        Object toValue(@Nullable Map<String, Object> source) {
            if (source == null || source.size() == 0) {
                return null;
            }
            Object top = source.get(ident.name());
            if (ident.isColumn()) {
                return top;
            }
            if (top==null){
                return null;
            }
            Object result = down(top, 0);
            return result == NOT_FOUND ? null : result;
        }
    }

}
