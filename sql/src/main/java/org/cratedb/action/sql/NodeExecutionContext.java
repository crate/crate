package org.cratedb.action.sql;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.indices.IndicesService;

import java.util.Set;
import java.util.TreeSet;

public class NodeExecutionContext {

    private final IndicesService indicesService;

    @Inject
    public NodeExecutionContext(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    /**
     *
     * @param name the name of the table
     * @return the table
     */
    public TableExecutionContext tableContext(String name) {
        DocumentMapper dm = indicesService.indexServiceSafe(name).mapperService()
                .documentMapper("default");
        if (dm!=null){
            return new TableExecutionContext(name, dm);
        }
        return null;
    }

    public class TableExecutionContext {

        private final DocumentMapper documentMapper;

        TableExecutionContext(String name, DocumentMapper documentMapper) {
            this.documentMapper = documentMapper;

        }

        /**
         * returns all columns defined in the mapping as a sorted sequence
         * to be used in "*" selects.
         *
         * @return a sequence of column names
         */
        public Iterable<String> allCols() {
            Set<String> res = new TreeSet<String>();
            for (FieldMapper m : documentMapper.mappers()) {
                String name = m.names().name();
                if (!name.startsWith("_")) {
                    res.add(name);
                }
            }
            return res;
        }


    }

}
