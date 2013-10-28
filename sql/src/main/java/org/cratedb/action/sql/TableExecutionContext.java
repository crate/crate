package org.cratedb.action.sql;

import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.*;

public class TableExecutionContext implements ITableExecutionContext {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final MappingMetaData mappingMetaData;
    private final String tableName;
    private Map<String, Object> mapping;
    private Map<String, Object> mappingMeta;


    TableExecutionContext(String name, MappingMetaData mappingMetaData) {
        this.mappingMetaData = mappingMetaData;
        this.tableName = name;
    }


    protected Map<String, Object> mapping() {
        if (mapping == null) {
            try {
                if (mappingMetaData == null) {
                    mapping = new HashMap<>();
                } else {
                    mapping = mappingMetaData.sourceAsMap();
                }
            } catch (IOException ex) {
                logger.error(ex.getMessage(), ex);
                // :/
            }
        }
        return mapping;
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> mappingMeta() {
        if (mappingMeta == null) {
            Map<String, Object> mapping = mapping();
            if (mapping == null) {
                mappingMeta = new HashMap<>();
            } else {
                Object _meta = mapping.get("_meta");
                if (_meta == null) {
                    mappingMeta = new HashMap<>();
                } else {
                    assert _meta instanceof Map;
                    mappingMeta = (Map<String, Object>)_meta;
                }
            }
        }

        return mappingMeta;
    }

    /**
     *
     * @param name the name of the column
     * @param value the value to be mapped
     * @return the value converted to the proper type
     */
    public Object mappedValue(String name, Object value){
        // TODO: convert decimal
        return value;
    }

    /**
     * Returns the ``primary key`` column names defined at index creation under the ``_meta``
     * key. If not defined, return empty list.
     *
     * @return a list of primary key column names
     */
    @SuppressWarnings("unchecked")
    public List<String> primaryKeys() {
        List<String> pks = new ArrayList<>();

        Object srcPks = mappingMeta().get("primary_keys");
        if (srcPks instanceof String) {
            pks.add((String)srcPks);
        } else if (srcPks instanceof List) {
            pks.addAll((List)srcPks);
        }

        return pks;
    }

    /**
     * Returns the ``primary key`` column names defined at index creation under the ``_meta``
     * key. If none defined, add ``_id`` as primary key(Default).
     *
     * @return a list of primary key column names
     */
    public List<String> primaryKeysIncludingDefault() {
        List<String> primaryKeys = primaryKeys();
        if (primaryKeys.isEmpty()) {
            primaryKeys.add("_id"); // Default Primary Key (only for optimization, not for consistency checks)
        }
        return primaryKeys;
    }


    /**
     * returns all columns defined in the mapping as a sorted sequence
     * to be used in "*" selects.
     *
     * @return a sequence of column names
     */
    @SuppressWarnings("unchecked")
    public Iterable<String> allCols() {
        Set<String> res = new TreeSet<>();
        for (String columnName : ((Map<String, Object>)mapping().get("properties")).keySet()) {
            if (columnName.startsWith("_") || columnName.contains(".")) {
                continue;
            }

            res.add(columnName);
        }

        //// add object type field names
        //Map<String, ObjectMapper> objectMappers = documentMapper.objectMappers();
        //for (Map.Entry<String, ObjectMapper> entry : objectMappers.entrySet()) {
        //    ObjectMapper mapper = entry.getValue();
        //    if (mapper instanceof RootObjectMapper) {
        //        continue;
        //    }
        //    res.add(entry.getKey());
        //}
        return res;
    }

    /**
     * Check if given name is equal to defined routing name.
     *
     * @param name
     * @return
     */
    public Boolean isRouting(String name) {
        String routingPath = mappingMetaData.routing().path();
        if (routingPath == null) {
            // the primary key(s) values are saved under _id, so they are used as default
            // routing values
            if (primaryKeys().contains(name)) {
                return true;
            }
            routingPath = "_id";
        }
        return routingPath.equals(name);
    }
}
