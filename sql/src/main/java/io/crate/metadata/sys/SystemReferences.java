package io.crate.metadata.sys;

import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import org.cratedb.DataType;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class provides a static registry of global system table metadata.
 */
public class SystemReferences {

    public static final String SCHEMA = "sys";
    public static final TableIdent NODES_IDENT = new TableIdent(SCHEMA, "nodes");

    private static final Map<ReferenceIdent, ReferenceInfo> referenceInfos = new ConcurrentHashMap<>();

    public static ReferenceInfo register(ReferenceInfo info) {
        referenceInfos.put(info.ident(), info);
        return info;
    }

    public static ReferenceInfo get(ReferenceIdent ident) {
        return referenceInfos.get(ident);
    }

    public static ReferenceInfo registerNodeReference(String column, DataType type, List<String> path) {
        return register(new ReferenceInfo(
                new ReferenceIdent(NODES_IDENT, column, path),
                RowGranularity.NODE,
                type));
    }

}