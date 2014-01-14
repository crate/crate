package io.crate.metadata.sys;

import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import org.cratedb.DataType;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SystemReferences {

    public static final String SCHEMA = "sys";
    public static final TableIdent NODES = new TableIdent(SCHEMA, "nodes");

    private static final Map<ReferenceIdent, ReferenceInfo> referenceInfos = new ConcurrentHashMap<>();

    public static ReferenceInfo register(ReferenceInfo info) {
        referenceInfos.put(info.ident(), info);
        return info;
    }

    public static ReferenceInfo registerNodeReference(String column, DataType type, List<String> path) {
        return register(new ReferenceInfo(
                new ReferenceIdent(NODES, column, path),
                RowGranularity.NODE,
                type));
    }

}