package io.crate.metadata.doc;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import org.cratedb.DataType;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DocSysColumns {

    private static ImmutableMap<ColumnIdent, DataType> columnIdents = ImmutableMap.<ColumnIdent, DataType>builder()
            .put(new ColumnIdent("_id"), DataType.STRING)
            .put(new ColumnIdent("_version"), DataType.LONG)
            .put(new ColumnIdent("_score"), DataType.DOUBLE)
            .put(new ColumnIdent("_uid"), DataType.STRING)
            .put(new ColumnIdent("_source"), DataType.OBJECT)
            .build();

    private static ReferenceInfo newInfo(TableIdent table, ColumnIdent column, DataType dataType) {
        return new ReferenceInfo(new ReferenceIdent(table, column), RowGranularity.DOC, dataType);
    }

    public static List<Tuple<ColumnIdent, ReferenceInfo>> forTable(TableIdent tableIdent) {
        List<Tuple<ColumnIdent, ReferenceInfo>> columns = new ArrayList<>(columnIdents.size());

        for (Map.Entry<ColumnIdent, DataType> entry : columnIdents.entrySet()) {
            columns.add(new Tuple<>(entry.getKey(), newInfo(tableIdent, entry.getKey(), entry.getValue())));
        }

        return columns;
    }
}
