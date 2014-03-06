package io.crate.metadata.doc;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import io.crate.DataType;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DocSysColumns {


    public static final ColumnIdent ID = new ColumnIdent("_id");
    public static final ColumnIdent VERSION = new ColumnIdent("_version");
    public static final ColumnIdent SCORE = new ColumnIdent("_score");
    public static final ColumnIdent UID = new ColumnIdent("_uid");
    public static final ColumnIdent SOURCE = new ColumnIdent("_source");

    public static ImmutableMap<ColumnIdent, DataType> columnIdents = ImmutableMap.<ColumnIdent, DataType>builder()
            .put(ID, DataType.STRING)
            .put(VERSION, DataType.LONG)
            .put(SCORE, DataType.DOUBLE)
            .put(UID, DataType.STRING)
            .put(SOURCE, DataType.OBJECT)
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
