package io.crate.operator.reference.sys;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.sys.SysExpression;
import io.crate.metadata.sys.SystemReferences;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.monitor.os.OsService;

public class NodeLoadExpression extends SysObjectReference<Double> {

    public static final String COLNAME = "load";


    public static final String ONE = "1";
    public static final String FIVE = "5";
    public static final String FIFTEEN = "15";

    public static final ReferenceInfo INFO_LOAD = SystemReferences.registerNodeReference(
            COLNAME, DataType.OBJECT, null);
    public static final ReferenceInfo INFO_LOAD_1 = SystemReferences.registerNodeReference(
            COLNAME, DataType.DOUBLE, ImmutableList.of(ONE));
    public static final ReferenceInfo INFO_LOAD_5 = SystemReferences.registerNodeReference(
            COLNAME, DataType.DOUBLE, ImmutableList.of(FIVE));
    public static final ReferenceInfo INFO_LOAD_15 = SystemReferences.registerNodeReference(
            COLNAME, DataType.DOUBLE, ImmutableList.of(FIFTEEN));


    private final OsService osService;

    @Inject
    public NodeLoadExpression(OsService osService) {
        this.osService = osService;
        childImplementations.put(ONE, new LoadExpression(0, INFO_LOAD_1));
        childImplementations.put(FIVE, new LoadExpression(1, INFO_LOAD_5));
        childImplementations.put(FIFTEEN, new LoadExpression(2, INFO_LOAD_15));
    }

    class LoadExpression extends SysExpression<Double> {

        private final int idx;
        private final ReferenceInfo info;

        LoadExpression(int idx, ReferenceInfo info) {
            this.idx = idx;
            this.info = info;
        }

        @Override
        public ReferenceInfo info() {
            return info;
        }

        @Override
        public Double value() {
            return osService.stats().loadAverage()[idx];
        }
    }

    @Override
    public ReferenceInfo info() {
        return INFO_LOAD;
    }

}
