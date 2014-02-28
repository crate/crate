package io.crate.planner.node;

import org.elasticsearch.common.settings.Settings;

import java.util.Map;

public class ESCreateTableNode extends DDLPlanNode {

    public Map indexMapping() {
        return null;
    }

    public String tableName() {
        return null;
    }

    public Settings indexSettings() {
        return null;
    }
}