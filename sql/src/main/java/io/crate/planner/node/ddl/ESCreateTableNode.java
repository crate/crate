package io.crate.planner.node.ddl;

import io.crate.planner.node.PlanVisitor;
import org.cratedb.DataType;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Map;

public class ESCreateTableNode extends DDLPlanNode {

    private final String tableName;
    private final Settings indexSettings;
    private final Map indexMapping;

    public ESCreateTableNode(String tableName,
                             Settings indexSettings,
                             Map indexMapping) {
        this.tableName = tableName;
        this.indexSettings = indexSettings;
        this.indexMapping = indexMapping;
    }

    public Map indexMapping() {
        return indexMapping;
    }

    public String tableName() {
        return tableName;
    }

    public Settings indexSettings() {
        return indexSettings;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitESCreateTableNode(this, context);
    }
}