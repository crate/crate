package io.crate.planner.node.ddl;

import io.crate.planner.node.PlanVisitor;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

public class ESCreateIndexNode extends DDLPlanNode {

    private final String tableName;
    private final Settings indexSettings;
    private final Map indexMapping;

    public ESCreateIndexNode(String tableName,
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
        return visitor.visitESCreateIndexNode(this, context);
    }
}