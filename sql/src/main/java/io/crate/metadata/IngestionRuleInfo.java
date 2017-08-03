package io.crate.metadata;


public class IngestionRuleInfo {

    private final String name;
    private final String source;
    private final String target;
    private final String condition;

    public IngestionRuleInfo(String name, String source, String target, String condition) {
        this.name = name;
        this.source = source;
        this.target = target;
        this.condition = condition;
    }

    public String getName() {
        return name;
    }

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public String getCondition() {
        return condition;
    }
}
