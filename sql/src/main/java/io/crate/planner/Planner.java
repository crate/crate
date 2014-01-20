package io.crate.planner;

import io.crate.analyze.Analysis;
import io.crate.sql.tree.DefaultTraversalVisitor;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class Planner extends DefaultTraversalVisitor<Void, Analysis> {

    public Plan plan(Analysis analysis) {
        analysis.query();
        //RelationPlanner planner = new RelationPlanner(analysis, symbolAllocator, idAllocator, metadata, session);
        //plan = planner.process(analysis.getQuery(), null);

        return null;
    }
}
