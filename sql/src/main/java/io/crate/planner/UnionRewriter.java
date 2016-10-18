package io.crate.planner;

import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.TwoRelationsUnion;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.QueriedRelation;

import java.util.ArrayList;
import java.util.List;

/**
 * Processes a tree of Unions and returns a flat List of all sub-relations
 */
public final class UnionRewriter {

    public static List<QueriedRelation> flatten(TwoRelationsUnion twoRelationsUnion) {
        List<QueriedRelation> relations = new ArrayList<>();
        FlatteningVisitor.INSTANCE.process(twoRelationsUnion, relations);
        return relations;
    }

    private static class FlatteningVisitor
        extends AnalyzedRelationVisitor<List<QueriedRelation>, Void> {

        private static final FlatteningVisitor INSTANCE = new FlatteningVisitor();

        @Override
        public Void visitQueriedTable(QueriedTable relation, List<QueriedRelation> relations) {
            relations.add(relation);
            return null;
        }

        @Override
        public Void visitQueriedDocTable(QueriedDocTable relation, List<QueriedRelation> relations) {
            relations.add(relation);
            return null;
        }

        @Override
        public Void visitMultiSourceSelect(MultiSourceSelect relation, List<QueriedRelation> relations) {
            relations.add(relation);
            return null;
        }

        @Override
        public Void visitQueriedSelectRelation(QueriedSelectRelation relation, List<QueriedRelation> relations) {
            relations.add(relation);
            return null;
        }

        @Override
        public Void visitTwoRelationsUnion(TwoRelationsUnion twoRelationsUnion, List<QueriedRelation> relations) {
            process(twoRelationsUnion.first(), relations);
            process(twoRelationsUnion.second(), relations);
            return null;
        }
    }
}
