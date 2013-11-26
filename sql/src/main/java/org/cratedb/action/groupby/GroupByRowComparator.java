package org.cratedb.action.groupby;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.cratedb.action.sql.OrderByColumnIdx;

import java.util.Comparator;

public class GroupByRowComparator implements Comparator<GroupByRow> {

    private final OrderByColumnIdx[] orderByIndices;
    private final GroupByFieldExtractor[] extractors;

    public GroupByRowComparator(GroupByFieldExtractor[] groupByFieldExtractors,
                                OrderByColumnIdx[] orderByIndices) {
        this.extractors = groupByFieldExtractors;
        this.orderByIndices = orderByIndices;
    }

    @Override
    public int compare(GroupByRow o1, GroupByRow o2) {
        ComparisonChain chain = ComparisonChain.start();
        for (OrderByColumnIdx orderByIndex : orderByIndices) {
            Object left = extractors[orderByIndex.index].getValue(o1);
            Object right = extractors[orderByIndex.index].getValue(o2);

            if (left != null && right != null) {
                chain = chain.compare((Comparable)left, (Comparable)right, orderByIndex.ordering);
            } else if (right != null) {
                chain = chain.compare(0, 1);
            } else if (left != null) {
                chain = chain.compare(1, 0);
            } else {
                chain = chain.compare(0, 0);
            }
        }

        return chain.result();
    }
}
