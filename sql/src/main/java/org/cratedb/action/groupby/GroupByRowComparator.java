package org.cratedb.action.groupby;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.cratedb.action.sql.OrderByColumnIdx;

import java.util.Comparator;

public class GroupByRowComparator implements Comparator<GroupByRow> {

    private final OrderByColumnIdx[] orderByIndices;

    public GroupByRowComparator(OrderByColumnIdx[] orderByIndices) {
        this.orderByIndices = orderByIndices;
    }

    @Override
    public int compare(GroupByRow o1, GroupByRow o2) {
        ComparisonChain chain = ComparisonChain.start();
        for (OrderByColumnIdx orderByIndex : orderByIndices) {
            Object left = o1.get(orderByIndex.index);
            Object right = o2.get(orderByIndex.index);

            if (left == null) {
                return 1;
            }

            chain = chain.compare((Comparable)left, (Comparable)right, orderByIndex.ordering);
        }

        return chain.result();
    }
}
