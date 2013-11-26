package org.cratedb.action.sql;

import com.google.common.collect.ComparisonChain;

import java.util.Comparator;
import java.util.List;

public class SQLRowComparator implements Comparator<List> {

    private final OrderByColumnIdx[] orderByIndices;

    public SQLRowComparator(OrderByColumnIdx[] orderByIndices) {
        this.orderByIndices = orderByIndices;
    }

    @Override
    public int compare(List o1, List o2) {
        ComparisonChain chain = ComparisonChain.start();
        for (OrderByColumnIdx orderByIndex : orderByIndices) {
            Object left = o1.get(orderByIndex.index);
            Object right = o2.get(orderByIndex.index);

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
