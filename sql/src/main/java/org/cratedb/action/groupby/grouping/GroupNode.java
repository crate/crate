package org.cratedb.action.groupby.grouping;

import java.util.Map;

public class GroupNode<KeyType, SubKeyType, SubNodeType> {

    private final KeyType key;
    private final Map<SubKeyType, SubNodeType> subnodes;

    public GroupNode(KeyType key, Map<SubKeyType, SubNodeType> subnodes) {
        this.key = key;
        this.subnodes = subnodes;
    }

}
