package io.crate.common.collections;


import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

/**
 * Map based on trie.
 * Supports only basic put/get/remove/containsKey operations
 * Supports null keys and values.
 *
 * Instead of of implementing primitive map per type, public API
 * casts number to long (only once in the beginning) as it's the widest type which can handle all other types.
 *
 * Each of get/put/remove/containsKey does maximum 18 (max digits in long) operations (arithmetic and state transition) so it's O(1).
 */
public class NumericTrieMap<K extends Number, V> implements Map<K,V> {

    // We only add to the array list, it never shrinks.
    private ArrayList<TrieNode> nodes = new ArrayList<>();

    // isLeaf and value are false and null correspondingly.
    protected TrieNode ROOT_NODE = new TrieNode();
    protected TrieNode NULL_KEY_NODE = new TrieNode();

    enum DIGIT {
        ZERO,
        ONE,
        TWO,
        THREE,
        FOUR,
        FIVE,
        SIX,
        SEVEN,
        EIGHT,
        NINE
    }

    /**
     * transitions: some digit d in number ***d*** -> index in 'nodes' list where the next node is stored;
     * isLeaf - indicates whether V has a value. V == null cannot be used to leaf detection as it can mean both absence of a value and presence of the NULL value.
     * Also, isLeaf is needed to perform 'soft delete' without shrinking 'nodes' ArrayList.
     */
     class TrieNode {
         private EnumMap<DIGIT, Integer> transitions = new EnumMap<>(DIGIT.class);
         private V value;
         private boolean isLeaf;

        public TrieNode() {

        }

        public boolean isLeaf() {
            return isLeaf;
        }

        public V value() {
            return this.value;
        }

        public V putValue(V value) {
            return this.value = value;
        }

        public void setLeaf(boolean isLeaf) {
           this.isLeaf = isLeaf;
        }

        public void addTransition(int digit, int nodeIndex) {
            transitions.put(DIGIT.values()[digit], nodeIndex);
        }

        @Nullable
        public TrieNode goToState(int digit) {
            Integer index = transitions.get(digit);
            return index == null ? null : nodes.get(index);
        }
    }

    @Override
    public int size() {
        // ROOT_NODE is an internal node, doesn't count.
        return nodes.size() + (NULL_KEY_NODE.isLeaf ? 1 : 0);
    }

    @Override
    public boolean isEmpty() {
         // ROOT_NODE is an internal node, doesn't count.
         return nodes.isEmpty() && NULL_KEY_NODE.isLeaf == false;
    }

    @Override
    public void clear() {
        nodes.clear();
        ROOT_NODE.transitions.entrySet().forEach(entry -> entry.setValue(null));
        NULL_KEY_NODE.isLeaf = false;
    }

    /**
     * Traverses trie and returns a node representing the number or null
     * if trie doesn't contain the number. It's internal API as it's common
     * base for get/remove/contains operations.
     *
     * @param key is intentionally primitive as traversal involves
     * division and modulo operations and we don't want to do them on Immutable
     * classes to avoid multiple allocations.
     */
    @Nullable
    private TrieNode findNodeForNumber(long key) {
        TrieNode currentNode = ROOT_NODE;
        // Need at least one iteration when key == 0
        do {
            int lastDigit = (int) key % 10;
            currentNode = currentNode.goToState(lastDigit);
            key = key / 10;
        }
        while (key > 0 && currentNode != null);
        return currentNode;
    }

    @Override
    public boolean containsKey(Object key) {
        if (key == null) {
            return NULL_KEY_NODE.isLeaf();
        }
        TrieNode keyNode = findNodeForNumber((long) key);
        return keyNode == null ? false : keyNode.isLeaf();
    }

    @Override
    public V get(Object key) {
        if (key == null) {
            return NULL_KEY_NODE.isLeaf() ? NULL_KEY_NODE.value() : null;
        }
        TrieNode keyNode = findNodeForNumber((long) key);
        return keyNode == null ? null : (keyNode.isLeaf() ? keyNode.value() : null);
    }

    @Override
    public V put(K key, V value) {
        if (key == null) {
            V oldVal = NULL_KEY_NODE.isLeaf() ? NULL_KEY_NODE.value() : null;
            NULL_KEY_NODE.putValue(value);
            return oldVal;
        }

        long primitiveKey = (long) key;
        TrieNode currentNode = ROOT_NODE;
        // Need at least one iteration when key == 0
        do {
            int lastDigit = (int) primitiveKey % 10;
            TrieNode nextNode = currentNode.goToState(lastDigit);
            if (nextNode == null) {
                nextNode = new TrieNode();
                nodes.add(nextNode);
                currentNode.addTransition(lastDigit, nodes.size() - 1);
            }
            primitiveKey = primitiveKey / 10;
            currentNode = nextNode;
        }
        while (primitiveKey > 0 && currentNode != null);

        V oldVal = currentNode == null ? null : (currentNode.isLeaf() ? currentNode.value() : null);
        currentNode.setLeaf(true);
        currentNode.putValue(value);
        return oldVal;
    }

    /**
     * Only removes value and resets leaf flag but TrieNode itself is not deleted as it's expensive operation.
     * A parent node will have a reference to the node in it's transitions but it's fine as long as we reset isLeaf flag.
     */
    @Override
    public V remove(Object key) {
        if (key == null) {
            V oldVal = NULL_KEY_NODE.isLeaf() ? NULL_KEY_NODE.value() : null;
            NULL_KEY_NODE.setLeaf(false);
            return oldVal;
        }

        TrieNode keyNode = findNodeForNumber((long) key);
        V oldVal = keyNode == null ? null : (keyNode.isLeaf() ? keyNode.value() : null);
        keyNode.putValue(null);
        keyNode.setLeaf(false);
        return oldVal;
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("containsValue is not supported on NumericTrieMap");
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("putAll is not supported on NumericTrieMap");
    }

    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException("keySet is not supported on NumericTrieMap");
    }

    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException("values is not supported on NumericTrieMap");
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException("entrySet is not supported on NumericTrieMap");
    }
}
