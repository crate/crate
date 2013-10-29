package org.cratedb.sql.parser.parser;

import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;

public class AnalyzerElements extends QueryTreeNode {

    NamedNodeWithOptionalProperties tokenizer = null;
    GenericProperties properties = null;
    TokenFilterList tokenFilters = new TokenFilterList();
    CharFilterList charFilters = new CharFilterList();

    public void init(Object genericProperties, Object tokenFilterList, Object charFilterList) {
        properties = (GenericProperties) genericProperties;
        tokenFilters = (TokenFilterList)tokenFilterList;
        charFilters = (CharFilterList)charFilterList;
    }

    public void setTokenizer(NamedNodeWithOptionalProperties tokenizerNode) {
        if (this.tokenizer != null) {
            throw new SQLParseException("Double tokenizer");
        }
        this.tokenizer = tokenizerNode;
    }

    public NamedNodeWithOptionalProperties getTokenizer() {
        return tokenizer;
    }

    public GenericProperties getProperties() {
        return this.properties;
    }

    public CharFilterList getCharFilters() {
        return this.charFilters;
    }

    public TokenFilterList getTokenFilters() {
        return this.tokenFilters;
    }
    
    public void addTokenFilter(NamedNodeWithOptionalProperties tokenFilterNode) {
        this.tokenFilters.add(tokenFilterNode);
    }

    public void addCharFilter(NamedNodeWithOptionalProperties charFilterNode) {
        this.charFilters.add(charFilterNode);
    }

    @Override
    public void copyFrom(QueryTreeNode other) throws StandardException {
        super.copyFrom(other);
        AnalyzerElements elements = (AnalyzerElements) other;
        tokenizer = (NamedNodeWithOptionalProperties)getNodeFactory().copyNode(elements.getTokenizer(), getParserContext());

        properties = (GenericProperties)getNodeFactory().copyNode(elements.getProperties(), getParserContext());

        tokenFilters = new TokenFilterList();
        for (NamedNodeWithOptionalProperties node : elements.getTokenFilters()) {
            tokenFilters.add((NamedNodeWithOptionalProperties)getNodeFactory().copyNode(node, getParserContext()));
        }

        charFilters = new CharFilterList();
        for (NamedNodeWithOptionalProperties node : elements.getCharFilters()) {
            charFilters.add((NamedNodeWithOptionalProperties)getNodeFactory().copyNode(node, getParserContext()));
        }
    }

    @Override
    public void printSubNodes(int depth) {
        super.printSubNodes(depth);
        if (tokenizer != null) {
            printLabel(depth, "Tokenizer: ");
            tokenizer.treePrint(depth + 1);
        }

        printLabel(depth, "Properties: ");
        properties.treePrint(depth + 1);

        printLabel(depth, "TokenFilters: ");
        tokenFilters.treePrint(depth + 1);

        printLabel(depth, "CharFilters: ");
        charFilters.treePrint(depth + 1);

    }

    @Override
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        if (tokenizer != null) {
            tokenizer.accept(v);
        }
        properties.accept(v);
        tokenFilters.accept(v);
        charFilters.accept(v);
    }


}
