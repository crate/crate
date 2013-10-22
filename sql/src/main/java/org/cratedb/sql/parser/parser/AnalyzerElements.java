package org.cratedb.sql.parser.parser;

import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;

public class AnalyzerElements extends QueryTreeNode {

    TokenizerNode tokenizer = null;
    TokenFilterList tokenFilters = new TokenFilterList();
    CharFilterList charFilters = new CharFilterList();

    public void init(Object tokenFilterList, Object charFilterList) {
        tokenFilters = (TokenFilterList)tokenFilterList;
        charFilters = (CharFilterList)charFilterList;
    }

    public void setTokenizer(TokenizerNode tokenizerNode) {
        if (this.tokenizer != null) {
            throw new SQLParseException("Double tokenizer");
        }
        this.tokenizer = tokenizerNode;
    }

    public void addTokenFilter(TokenFilterNode tokenFilterNode) {
        this.tokenFilters.add(tokenFilterNode);
    }

    public void addCharFilter(CharFilterNode charFilterNode) {
        this.charFilters.add(charFilterNode);
    }

    public TokenizerNode getTokenizer() {
        return tokenizer;
    }

    public Iterable<CharFilterNode> getCharFilters() {
        return this.charFilters;
    }

    public Iterable<TokenFilterNode> getTokenFilters() {
        return this.tokenFilters;
    }

    @Override
    public void copyFrom(QueryTreeNode other) throws StandardException {
        super.copyFrom(other);
        AnalyzerElements elements = (AnalyzerElements) other;
        tokenizer = (TokenizerNode)getNodeFactory().copyNode(elements.getTokenizer(), getParserContext());

        tokenFilters = new TokenFilterList();
        for (TokenFilterNode node : elements.getTokenFilters()) {
            tokenFilters.add((TokenFilterNode)getNodeFactory().copyNode(node, getParserContext()));
        }

        charFilters = new CharFilterList();
        for (CharFilterNode node : elements.getCharFilters()) {
            charFilters.add((CharFilterNode)getNodeFactory().copyNode(node, getParserContext()));
        }
    }

    @Override
    public void printSubNodes(int depth) {
        super.printSubNodes(depth);
        if (tokenizer != null) {
            printLabel(depth, "Tokenizer:\n");
            tokenizer.treePrint(depth + 1);
        }
        printLabel(depth, "TokenFilters:\n");
        tokenFilters.treePrint(depth + 1);

        printLabel(depth, "CharFilters:\n");
        charFilters.treePrint(depth + 1);

    }

    @Override
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        if (tokenizer != null) {
            tokenizer.accept(v);
        }
        tokenFilters.accept(v);
        charFilters.accept(v);
    }


}
