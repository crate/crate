package org.cratedb.action.parser;

import com.google.common.collect.ImmutableMap;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.service.SQLService;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Parsing CREATE ANALYZER Statements.
 */
public class AnalyzerVisitor extends XContentVisitor {

    private boolean stopTraverse = false;

    private boolean doneParsing = true;
    private String analyzerName = null;
    private String extendedAnalyzerName = null;

    private Tuple<String, Settings> tokenizerDefinition = null;
    private List<Tuple<String,Settings>> charFilters = new ArrayList<>();
    private List<Tuple<String, Settings>> tokenFilters = new ArrayList<>();


    @Override
    public XContentBuilder getXContentBuilder() throws StandardException {
        throw new UnsupportedOperationException();
    }

    public AnalyzerVisitor(ParsedStatement parsedStatement) throws StandardException {
        super(parsedStatement);
        stopTraverse = false;
    }

    public Visitable visit(CreateAnalyzerNode node) throws StandardException {
        analyzerName = node.getObjectName().getTableName(); // extend to use schema here
        if (analyzerName.equals("default")) {
            throw new SQLParseException("Overriding the default analyzer is forbidden");
        }
        if (stmt.context().analyzerService().hasBuiltInAnalyzer(analyzerName)) {
            throw new SQLParseException(String.format("Cannot override builtin analyzer '%s'", analyzerName));
        }
        if (node.getExtendsName() != null) {
            visit(node.getExtendsName());
        }
        visit(node.getElements());
        doneParsing = true;
        return node;
    }

    public Visitable visit(TableName extendsName) throws StandardException {
        String extended = extendsName.getTableName();
        if (!stmt.context().analyzerService().hasAnalyzer(extended)) {
            throw new SQLParseException(String.format("Extended Analyzer '%s' does not exist", extended));
        }
        extendedAnalyzerName = extended;
        return extendsName;
    }

    public Visitable visit(AnalyzerElements elements) throws StandardException {
        if (elements.getTokenizer() != null) {
            visit(elements.getTokenizer());
        }
        visit(elements.getTokenFilters());
        visit(elements.getCharFilters());
        // done :)
        return elements;
    }

    /**
     * evaluate tokenizer-definition and check for correctness
     * @param tokenizer
     * @return
     * @throws StandardException
     */
    public Visitable visit(NamedNodeWithOptionalProperties tokenizer) throws StandardException {

        String name = tokenizer.getName();
        GenericProperties properties = tokenizer.getProperties();

        // use a builtin tokenizer without parameters
        if (properties == null) {
            // validate
            if (!stmt.context().analyzerService().hasBuiltInTokenizer(name)) {
                throw new SQLParseException(String.format("Non-existing built-in tokenizer '%s'", name));
            }
            // build
            tokenizerDefinition = new Tuple<>(name, ImmutableSettings.EMPTY);
        } else {
            // validate
            if (!stmt.context().analyzerService().hasBuiltInTokenFilter(name)) {
                // type mandatory
                String evaluatedType = extractType(properties);
                if (!stmt.context().analyzerService().hasTokenFilter(evaluatedType)) {
                    throw new SQLParseException(String.format("Non-existing tokenizer type '%s'", evaluatedType));
                }
            } else {
                throw new SQLParseException(String.format("tokenizer name '%s' is reserved", name));
            }

            // build
            ImmutableSettings.Builder builder = ImmutableSettings.builder();
            for (Map.Entry<String, QueryTreeNode> tokenizerProperty : properties.iterator()) {
                genericPropertyToSetting(builder,
                        getSettingsKey("index.analysis.tokenizer.%s.%s", name, tokenizerProperty.getKey()),
                        tokenizerProperty.getValue());
            }
            tokenizerDefinition = new Tuple<>(name, builder.build());
        }

        return tokenizer;
    }

    public Visitable visit(TokenFilterList tokenFilterList) throws StandardException {
        for (TokenFilterNode tokenFilterNode : tokenFilterList) {

            String name = tokenFilterNode.getName();
            GenericProperties properties = tokenFilterNode.getProperties();

            // use a builtin tokenfilter without parameters
            if (properties == null) {
                // validate
                if (!stmt.context().analyzerService().hasBuiltInTokenFilter(name)) {
                    throw new SQLParseException(String.format("Non-existing built-in token-filter '%s'", name));
                }
                // build
                tokenFilters.add(new Tuple<>(name, ImmutableSettings.EMPTY));
            } else {
                // validate
                if (!stmt.context().analyzerService().hasBuiltInTokenFilter(name)) {
                    // type mandatory when name is not a builtin filter
                    String evaluatedType = extractType(properties);
                    if (!stmt.context().analyzerService().hasTokenFilter(evaluatedType)) {
                        throw new SQLParseException(String.format("Non-existing token-filter type '%s'", evaluatedType));
                    }
                } else {
                    if (properties.get("type") != null) {
                        throw new SQLParseException(String.format("token-filter name '%s' is reserved, 'type' property forbidden here", name));
                    }
                }

                // build
                ImmutableSettings.Builder builder = ImmutableSettings.builder();
                for (Map.Entry<String, QueryTreeNode> tokenFilterProperty : properties.iterator()) {
                    genericPropertyToSetting(builder,
                            getSettingsKey("index.analysis.filter.%s.%s", name, tokenFilterProperty.getKey()),
                            tokenFilterProperty.getValue());
                }
                tokenFilters.add(new Tuple<>(name, builder.build()));
            }
        }
        return tokenFilterList;
    }


    public Visitable visit(CharFilterList charFilterList) throws StandardException {
        for (CharFilterNode charFilterNode : charFilterList) {

            String name = charFilterNode.getName();
            GenericProperties properties = charFilterNode.getProperties();

            // use a builtin tokenfilter without parameters
            if (properties == null) {
                // validate
                if (!stmt.context().analyzerService().hasBuiltInCharFilter(name)) {
                    throw new SQLParseException(String.format("Non-existing built-in char-filter '%s'", name));
                }
                // build
                charFilters.add(new Tuple<>(name, ImmutableSettings.EMPTY));
            } else {
                String type = extractType(properties);
                if (!stmt.context().analyzerService().hasCharFilter(type)) {
                    throw new SQLParseException(String.format("Non-existing char-filter type '%s'", type));
                }

                // build
                ImmutableSettings.Builder builder = ImmutableSettings.builder();
                for (Map.Entry<String, QueryTreeNode> charFilterProperty: properties.iterator()) {
                    genericPropertyToSetting(builder,
                            getSettingsKey("index.analysis.char_filter.%s.%s", name, charFilterProperty.getKey()),
                            charFilterProperty.getValue());
                }
                charFilters.add(new Tuple<>(name, builder.build()));
            }
        }
        return charFilterList;
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        QueryTreeNode treeNode = (QueryTreeNode)node;
        switch (treeNode.getNodeType()) {
            case NodeTypes.CREATE_ANALYZER_NODE:
                stopTraverse = true;
                return visit((CreateAnalyzerNode)node);
            default:
                return node;
        }
    }

    /**
     * validate Properties
     */
    private String extractType(GenericProperties properties) throws StandardException {
        // validate
        QueryTreeNode typeNode = properties.get("type");
        if (typeNode == null) {
            throw new SQLParseException("'type' property missing");
        }

        if (!(typeNode instanceof ValueNode)) {
            throw new SQLParseException("'type' property invalid");
        }
        return (String)evaluateValueNode(null, (ValueNode)typeNode, false);
    }

    /**
     * put a genericProperty into a settings-structure, hide the details of handling valuenodes and lists thereof
     * @param builder
     * @param name
     * @param value
     * @throws StandardException
     */
    private void genericPropertyToSetting(ImmutableSettings.Builder builder, String name, QueryTreeNode value) throws StandardException {
        if (value instanceof ValueNode) {
            builder.put(name, evaluateValueNode(null, (ValueNode) value, false));
        } else if (value instanceof ValueNodeList) {
            ValueNodeList valueNodeList = (ValueNodeList)value;
            List<String> values = new ArrayList<>(valueNodeList.size());
            for (ValueNode node : valueNodeList) {
                values.add(evaluateValueNode(null, node, false).toString());
            }
            builder.putArray(name, values.toArray(new String[values.size()]));
        }
    }

    public static String getSettingsKey(String suffix, Object ... formatArgs) {
        if (formatArgs != null) {
            suffix = String.format(suffix, formatArgs);
        }

        return suffix; //String.format("%s.%s", SQLService.CUSTOM_ANALYZER_SETTINGS_PREFIX, suffix);
    }

    public Settings analyzerSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        builder.put(
                getSettingsKey("index.analysis.analyzer.%s.type", analyzerName),
                (extendedAnalyzerName == null ? "custom" : extendedAnalyzerName)
        );
        if (tokenizerDefinition != null) {
            builder.put(
                    getSettingsKey("index.analysis.analyzer.%s.tokenizer", analyzerName),
                    tokenizerDefinition.v1()
            );
        }
        if (charFilters.size() > 0) {
            String[] charFilterNames = new String[charFilters.size()];
            for (int i=0; i<charFilters.size(); i++) {
                charFilterNames[i] = charFilters.get(i).v1();
            }
            builder.putArray(
                    getSettingsKey("index.analysis.analyzer.%s.char_filter", analyzerName),
                    charFilterNames
            );
        }
        if (tokenFilters.size() > 0) {
            String[] tokenFilterNames = new String[tokenFilters.size()];
            for (int i = 0; i<tokenFilters.size(); i++) {
                tokenFilterNames[i] = tokenFilters.get(i).v1();
            }
            builder.putArray(
                    getSettingsKey("index.analysis.analyzer.%s.filter", analyzerName),
                    tokenFilterNames
            );
        }
        return builder.build();
    }

    /**
     * build settings ready for putting into clusterstate
     * @return the analyzer settings corresponding to the analyzed <tt>CREATE ANALYZER</tt> statement
     * @throws SettingsException in case we can't build the settings yet
     */
    public Settings buildSettings() throws SettingsException, IOException {
        if (!doneParsing) {
            throw new SettingsException("cannot build settings for analyzer yet");
        }
        // create analyzer settings - possibly referencing charFilters, tokenFilters, tokenizers defined here
        ImmutableSettings.Builder builder = ImmutableSettings.builder();

        String encodedAnalyzerSettings = settingsToXContent(analyzerSettings()).toUtf8();
        builder.put(
                String.format("%s.analyzer.%s", SQLService.CUSTOM_ANALYZER_SETTINGS_PREFIX, analyzerName),
                encodedAnalyzerSettings
        );

        if (tokenizerDefinition != null) {
            builder.put(
                    String.format("%s.tokenizer.%s", SQLService.CUSTOM_ANALYZER_SETTINGS_PREFIX, tokenizerDefinition.v1()),
                    settingsToXContent(tokenizerDefinition.v2()).toUtf8()
            );
        }
        for (Tuple<String, Settings> tokenFilterDefinition: tokenFilters) {
            builder.put(
                    String.format("%s.filter.%s", SQLService.CUSTOM_ANALYZER_SETTINGS_PREFIX, tokenFilterDefinition.v1()),
                    settingsToXContent(tokenFilterDefinition.v2()).toUtf8()
            );
        }
        for (Tuple<String, Settings> charFilterDefinition : charFilters) {
            builder.put(
                    String.format("%s.char_filter.%s", SQLService.CUSTOM_ANALYZER_SETTINGS_PREFIX, charFilterDefinition.v1()),
                    settingsToXContent(charFilterDefinition.v2()).toUtf8()
            );
        }
        return builder.build();
    }

    public static BytesReference settingsToXContent(Settings settings) throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        for (ImmutableMap.Entry<String, String> entry : settings.getAsMap().entrySet()) {
            xContentBuilder.field(entry.getKey(), entry.getValue());
        }
        return xContentBuilder.endObject().bytes();
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return stopTraverse;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }
}
