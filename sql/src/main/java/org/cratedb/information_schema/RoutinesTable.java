package org.cratedb.information_schema;


import com.google.common.collect.ImmutableMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.cratedb.action.sql.analyzer.AnalyzerService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Map;

/**
 * virtual information_schema table ``routines`` listing builtin analyzers, tokenizers,
 * token_filters and char_filters for usage in ``CREATE ANALYZER`` statements
 * and custom analyzers created by ``CREATE ANALYZER`` statements.
 */
public class RoutinesTable extends AbstractInformationSchemaTable {

    public static final String NAME = "routines";

    public static class RoutineTypes {
        public static final String ANALYZER = "ANALYZER";
        public static final String TOKENIZER = "TOKENIZER";
        public static final String TOKEN_FILTER = "TOKEN_FILTER";
        public static final String CHAR_FILTER = "CHAR_FILTER";
    }

    public static class Columns {
        public static final String ROUTINE_NAME = "routine_name";
        public static final String ROUTINE_TYPE = "routine_type";
        public static final String ROUTINE_DEFINITION = "routine_definition";
    }
    public static final String BUILTIN = "BUILTIN";

    private ImmutableMap<String, InformationSchemaColumn> fieldMapper = new ImmutableMap
            .Builder<String, InformationSchemaColumn>()
            .put(Columns.ROUTINE_NAME, new InformationSchemaStringColumn(Columns.ROUTINE_NAME))
            .put(Columns.ROUTINE_TYPE, new InformationSchemaStringColumn(Columns.ROUTINE_TYPE))
            .put(Columns.ROUTINE_DEFINITION, new InformationSchemaStringColumn(Columns.ROUTINE_DEFINITION))
            .build();

    private final AnalyzerService analyzerService;

    StringField routineNameField = new StringField(Columns.ROUTINE_NAME, "", Field.Store.YES);
    StringField routineTypeField = new StringField(Columns.ROUTINE_TYPE, "", Field.Store.YES);
    StringField routineDefinitionField = new StringField(Columns.ROUTINE_DEFINITION, "",
            Field.Store.YES);

    @Inject
    public RoutinesTable(AnalyzerService analyzerService) {
        super();
        this.analyzerService = analyzerService;
    }

    @Override
    public void doIndex(ClusterState clusterState) throws IOException {
        for (String builtinAnalyzer : this.analyzerService.getBuiltInAnalyzers()) {
            Document doc = new Document();
            routineNameField.setStringValue(builtinAnalyzer);
            doc.add(routineNameField);

            routineTypeField.setStringValue(RoutineTypes.ANALYZER);
            doc.add(routineTypeField);

            routineDefinitionField.setStringValue(BUILTIN);
            doc.add(routineDefinitionField);

            indexWriter.addDocument(doc);
        }
        for (String builtinTokenizer : this.analyzerService.getBuiltInTokenizers()) {
            Document doc = new Document();
            routineNameField.setStringValue(builtinTokenizer);
            doc.add(routineNameField);

            routineTypeField.setStringValue(RoutineTypes.TOKENIZER);
            doc.add(routineTypeField);

            routineDefinitionField.setStringValue(BUILTIN);
            doc.add(routineDefinitionField);

            indexWriter.addDocument(doc);
        }
        for (String builtinTokenFilter : this.analyzerService.getBuiltInTokenFilters()) {
            Document doc = new Document();
            routineNameField.setStringValue(builtinTokenFilter);
            doc.add(routineNameField);

            routineTypeField.setStringValue(RoutineTypes.TOKEN_FILTER);
            doc.add(routineTypeField);

            routineDefinitionField.setStringValue(BUILTIN);
            doc.add(routineDefinitionField);

            indexWriter.addDocument(doc);
        }
        for (String builtinCharFilter : this.analyzerService.getBuiltInCharFilters()) {
            Document doc = new Document();
            routineNameField.setStringValue(builtinCharFilter);
            doc.add(routineNameField);

            routineTypeField.setStringValue(RoutineTypes.CHAR_FILTER);
            doc.add(routineTypeField);

            routineDefinitionField.setStringValue(BUILTIN);
            doc.add(routineDefinitionField);

            indexWriter.addDocument(doc);
        }

        for (Map.Entry<String, Settings> entry : this.analyzerService.getCustomAnalyzers()
                .entrySet()) {
            Document doc = new Document();

            routineNameField.setStringValue(entry.getKey());
            doc.add(routineNameField);

            routineTypeField.setStringValue(RoutineTypes.ANALYZER);
            doc.add(routineTypeField);

            String source = this.analyzerService.getCustomAnalyzerSource(entry.getKey());
            routineDefinitionField.setStringValue(source);
            doc.add(routineDefinitionField);

            indexWriter.addDocument(doc);
        }
    }

    @Override
    public Iterable<String> cols() {
        return fieldMapper().keySet();
    }

    @Override
    public ImmutableMap<String, InformationSchemaColumn> fieldMapper() {
        return fieldMapper;
    }
}
