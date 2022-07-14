/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.array.DynamicArrayFieldMapperBuilderFactory;

import io.crate.common.collections.Tuple;

/** A parser for documents, given mappings from a DocumentMapper */
final class DocumentParser {

    private final IndexSettings indexSettings;
    private final DocumentMapperParser docMapperParser;
    private final DocumentMapper docMapper;

    DocumentParser(IndexSettings indexSettings, DocumentMapperParser docMapperParser, DocumentMapper docMapper) {
        this.indexSettings = indexSettings;
        this.docMapperParser = docMapperParser;
        this.docMapper = docMapper;
    }

    ParsedDocument parseDocument(SourceToParse source, MetadataFieldMapper[] metadataFieldsMappers) throws MapperParsingException {
        validateType();

        final Mapping mapping = docMapper.mapping();
        final ParseContext.InternalParseContext context;
        final XContentType xContentType = source.getXContentType();

        try (XContentParser parser = XContentHelper.createParser(docMapperParser.getXContentRegistry(),
            LoggingDeprecationHandler.INSTANCE, source.source(), xContentType)) {
            context = new ParseContext.InternalParseContext(indexSettings, docMapperParser, docMapper, source, parser);
            validateStart(parser);
            internalParseDocument(mapping, metadataFieldsMappers, context, parser);
            validateEnd(parser);
        } catch (Exception e) {
            throw wrapInMapperParsingException(source, e);
        }
        String remainingPath = context.path().pathAsText("");
        if (remainingPath.isEmpty() == false) {
            throw new IllegalStateException("found leftover path elements: " + remainingPath);
        }

        return new ParsedDocument(
            context.version(),
            context.seqID(),
            context.sourceToParse().id(),
            context.docs(),
            context.sourceToParse().source(),
            createDynamicUpdate(mapping, docMapper, context.getDynamicMappers())
        );
    }

    private static void internalParseDocument(Mapping mapping, MetadataFieldMapper[] metadataFieldsMappers,
                                              ParseContext.InternalParseContext context, XContentParser parser) throws IOException {
        final boolean emptyDoc = isEmptyDoc(mapping, parser);

        for (MetadataFieldMapper metadataMapper : metadataFieldsMappers) {
            metadataMapper.preParse(context);
        }

        if (emptyDoc == false) {
            parseObjectOrNested(context, mapping.root);
        }

        for (MetadataFieldMapper metadataMapper : metadataFieldsMappers) {
            metadataMapper.postParse(context);
        }
    }

    private void validateType() {
        if (!docMapper.type().equals("default")) {
            throw new MapperParsingException("DocumentMapper type must be `default`. We don't allow other types");
        }
    }

    private static void validateStart(XContentParser parser) throws IOException {
        // will result in START_OBJECT
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new MapperParsingException("Malformed content, must start with an object");
        }
    }

    private static void validateEnd(XContentParser parser) throws IOException {
        XContentParser.Token token;// only check for end of tokens if we created the parser here
        // try to parse the next token, this should be null if the object is ended properly
        // but will throw a JSON exception if the extra tokens is not valid JSON (this will be handled by the catch)
        token = parser.nextToken();
        if (token != null) {
            throw new IllegalArgumentException("Malformed content, found extra data after parsing: " + token);
        }
    }

    private static boolean isEmptyDoc(Mapping mapping, XContentParser parser) throws IOException {
        final XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.END_OBJECT) {
            // empty doc, we can handle it...
            return true;
        } else if (token != XContentParser.Token.FIELD_NAME) {
            throw new MapperParsingException("Malformed content, after first object, either the type field or the actual properties should exist");
        }
        return false;
    }


    private static MapperParsingException wrapInMapperParsingException(SourceToParse source, Exception e) {
        // if its already a mapper parsing exception, no need to wrap it...
        if (e instanceof MapperParsingException) {
            return (MapperParsingException) e;
        }

        // Throw a more meaningful message if the document is empty.
        if (source.source() != null && source.source().length() == 0) {
            return new MapperParsingException("failed to parse, document is empty");
        }

        return new MapperParsingException("failed to parse", e);
    }

    private static String[] splitAndValidatePath(String fullFieldPath) {
        if (fullFieldPath.contains(".")) {
            String[] parts = fullFieldPath.split("\\.");
            for (String part : parts) {
                if (Strings.hasText(part) == false) {
                    // check if the field name contains only whitespace
                    if (Strings.isEmpty(part) == false) {
                        throw new IllegalArgumentException(
                                "object field cannot contain only whitespace: ['" + fullFieldPath + "']");
                    }
                    throw new IllegalArgumentException(
                            "object field starting or ending with a [.] makes object resolution ambiguous: [" + fullFieldPath + "]");
                }
            }
            return parts;
        } else {
            if (Strings.isEmpty(fullFieldPath)) {
                throw new IllegalArgumentException("field name cannot be an empty string");
            }
            return new String[] {fullFieldPath};
        }
    }

    /** Creates a Mapping containing any dynamically added fields, or returns null if there were no dynamic mappings. */
    @Nullable
    static Mapping createDynamicUpdate(Mapping mapping, DocumentMapper docMapper, List<Mapper> dynamicMappers) {
        if (dynamicMappers.isEmpty()) {
            return null;
        }
        // We build a mapping by first sorting the mappers, so that all mappers containing a common prefix
        // will be processed in a contiguous block. When the prefix is no longer seen, we pop the extra elements
        // off the stack, merging them upwards into the existing mappers.
        dynamicMappers.sort(Comparator.comparing(Mapper::name));
        Iterator<Mapper> dynamicMapperItr = dynamicMappers.iterator();
        List<ObjectMapper> parentMappers = new ArrayList<>();
        Mapper firstUpdate = dynamicMapperItr.next();
        parentMappers.add(createUpdate(mapping.root(), splitAndValidatePath(firstUpdate.name()), 0, firstUpdate));
        Mapper previousMapper = null;
        while (dynamicMapperItr.hasNext()) {
            Mapper newMapper = dynamicMapperItr.next();
            if (previousMapper != null && newMapper.name().equals(previousMapper.name())) {
                // We can see the same mapper more than once, for example, if we had foo.bar and foo.baz, where
                // foo did not yet exist. This will create 2 copies in dynamic mappings, which should be identical.
                // Here we just skip over the duplicates, but we merge them to ensure there are no conflicts.
                newMapper.merge(previousMapper);
                continue;
            }
            previousMapper = newMapper;
            String[] nameParts = splitAndValidatePath(newMapper.name());

            // We first need the stack to only contain mappers in common with the previously processed mapper
            // For example, if the first mapper processed was a.b.c, and we now have a.d, the stack will contain
            // a.b, and we want to merge b back into the stack so it just contains a
            int i = removeUncommonMappers(parentMappers, nameParts);

            // Then we need to add back mappers that may already exist within the stack, but are not on it.
            // For example, if we processed a.b, followed by an object mapper a.c.d, and now are adding a.c.d.e
            // then the stack will only have a on it because we will have already merged a.c.d into the stack.
            // So we need to pull a.c, followed by a.c.d, onto the stack so e can be added to the end.
            i = expandCommonMappers(parentMappers, nameParts, i);

            // If there are still parents of the new mapper which are not on the stack, we need to pull them
            // from the existing mappings. In order to maintain the invariant that the stack only contains
            // fields which are updated, we cannot simply add the existing mappers to the stack, since they
            // may have other subfields which will not be updated. Instead, we pull the mapper from the existing
            // mappings, and build an update with only the new mapper and its parents. This then becomes our
            // "new mapper", and can be added to the stack.
            if (i < nameParts.length - 1) {
                newMapper = createExistingMapperUpdate(parentMappers, nameParts, i, docMapper, newMapper);
            }

            if (newMapper instanceof ObjectMapper) {
                parentMappers.add((ObjectMapper)newMapper);
            } else {
                addToLastMapper(parentMappers, newMapper, true);
            }
        }
        popMappers(parentMappers, 1, true);
        assert parentMappers.size() == 1;

        return mapping.mappingUpdate(parentMappers.get(0));
    }

    private static void popMappers(List<ObjectMapper> parentMappers, int keepBefore, boolean merge) {
        assert keepBefore >= 1; // never remove the root mapper
        // pop off parent mappers not needed by the current mapper,
        // merging them backwards since they are immutable
        for (int i = parentMappers.size() - 1; i >= keepBefore; --i) {
            addToLastMapper(parentMappers, parentMappers.remove(i), merge);
        }
    }

    /**
     * Adds a mapper as an update into the last mapper. If merge is true, the new mapper
     * will be merged in with other child mappers of the last parent, otherwise it will be a new update.
     */
    private static void addToLastMapper(List<ObjectMapper> parentMappers, Mapper mapper, boolean merge) {
        assert parentMappers.size() >= 1;
        int lastIndex = parentMappers.size() - 1;
        ObjectMapper withNewMapper = parentMappers.get(lastIndex).mappingUpdate(mapper);
        if (merge) {
            withNewMapper = parentMappers.get(lastIndex).merge(withNewMapper);
        }
        parentMappers.set(lastIndex, withNewMapper);
    }

    /**
     * Removes mappers that exist on the stack, but are not part of the path of the current nameParts,
     * Returns the next unprocessed index from nameParts.
     */
    private static int removeUncommonMappers(List<ObjectMapper> parentMappers, String[] nameParts) {
        int keepBefore = 1;
        while (keepBefore < parentMappers.size() &&
            parentMappers.get(keepBefore).simpleName().equals(nameParts[keepBefore - 1])) {
            ++keepBefore;
        }
        popMappers(parentMappers, keepBefore, true);
        return keepBefore - 1;
    }

    /**
     * Adds mappers from the end of the stack that exist as updates within those mappers.
     * Returns the next unprocessed index from nameParts.
     */
    private static int expandCommonMappers(List<ObjectMapper> parentMappers, String[] nameParts, int i) {
        ObjectMapper last = parentMappers.get(parentMappers.size() - 1);
        while (i < nameParts.length - 1 && last.getMapper(nameParts[i]) != null) {
            Mapper newLast = last.getMapper(nameParts[i]);
            assert newLast instanceof ObjectMapper;
            last = (ObjectMapper) newLast;
            parentMappers.add(last);
            ++i;
        }
        return i;
    }

    /** Creates an update for intermediate object mappers that are not on the stack, but parents of newMapper. */
    private static ObjectMapper createExistingMapperUpdate(List<ObjectMapper> parentMappers, String[] nameParts, int i,
                                                           DocumentMapper docMapper, Mapper newMapper) {
        String updateParentName = nameParts[i];
        final ObjectMapper lastParent = parentMappers.get(parentMappers.size() - 1);
        if (parentMappers.size() > 1) {
            // only prefix with parent mapper if the parent mapper isn't the root (which has a fake name)
            updateParentName = lastParent.name() + '.' + nameParts[i];
        }
        ObjectMapper updateParent = docMapper.objectMappers().get(updateParentName);
        assert updateParent != null : updateParentName + " doesn't exist";
        return createUpdate(updateParent, nameParts, i + 1, newMapper);
    }

    /** Build an update for the parent which will contain the given mapper and any intermediate fields. */
    private static ObjectMapper createUpdate(ObjectMapper parent, String[] nameParts, int i, Mapper mapper) {
        List<ObjectMapper> parentMappers = new ArrayList<>();
        ObjectMapper previousIntermediate = parent;
        for (; i < nameParts.length - 1; ++i) {
            Mapper intermediate = previousIntermediate.getMapper(nameParts[i]);
            assert intermediate != null : "Field " + previousIntermediate.name() + " does not have a subfield " + nameParts[i];
            assert intermediate instanceof ObjectMapper;
            parentMappers.add((ObjectMapper)intermediate);
            previousIntermediate = (ObjectMapper)intermediate;
        }
        if (parentMappers.isEmpty() == false) {
            // add the new mapper to the stack, and pop down to the original parent level
            addToLastMapper(parentMappers, mapper, false);
            popMappers(parentMappers, 1, false);
            mapper = parentMappers.get(0);
        }
        return parent.mappingUpdate(mapper);
    }

    static void parseObjectOrNested(ParseContext context, ObjectMapper mapper) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            // the object is null ("obj1" : null), simply bail
            return;
        }

        String currentFieldName = parser.currentName();
        if (token.isValue()) {
            throw new MapperParsingException("object mapping for [" + mapper.name() + "] tried to parse field [" + currentFieldName + "] as object, but found a concrete value");
        }

        // if we are at the end of the previous object, advance
        if (token == XContentParser.Token.END_OBJECT) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            // if we are just starting an OBJECT, advance, this is the object we are parsing, we need the name first
            token = parser.nextToken();
        }

        innerParseObject(context, mapper, parser, currentFieldName, token);
        // restore the enable path flag
    }

    private static void innerParseObject(ParseContext context, ObjectMapper mapper, XContentParser parser, String currentFieldName, XContentParser.Token token) throws IOException {
        assert token == XContentParser.Token.FIELD_NAME || token == XContentParser.Token.END_OBJECT;
        String[] paths = null;
        while (token != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                paths = splitAndValidatePath(currentFieldName);
            } else if (token == XContentParser.Token.START_OBJECT) {
                parseObject(context, mapper, currentFieldName, paths);
            } else if (token == XContentParser.Token.START_ARRAY) {
                parseArray(context, mapper, currentFieldName, paths);
            } else if (token == XContentParser.Token.VALUE_NULL) {
                parseNullValue(context, mapper, currentFieldName, paths);
            } else if (token == null) {
                throw new MapperParsingException("object mapping for [" + mapper.name() + "] tried to parse field [" + currentFieldName + "] as object, but got EOF, has a concrete value been provided to it?");
            } else if (token.isValue()) {
                parseValue(context, mapper, currentFieldName, token, paths);
            }
            token = parser.nextToken();
        }
    }

    static void parseObjectOrField(ParseContext context, Mapper mapper) throws IOException {
        if (mapper instanceof ObjectMapper) {
            parseObjectOrNested(context, (ObjectMapper) mapper);
        } else if (mapper instanceof FieldMapper) {
            FieldMapper fieldMapper = (FieldMapper) mapper;
            fieldMapper.parse(context);
            parseCopyFields(context, fieldMapper.copyTo().copyToFields());
        } else {
            throw new IllegalStateException("The provided mapper [" + mapper.name() + "] has an unrecognized type [" +
                mapper.getClass().getSimpleName() + "].");
        }
    }

    static void parseObject(final ParseContext context, ObjectMapper mapper, String currentFieldName, String[] paths) throws IOException {
        assert currentFieldName != null;

        Mapper objectMapper = getMapper(mapper, currentFieldName, paths);
        if (objectMapper != null) {
            context.path().add(currentFieldName);
            parseObjectOrField(context, objectMapper);
            context.path().remove();
        } else {
            currentFieldName = paths[paths.length - 1];
            Tuple<Integer, ObjectMapper> parentMapperTuple = getDynamicParentMapper(context, paths, mapper);
            ObjectMapper parentMapper = parentMapperTuple.v2();
            ObjectMapper.Dynamic dynamic = dynamicOrDefault(parentMapper, context);
            if (dynamic == ObjectMapper.Dynamic.STRICT) {
                throw new StrictDynamicMappingException(mapper.fullPath(), currentFieldName);
            } else if (dynamic == ObjectMapper.Dynamic.TRUE) {
                Mapper.Builder<?> builder = new ObjectMapper.Builder<>(currentFieldName);
                builder.position(getPositionEstimate(context));
                Mapper.BuilderContext builderContext = new Mapper.BuilderContext(context.indexSettings().getSettings(), context.path());
                objectMapper = builder.build(builderContext);
                context.addDynamicMapper(objectMapper);
                context.path().add(currentFieldName);
                parseObjectOrField(context, objectMapper);
                context.path().remove();
            } else {
                // not dynamic, read everything up to end object
                context.parser().skipChildren();
            }
            for (int i = 0; i < parentMapperTuple.v1(); i++) {
                context.path().remove();
            }
        }
    }

    /**
     * Returns position estimates instead of the exact positions, which will be used for the actual column position calculations
     * by {@link org.elasticsearch.cluster.metadata.ColumnPositionResolver#updatePositions(int)}
     */
    static int getPositionEstimate(ParseContext context) {
        return -1 - context.getDynamicMappers().size();
    }

    private static void parseArray(ParseContext context,
                                   ObjectMapper parentMapper,
                                   String lastFieldName,
                                   String[] paths) throws IOException {
        String arrayFieldName = lastFieldName;

        Mapper mapper = getMapper(parentMapper, lastFieldName, paths);
        if (mapper != null) {
            // There is a concrete mapper for this field already. Need to check if the mapper
            // expects an array, if so we pass the context straight to the mapper and if not
            // we serialize the array components
            if (mapper instanceof ArrayValueMapperParser) {
                if (mapper instanceof ObjectMapper) {
                    parseObject(context, (ObjectMapper) mapper, lastFieldName, paths);
                } else {
                    ((FieldMapper) mapper).parse(context);
                }
            } else {
                parseNonDynamicArray(context, parentMapper, lastFieldName, arrayFieldName);
            }
        } else {
            arrayFieldName = paths[paths.length - 1];
            lastFieldName = arrayFieldName;
            Tuple<Integer, ObjectMapper> parentMapperTuple = getDynamicParentMapper(context, paths, parentMapper);
            parentMapper = parentMapperTuple.v2();
            ObjectMapper.Dynamic dynamic = dynamicOrDefault(parentMapper, context);
            if (dynamic == ObjectMapper.Dynamic.STRICT) {
                throw new StrictDynamicMappingException(parentMapper.fullPath(), arrayFieldName);
            } else if (dynamic == ObjectMapper.Dynamic.TRUE) {
                MapperService mapperService = context.mapperService();
                DynamicArrayFieldMapperBuilderFactory dynamicArrayFieldMapperBuilderFactory =
                    mapperService.mapperRegistry.getDynamicArrayBuilderFactory();

                if (dynamicArrayFieldMapperBuilderFactory != null) {
                    mapper = dynamicArrayFieldMapperBuilderFactory.create(
                            arrayFieldName,
                            parentMapper,
                            context
                    );
                    if (mapper != null) {
                        context.addDynamicMapper(mapper);
                        context.path().add(arrayFieldName);
                        parseObjectOrField(context, mapper);
                        context.path().remove();
                    }
                } else {
                    parseNonDynamicArray(context, parentMapper, lastFieldName, arrayFieldName);
                }
            } else {
                // TODO: shouldn't this skip, not parse?
                parseNonDynamicArray(context, parentMapper, lastFieldName, arrayFieldName);
            }
            for (int i = 0; i < parentMapperTuple.v1(); i++) {
                context.path().remove();
            }
        }
    }

    private static void parseNonDynamicArray(ParseContext context, ObjectMapper mapper, String lastFieldName, String arrayFieldName) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token;
        final String[] paths = splitAndValidatePath(lastFieldName);
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.START_OBJECT) {
                parseObject(context, mapper, lastFieldName, paths);
            } else if (token == XContentParser.Token.START_ARRAY) {
                parseArray(context, mapper, lastFieldName, paths);
            } else if (token == XContentParser.Token.VALUE_NULL) {
                parseNullValue(context, mapper, lastFieldName, paths);
            } else if (token == null) {
                throw new MapperParsingException("object mapping for [" + mapper.name() + "] with array for [" + arrayFieldName + "] tried to parse as array, but got EOF, is there a mismatch in types for the same field?");
            } else {
                assert token.isValue();
                parseValue(context, mapper, lastFieldName, token, paths);
            }
        }
    }

    private static void parseValue(final ParseContext context,
                                   ObjectMapper parentMapper,
                                   String currentFieldName,
                                   XContentParser.Token token,
                                   String[] paths) throws IOException {
        if (currentFieldName == null) {
            throw new MapperParsingException("object mapping [" + parentMapper.name() + "] trying to serialize a value with no field associated with it, current value [" + context.parser().textOrNull() + "]");
        }
        Mapper mapper = getMapper(parentMapper, currentFieldName, paths);
        if (mapper != null) {
            parseObjectOrField(context, mapper);
        } else {
            currentFieldName = paths[paths.length - 1];
            Tuple<Integer, ObjectMapper> parentMapperTuple = getDynamicParentMapper(context, paths, parentMapper);
            parentMapper = parentMapperTuple.v2();
            parseDynamicValue(context, parentMapper, currentFieldName, token);
            for (int i = 0; i < parentMapperTuple.v1(); i++) {
                context.path().remove();
            }
        }
    }

    private static void parseNullValue(ParseContext context, ObjectMapper parentMapper, String lastFieldName,
                                       String[] paths) throws IOException {
        // we can only handle null values if we have mappings for them
        Mapper mapper = getMapper(parentMapper, lastFieldName, paths);
        if (mapper != null) {
            // TODO: passing null to an object seems bogus?
            parseObjectOrField(context, mapper);
        } else if (parentMapper.dynamic() == ObjectMapper.Dynamic.STRICT) {
            throw new StrictDynamicMappingException(parentMapper.fullPath(), lastFieldName);
        }
    }

    private static Mapper.Builder<?> newLongBuilder(String name) {
        return new NumberFieldMapper.Builder(name, NumberFieldMapper.NumberType.LONG);
    }

    private static Mapper.Builder<?> newFloatBuilder(String name) {
        return new NumberFieldMapper.Builder(name, NumberFieldMapper.NumberType.FLOAT);
    }

    static Mapper.Builder<?> createBuilderFromDynamicValue(final ParseContext context, XContentParser.Token token, String currentFieldName) throws IOException {
        if (token == XContentParser.Token.VALUE_STRING) {
            return new KeywordFieldMapper.Builder(currentFieldName);
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            XContentParser.NumberType numberType = context.parser().numberType();
            if (numberType == XContentParser.NumberType.INT || numberType == XContentParser.NumberType.LONG) {
                return newLongBuilder(currentFieldName);
            } else if (numberType == XContentParser.NumberType.FLOAT || numberType == XContentParser.NumberType.DOUBLE) {
                // no templates are defined, we use float by default instead of double
                // since this is much more space-efficient and should be enough most of
                // the time
                return newFloatBuilder(currentFieldName);
            }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            return new BooleanFieldMapper.Builder(currentFieldName);
        } else if (token == XContentParser.Token.START_OBJECT) {
            return new ObjectMapper.Builder(currentFieldName);
        }
        // TODO how do we identify dynamically that its a binary value?
        throw new IllegalStateException("Can't handle serializing a dynamic type with content token [" + token + "] and field name [" + currentFieldName + "]");
    }

    private static void parseDynamicValue(final ParseContext context, ObjectMapper parentMapper, String currentFieldName, XContentParser.Token token) throws IOException {
        ObjectMapper.Dynamic dynamic = dynamicOrDefault(parentMapper, context);
        if (dynamic == ObjectMapper.Dynamic.STRICT) {
            throw new StrictDynamicMappingException(parentMapper.fullPath(), currentFieldName);
        }
        if (dynamic == ObjectMapper.Dynamic.FALSE) {
            return;
        }
        final Mapper.BuilderContext builderContext = new Mapper.BuilderContext(context.indexSettings().getSettings(), context.path());
        final Mapper.Builder<?> builder = createBuilderFromDynamicValue(context, token, currentFieldName);
        builder.position(getPositionEstimate(context));
        Mapper mapper = builder.build(builderContext);
        context.addDynamicMapper(mapper);
        parseObjectOrField(context, mapper);
    }

    /** Creates instances of the fields that the current field should be copied to */
    static void parseCopyFields(ParseContext context, List<String> copyToFields) throws IOException {
        if (!context.isWithinCopyTo() && copyToFields.isEmpty() == false) {
            context = context.createCopyToContext();
            for (String field : copyToFields) {
                // In case of a hierarchy of nested documents, we need to figure out
                // which document the field should go to
                ParseContext.Document targetDoc = null;
                for (ParseContext.Document doc = context.doc(); doc != null; doc = doc.getParent()) {
                    if (field.startsWith(doc.getPrefix())) {
                        targetDoc = doc;
                        break;
                    }
                }
                assert targetDoc != null;
                final ParseContext copyToContext;
                if (targetDoc == context.doc()) {
                    copyToContext = context;
                } else {
                    copyToContext = context.switchDoc(targetDoc);
                }
                parseCopy(field, copyToContext);
            }
        }
    }

    /** Creates an copy of the current field with given field name and boost */
    private static void parseCopy(String field, ParseContext context) throws IOException {
        Mapper mapper = context.docMapper().mappers().getMapper(field);
        if (mapper != null) {
            if (mapper instanceof FieldMapper) {
                ((FieldMapper) mapper).parse(context);
            } else {
                throw new IllegalStateException("The provided mapper [" + mapper.name() +
                    "] has an unrecognized type [" + mapper.getClass().getSimpleName() + "].");
            }
        } else {
            // The path of the dest field might be completely different from the current one so we need to reset it
            context = context.overridePath(new ContentPath(0));

            final String[] paths = splitAndValidatePath(field);
            final String fieldName = paths[paths.length - 1];
            Tuple<Integer, ObjectMapper> parentMapperTuple = getDynamicParentMapper(context, paths, null);
            ObjectMapper objectMapper = parentMapperTuple.v2();
            parseDynamicValue(context, objectMapper, fieldName, context.parser().currentToken());
            for (int i = 0; i < parentMapperTuple.v1(); i++) {
                context.path().remove();
            }
        }
    }

    private static Tuple<Integer, ObjectMapper> getDynamicParentMapper(ParseContext context, final String[] paths,
            ObjectMapper currentParent) {
        ObjectMapper mapper = currentParent == null ? context.root() : currentParent;
        int pathsAdded = 0;
        ObjectMapper parent = mapper;
        for (int i = 0; i < paths.length - 1; i++) {
            String currentPath = context.path().pathAsText(paths[i]);
            Mapper existingFieldMapper = context.docMapper().mappers().getMapper(currentPath);
            if (existingFieldMapper != null) {
                throw new MapperParsingException(
                    "Could not dynamically add mapping for field [{}]. Existing mapping for [{}] must be of type object but found [{}].",
                    null,
                    String.join(".", paths),
                    currentPath,
                    existingFieldMapper.typeName()
                );
            }
            mapper = context.docMapper().objectMappers().get(currentPath);
            if (mapper == null) {
                // One mapping is missing, check if we are allowed to create a dynamic one.
                ObjectMapper.Dynamic dynamic = dynamicOrDefault(parent, context);
                switch (dynamic) {
                    case STRICT:
                        throw new StrictDynamicMappingException(parent.fullPath(), paths[i]);
                    case TRUE:
                        Mapper.Builder<?> builder = new ObjectMapper.Builder<>(paths[i]);
                        builder.position(getPositionEstimate(context));
                        Mapper.BuilderContext builderContext = new Mapper.BuilderContext(context.indexSettings().getSettings(),
                            context.path());
                        mapper = (ObjectMapper) builder.build(builderContext);
                        context.addDynamicMapper(mapper);
                        break;
                    case FALSE:
                        // Should not dynamically create any more mappers so return the last mapper
                        return new Tuple<>(pathsAdded, parent);

                    default:
                        throw new IllegalArgumentException("Unexpected dynamic value: " + dynamic);
                }
            }
            context.path().add(paths[i]);
            pathsAdded++;
            parent = mapper;
        }
        return new Tuple<>(pathsAdded, mapper);
    }

    // find what the dynamic setting is given the current parse context and parent
    private static ObjectMapper.Dynamic dynamicOrDefault(ObjectMapper parentMapper, ParseContext context) {
        ObjectMapper.Dynamic dynamic = parentMapper.dynamic();
        while (dynamic == null) {
            int lastDotNdx = parentMapper.name().lastIndexOf('.');
            if (lastDotNdx == -1) {
                // no dot means we the parent is the root, so just delegate to the default outside the loop
                break;
            }
            String parentName = parentMapper.name().substring(0, lastDotNdx);
            parentMapper = context.docMapper().objectMappers().get(parentName);
            if (parentMapper == null) {
                // If parentMapper is ever null, it means the parent of the current mapper was dynamically created.
                // But in order to be created dynamically, the dynamic setting of that parent was necessarily true
                return ObjectMapper.Dynamic.TRUE;
            }
            dynamic = parentMapper.dynamic();
        }
        if (dynamic == null) {
            return context.root().dynamic() == null ? ObjectMapper.Dynamic.TRUE : context.root().dynamic();
        }
        return dynamic;
    }

    // looks up a child mapper, but takes into account field names that expand to objects
    private static Mapper getMapper(ObjectMapper objectMapper, String fieldName, String[] subfields) {
        for (int i = 0; i < subfields.length - 1; ++i) {
            Mapper mapper = objectMapper.getMapper(subfields[i]);
            if (mapper == null || (mapper instanceof ObjectMapper) == false) {
                return null;
            }
            objectMapper = (ObjectMapper)mapper;
        }
        return objectMapper.getMapper(subfields[subfields.length - 1]);
    }
}
