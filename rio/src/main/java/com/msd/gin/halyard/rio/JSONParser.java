/*
 * Copyright 2018 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.rio;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFParserFactory;
import org.eclipse.rdf4j.rio.RioSetting;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFParser;
import org.eclipse.rdf4j.rio.helpers.BooleanRioSetting;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.util.JsonParserDelegate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 * @author Adam Sotona (MSD)
 */
public final class JSONParser extends AbstractRDFParser {

    public static class Factory implements RDFParserFactory {

        @Override
        public RDFFormat getRDFFormat() {
            return JSON;
        }

        @Override
        public RDFParser getParser() {
            return new JSONParser();
        }

    }

    public static final RDFFormat JSON = new RDFFormat("JSON", Arrays.asList("application/json"), StandardCharsets.UTF_8, Collections.singletonList("json"), RDFFormat.NO_NAMESPACES, RDFFormat.NO_CONTEXTS, RDFFormat.NO_RDF_STAR);
    public static final RioSetting<Boolean> GENERATE_ONTOLOGY = new BooleanRioSetting(JSONParser.class.getCanonicalName() + ".GENERATE_ONTOLOGY", "Generate ontology statements while parsing", Boolean.FALSE);
    public static final RioSetting<Boolean> GENERATE_DATA = new BooleanRioSetting(JSONParser.class.getCanonicalName() + ".GENERATE_DATA", "Generate data statements while parsing", Boolean.TRUE);

    private static final String MD_ALGORITHM = "SHA-512";

    private final JsonFactory jsonFactory = new ObjectMapper().getFactory();
    private final MessageDigest md;


    private boolean generateOntology, generateData;
    private IRI baseURI;

    public JSONParser() {
        this(MD_ALGORITHM);
    }

    JSONParser(String mdAlgorithm) {
        try {
            md = MessageDigest.getInstance(mdAlgorithm);
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public RDFFormat getRDFFormat() {
        return JSON;
    }

    @Override
    public Collection<RioSetting<?>> getSupportedSettings() {
        Collection<RioSetting<?>> supportedSettings = super.getSupportedSettings();
        supportedSettings.add(GENERATE_DATA);
        supportedSettings.add(GENERATE_ONTOLOGY);
        return supportedSettings;
    }

    @Override
    public void parse(InputStream in, String baseURI) throws IOException, RDFParseException, RDFHandlerException {
        parse(jsonFactory.createParser(in), baseURI);
    }

    @Override
    public void parse(Reader reader, String baseURI) throws IOException, RDFParseException, RDFHandlerException {
        parse(jsonFactory.createParser(reader), baseURI);
    }

    private void parse(JsonParser jsonSource, String baseURI) throws IOException {
        this.baseURI = valueFactory.createIRI(baseURI);
        setBaseURI(baseURI);
        generateOntology = getParserConfig().get(GENERATE_ONTOLOGY);
        generateData = getParserConfig().get(GENERATE_DATA);
        try (JsonParser parser = new JsonParserDelegate(jsonSource) {
            @Override
            public JsonToken nextToken() throws IOException {
                JsonToken t = super.nextToken();
                return t != null && t.isNumeric() ? JsonToken.VALUE_STRING : t;
            }

            @Override
            public JsonToken getCurrentToken() {
                JsonToken t = super.getCurrentToken();
                return t != null && t.isNumeric() ? JsonToken.VALUE_STRING : t;
            }

            @Override
            public int getCurrentTokenId() {
                int id = super.getCurrentTokenId();
                return (id == JsonTokenId.ID_NUMBER_INT || id == JsonTokenId.ID_NUMBER_FLOAT) ? JsonTokenId.ID_STRING : id;
            }

        }) {
            if (rdfHandler != null) {
                rdfHandler.startRDF();
            }
            if (generateOntology) {
                handleNamespace(RDF.PREFIX, RDF.NAMESPACE);
                handleNamespace(RDFS.PREFIX, RDFS.NAMESPACE);
                handleNamespace(OWL.PREFIX, OWL.NAMESPACE);
                handleStatement(valueFactory.createStatement(this.baseURI, RDF.TYPE, OWL.ONTOLOGY));
            }
            handleNamespace("", this.baseURI.stringValue());
            TreeNode root = parser.readValueAsTree();
            if (root != null) {
                if (root.isArray()) {
                    for (int i=0; i<root.size(); i++) {
                        transform(root.get(i));
                    }
                } else if (root.isObject()) {
                    transform(root);
                } else {
                    throw new IllegalArgumentException("Illegal node type");
                }
                if (parser.nextToken() != null) {
                    throw new IllegalArgumentException("Invalid JSON format");
                }
            }
            if (rdfHandler != null) {
                rdfHandler.endRDF();
            }
        } catch (URISyntaxException ex) {
            throw new RDFParseException(ex);
        }
    }
    private void hash(TreeNode node) {
        if (node.isArray()) {
            md.update((byte)'l');
            for (int i = 0; i < node.size(); i++) {
                hash(node.get(i));
            }
            md.update((byte)'e');
        } else if (node.isObject()) {
            String[] fieldNames = new String[node.size()];
            Iterator<String> it = node.fieldNames();
            for (int i=0; i< fieldNames.length; i++) {
                fieldNames[i] = it.next();
            }
            Arrays.sort(fieldNames);
            md.update((byte)'d');
            for (String fName : fieldNames) {
                hash(fName);
                hash(node.get(fName));
            }
            md.update((byte)'e');
        } else if (node.isValueNode()) {
            String val = ((JsonNode)node).textValue();
            if (val != null) {
                hash(val);
            }
        } else {
            throw new IllegalArgumentException(node.toString());
        }
    }

    private void hash(String val) {
        byte[] bs = val.getBytes(StandardCharsets.UTF_8);
        byte[] bsLen = String.valueOf(bs.length).getBytes(StandardCharsets.UTF_8);
        md.update(bsLen);
        md.update((byte)':');
        md.update(bs);
    }

    private void handleNamespace(String prefix, String uri) {
        if (rdfHandler != null) {
            rdfHandler.handleNamespace(prefix, uri);
        }
    }

    private void handleStatement(Statement st) {
        if (rdfHandler != null) {
            rdfHandler.handleStatement(st);
        }
    }

    private void transform(TreeNode node) throws URISyntaxException {
        String hash;
        try {
            hash(node);
            hash = Base64.getUrlEncoder().withoutPadding().encodeToString(md.digest());
        } finally {
            md.reset();
        }
        treeWalk(null, null, valueFactory.createIRI(baseURI + hash).stringValue(), "", null, null, node);
    }

    private void treeWalk(IRI parentIRI, IRI parentClass, String path, String predicatePath, String label, Integer index, TreeNode node) throws URISyntaxException {
        if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                treeWalk(parentIRI, parentClass, path + ":" + i, predicatePath, label, i, node.get(i));
            }
        } else if (node.isObject()) {
            IRI pkIRI = valueFactory.createIRI((parentIRI == null ? "" : parentIRI.stringValue()) + path);
            IRI classIRI = valueFactory.createIRI(baseURI + predicatePath + ":Node");
            if (parentIRI != null) {
                IRI predicate = valueFactory.createIRI(baseURI + predicatePath);
                if (generateData) {
                    handleStatement(createStatement(parentIRI, predicate, pkIRI));
                }
                if (generateOntology) {
                    handleStatement(createStatement(predicate, RDF.TYPE, OWL.OBJECTPROPERTY));
                    handleStatement(createStatement(predicate, RDFS.LABEL, valueFactory.createLiteral(label)));
                    handleStatement(createStatement(predicate, RDFS.DOMAIN, parentClass));
                    handleStatement(createStatement(predicate, RDFS.RANGE, classIRI));
                }
            }
            if (generateData) {
                handleStatement(createStatement(pkIRI, RDF.TYPE, classIRI));
            }
            if (generateOntology) {
                handleStatement(createStatement(classIRI, RDF.TYPE, RDFS.CLASS));
            }
            if (index != null) {
                IRI indexIRI = valueFactory.createIRI(baseURI + predicatePath + ":index");
                if (generateData) {
                    handleStatement(createStatement(pkIRI, indexIRI, valueFactory.createLiteral(index.toString(), XSD.INTEGER)));
                }
                if (generateOntology) {
                    handleStatement(createStatement(indexIRI, RDF.TYPE, OWL.DATATYPEPROPERTY));
                    handleStatement(createStatement(indexIRI, RDFS.LABEL, valueFactory.createLiteral("index")));
                    handleStatement(createStatement(indexIRI, RDFS.DOMAIN, classIRI));
                }
            }
            Iterator<String> fieldNames = node.fieldNames();
            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                String encodedFieldName = new URI(null, null, fieldName).getRawFragment().replace(".", "%2E").replace(":", "%3A");
                treeWalk(pkIRI, classIRI, "." + encodedFieldName, (predicatePath.length() == 0 ? "": predicatePath + '.') + encodedFieldName, fieldName, null, node.get(fieldName));
            }
        } else if (node.isValueNode()) {
            if (parentIRI == null) {
                throw new IllegalArgumentException("Value without parent IRI");
            }
            IRI predicate = valueFactory.createIRI(baseURI + predicatePath);
            if (generateData) {
                String value = ((JsonNode)node).textValue();
                if (value != null) {
                    handleStatement(createStatement(parentIRI, predicate, valueFactory.createLiteral(value)));
                }
            }
            if (generateOntology) {
                handleStatement(createStatement(predicate, RDF.TYPE, OWL.DATATYPEPROPERTY));
                handleStatement(createStatement(predicate, RDFS.LABEL, valueFactory.createLiteral(label)));
                handleStatement(createStatement(predicate, RDFS.DOMAIN, parentClass));
            }
        } else {
                throw new IllegalArgumentException("Illegal node type");
        }
    }
}
