/*
 * Copyright © 2018 Merck Sharp & Dohme Corp., a subsidiary of Merck & Co., Inc.
 * All rights reserved.
 */
package com.msd.gin.halyard.tools.rio;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFParserFactory;
import org.eclipse.rdf4j.rio.jsonld.JSONLDParser;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class NDJSONLDParserFactory implements RDFParserFactory {

    public static final RDFFormat NDJSONLD = new RDFFormat("ND-JSON-LD", Arrays.asList("application/x-nd-json-ld"),
                    StandardCharsets.UTF_8, Arrays.asList("ndjsonld"),
                    RDFFormat.SUPPORTS_NAMESPACES, RDFFormat.SUPPORTS_CONTEXTS);

    @Override
    public RDFFormat getRDFFormat() {
        return NDJSONLD;
    }

    @Override
    public RDFParser getParser() {
        return new NDJSONLDParser();
    }

    public static class NDJSONLDParser extends JSONLDParser {

        public NDJSONLDParser() {
        }

        public NDJSONLDParser(ValueFactory valueFactory) {
            super(valueFactory);
        }

        @Override
        public RDFFormat getRDFFormat() {
            return NDJSONLD;
        }

        @Override
        public void parse(InputStream in, String baseURI) throws IOException, RDFParseException, RDFHandlerException {
            parse(new InputStreamReader(in, StandardCharsets.UTF_8), baseURI);
        }

        @Override
        public void parse(Reader reader, String baseURI) throws IOException, RDFParseException, RDFHandlerException {
            BufferedReader br = new BufferedReader(reader);
            String line;
            while ((line = br.readLine()) != null) {
                super.parse(new StringReader(line), baseURI);
            }
        }
    }
}
