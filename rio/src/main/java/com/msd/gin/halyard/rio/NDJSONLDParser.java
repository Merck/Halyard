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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
public class NDJSONLDParser extends JSONLDParser {

    public static class Factory implements RDFParserFactory {

        @Override
        public RDFFormat getRDFFormat() {
            return NDJSONLD;
        }

        @Override
        public RDFParser getParser() {
            return new NDJSONLDParser();
        }
    }

    public static final RDFFormat NDJSONLD = new RDFFormat("ND-JSON-LD", Arrays.asList("application/x-nd-json-ld"),
        StandardCharsets.UTF_8, Arrays.asList("ndjsonld"),
        RDFFormat.SUPPORTS_NAMESPACES, RDFFormat.SUPPORTS_CONTEXTS);

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
