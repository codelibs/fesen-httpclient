/*
 * Copyright 2012-2023 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fesen.client.action;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.CollectionUtil;
import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.action.admin.indices.get.GetIndexAction;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

public class HttpGetIndexAction extends HttpAction {

    protected final GetIndexAction action;

    public HttpGetIndexAction(final HttpClient client, final GetIndexAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final GetIndexRequest request, final ActionListener<GetIndexResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final GetIndexResponse getIndexResponse = fromXContent(parser);
                listener.onResponse(getIndexResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final GetIndexRequest request) {
        // RestGetIndexAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/", request.indices());
        curlRequest.param("include_defaults", Boolean.toString(request.includeDefaults()));
        return curlRequest;
    }

    protected static GetIndexResponse fromXContent(final XContentParser parser) throws IOException {
        Map<String, MappingMetadata> mappings = new HashMap<>();
        final Map<String, List<AliasMetadata>> aliases = new HashMap<>();
        final Map<String, Settings> settings = new HashMap<>();
        final Map<String, Settings> defaultSettings = new HashMap<>();
        final Map<String, String> dataStreams = new HashMap<>();
        final List<String> indices = new ArrayList<>();

        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        parser.nextToken();

        while (!parser.isClosed()) {
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                // we assume this is an index entry
                final String indexName = parser.currentName();
                indices.add(indexName);
                final IndexEntry indexEntry = parseIndexEntry(parser);
                // make the order deterministic
                CollectionUtil.timSort(indexEntry.indexAliases, Comparator.comparing(AliasMetadata::alias));
                aliases.put(indexName, Collections.unmodifiableList(indexEntry.indexAliases));
                mappings = indexEntry.indexMappings;
                settings.put(indexName, indexEntry.indexSettings);
                if (!indexEntry.indexDefaultSettings.isEmpty()) {
                    defaultSettings.put(indexName, indexEntry.indexDefaultSettings);
                }
                if (indexEntry.dataStream != null) {
                    dataStreams.put(indexName, indexEntry.dataStream);
                }
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            } else {
                parser.nextToken();
            }
        }
        return new GetIndexResponse(indices.toArray(new String[0]), mappings, aliases, settings, defaultSettings, dataStreams);
    }

    protected static IndexEntry parseIndexEntry(final XContentParser parser) throws IOException {
        List<AliasMetadata> indexAliases = null;
        Map<String, MappingMetadata> indexMappings = null;
        Settings indexSettings = null;
        Settings indexDefaultSettings = null;
        String dataStream = null;
        // We start at START_OBJECT since fromXContent ensures that
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            parser.nextToken();
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                switch (parser.currentName()) {
                case "aliases":
                    indexAliases = parseAliases(parser);
                    break;
                case "mappings":
                    indexMappings = parseMappings(parser);
                    break;
                case "settings":
                    indexSettings = Settings.fromXContent(parser);
                    break;
                case "defaults":
                    indexDefaultSettings = Settings.fromXContent(parser);
                    break;
                default:
                    parser.skipChildren();
                }
            } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                if ("data_stream".equals(parser.currentName())) {
                    dataStream = parser.text();
                }
                parser.skipChildren();
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            }
        }
        return new IndexEntry(indexAliases, indexMappings, indexSettings, indexDefaultSettings, dataStream);
    }

    protected static List<AliasMetadata> parseAliases(final XContentParser parser) throws IOException {
        final List<AliasMetadata> indexAliases = new ArrayList<>();
        // We start at START_OBJECT since parseIndexEntry ensures that
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            indexAliases.add(AliasMetadata.Builder.fromXContent(parser));
        }
        return indexAliases;
    }

    protected static Map<String, MappingMetadata> parseMappings(final XContentParser parser) throws IOException {
        final Map<String, MappingMetadata> indexMappings = new HashMap<>();
        // We start at START_OBJECT since parseIndexEntry ensures that
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            parser.nextToken();
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                final String mappingType = parser.currentName();
                indexMappings.put(mappingType, new MappingMetadata(mappingType, parser.map()));
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            }
        }
        return indexMappings;
    }

    // This is just an internal container to make stuff easier for returning
    protected static class IndexEntry {
        List<AliasMetadata> indexAliases = new ArrayList<>();
        Map<String, MappingMetadata> indexMappings = new HashMap<>();
        Settings indexSettings = Settings.EMPTY;
        Settings indexDefaultSettings = Settings.EMPTY;
        String dataStream;

        IndexEntry(final List<AliasMetadata> indexAliases, final Map<String, MappingMetadata> indexMappings, final Settings indexSettings,
                final Settings indexDefaultSettings, final String dataStream) {
            if (indexAliases != null) {
                this.indexAliases = indexAliases;
            }
            if (indexMappings != null) {
                this.indexMappings = indexMappings;
            }
            if (indexSettings != null) {
                this.indexSettings = indexSettings;
            }
            if (indexDefaultSettings != null) {
                this.indexDefaultSettings = indexDefaultSettings;
            }
            if (dataStream != null) {
                this.dataStream = dataStream;
            }
        }
    }
}
