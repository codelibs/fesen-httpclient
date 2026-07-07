/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
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
import java.util.List;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.io.stream.ByteArrayStreamOutput;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the Resolve Index API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpResolveIndexAction extends HttpAction {

    /** The resolve index action definition. */
    protected final ResolveIndexAction action;

    /**
     * Creates a new HTTP resolve index action.
     *
     * @param client the HTTP client used to send requests
     * @param action the resolve index action definition
     */
    public HttpResolveIndexAction(final HttpClient client, final ResolveIndexAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the resolve index request asynchronously.
     *
     * @param request the resolve index request
     * @param listener the listener notified with the resolve index response or a failure
     */
    public void execute(final ResolveIndexAction.Request request, final ActionListener<ResolveIndexAction.Response> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ResolveIndexAction.Response resolveIndexResponse = fromXContent(parser);
                listener.onResponse(resolveIndexResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Builds the curl request for the resolve index API.
     *
     * @param request the resolve index request
     * @return the curl request to send
     */
    protected CurlRequest getCurlRequest(final ResolveIndexAction.Request request) {
        // RestResolveIndexAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_resolve/index/" + UrlUtils.joinAndEncode(",", request.indices()));
        appendIndicesOptions(curlRequest, request.indicesOptions());
        return curlRequest;
    }

    /**
     * Parses the HTTP response body into a {@link ResolveIndexAction.Response}.
     *
     * <p>{@link ResolveIndexAction.Response} does not expose a {@code fromXContent} method and the
     * element types ({@code ResolvedIndex}, {@code ResolvedAlias}, {@code ResolvedDataStream}) have
     * package-private constructors, so the parsed values are re-serialized into the transport wire
     * format and read back through the response reader.</p>
     *
     * @param parser the content parser for the response body
     * @return the parsed resolve index response
     * @throws IOException if parsing fails
     */
    protected ResolveIndexAction.Response fromXContent(final XContentParser parser) throws IOException {
        final List<ResolvedIndexData> indices = new ArrayList<>();
        final List<ResolvedAliasData> aliases = new ArrayList<>();
        final List<ResolvedDataStreamData> dataStreams = new ArrayList<>();

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("indices".equals(fieldName)) {
                    parseIndices(parser, indices);
                } else if ("aliases".equals(fieldName)) {
                    parseAliases(parser, aliases);
                } else if ("data_streams".equals(fieldName)) {
                    parseDataStreams(parser, dataStreams);
                } else {
                    parser.skipChildren();
                }
            }
        }

        try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            out.writeVInt(indices.size());
            for (final ResolvedIndexData data : indices) {
                out.writeString(data.name);
                out.writeStringArray(data.aliases.toArray(new String[0]));
                out.writeStringArray(data.attributes.toArray(new String[0]));
                out.writeOptionalString(data.dataStream);
            }
            out.writeVInt(aliases.size());
            for (final ResolvedAliasData data : aliases) {
                out.writeString(data.name);
                out.writeStringArray(data.indices.toArray(new String[0]));
            }
            out.writeVInt(dataStreams.size());
            for (final ResolvedDataStreamData data : dataStreams) {
                out.writeString(data.name);
                out.writeStringArray(data.backingIndices.toArray(new String[0]));
                out.writeString(data.timestampField);
            }
            return action.getResponseReader().read(out.toStreamInput());
        }
    }

    /**
     * Parses the {@code indices} array of the response body.
     *
     * @param parser the content parser positioned at the {@code indices} array start
     * @param indices the list to append parsed resolved indices to
     * @throws IOException if parsing fails
     */
    protected static void parseIndices(final XContentParser parser, final List<ResolvedIndexData> indices) throws IOException {
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            final ResolvedIndexData data = new ResolvedIndexData();
            String fieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("aliases".equals(fieldName)) {
                        data.aliases = readStringArray(parser);
                    } else if ("attributes".equals(fieldName)) {
                        data.attributes = readStringArray(parser);
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if ("name".equals(fieldName)) {
                        data.name = parser.text();
                    } else if ("data_stream".equals(fieldName)) {
                        data.dataStream = parser.text();
                    }
                }
            }
            indices.add(data);
        }
    }

    /**
     * Parses the {@code aliases} array of the response body.
     *
     * @param parser the content parser positioned at the {@code aliases} array start
     * @param aliases the list to append parsed resolved aliases to
     * @throws IOException if parsing fails
     */
    protected static void parseAliases(final XContentParser parser, final List<ResolvedAliasData> aliases) throws IOException {
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            final ResolvedAliasData data = new ResolvedAliasData();
            String fieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("indices".equals(fieldName)) {
                        data.indices = readStringArray(parser);
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.VALUE_STRING && "name".equals(fieldName)) {
                    data.name = parser.text();
                }
            }
            aliases.add(data);
        }
    }

    /**
     * Parses the {@code data_streams} array of the response body.
     *
     * @param parser the content parser positioned at the {@code data_streams} array start
     * @param dataStreams the list to append parsed resolved data streams to
     * @throws IOException if parsing fails
     */
    protected static void parseDataStreams(final XContentParser parser, final List<ResolvedDataStreamData> dataStreams) throws IOException {
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            final ResolvedDataStreamData data = new ResolvedDataStreamData();
            String fieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("backing_indices".equals(fieldName)) {
                        data.backingIndices = readStringArray(parser);
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if ("name".equals(fieldName)) {
                        data.name = parser.text();
                    } else if ("timestamp_field".equals(fieldName)) {
                        data.timestampField = parser.text();
                    }
                }
            }
            dataStreams.add(data);
        }
    }

    /**
     * Reads a JSON array of string values into a list.
     *
     * @param parser the content parser positioned at the array start
     * @return the list of parsed string values
     * @throws IOException if parsing fails
     */
    protected static List<String> readStringArray(final XContentParser parser) throws IOException {
        final List<String> values = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            values.add(parser.text());
        }
        return values;
    }

    /**
     * Internal container holding the parsed fields of a resolved index entry.
     */
    protected static class ResolvedIndexData {
        String name;
        List<String> aliases = new ArrayList<>();
        List<String> attributes = new ArrayList<>();
        String dataStream;
    }

    /**
     * Internal container holding the parsed fields of a resolved alias entry.
     */
    protected static class ResolvedAliasData {
        String name;
        List<String> indices = new ArrayList<>();
    }

    /**
     * Internal container holding the parsed fields of a resolved data stream entry.
     */
    protected static class ResolvedDataStreamData {
        String name;
        List<String> backingIndices = new ArrayList<>();
        String timestampField;
    }
}
