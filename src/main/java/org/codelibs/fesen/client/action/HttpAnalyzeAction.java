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

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction.AnalyzeToken;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction.AnalyzeTokenList;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction.CharFilteredText;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction.DetailAnalyzeResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

public class HttpAnalyzeAction extends HttpAction {

    protected final AnalyzeAction action;

    public HttpAnalyzeAction(final HttpClient client, final AnalyzeAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final AnalyzeAction.Request request, final ActionListener<AnalyzeAction.Response> listener) {
        String source = null;
        try (final XContentBuilder builder = toXContent(request, JsonXContent.contentBuilder())) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AnalyzeAction.Response cancelTasksResponse = fromXContent(parser);
                listener.onResponse(cancelTasksResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final AnalyzeAction.Request request) {
        return client.getCurlRequest(POST, "/_analyze", request.index() == null ? new String[0] : request.indices());
    }

    protected XContentBuilder toXContent(final AnalyzeAction.Request request, final XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("text", request.text());
        if (!Strings.isNullOrEmpty(request.analyzer())) {
            builder.field("analyzer", request.analyzer());
        }
        if (request.tokenizer() != null) {
            builder.field("tokenizer", request.tokenizer());
        }
        if (request.tokenFilters().size() > 0) {
            builder.field("filter", request.tokenFilters());
        }
        if (request.charFilters().size() > 0) {
            builder.field("char_filter", request.charFilters());
        }
        if (!Strings.isNullOrEmpty(request.field())) {
            builder.field("field", request.field());
        }
        if (request.explain()) {
            builder.field("explain", true);
        }
        if (request.attributes().length > 0) {
            builder.field("attributes", request.attributes());
        }
        if (!Strings.isNullOrEmpty(request.normalizer())) {
            builder.field("normalizer", request.normalizer());
        }
        return builder.endObject();
    }

    static final class Fields {
        static final String TOKENS = "tokens";
        static final String TOKEN = "token";
        static final String START_OFFSET = "start_offset";
        static final String END_OFFSET = "end_offset";
        static final String TYPE = "type";
        static final String POSITION = "position";
        static final String POSITION_LENGTH = "positionLength";
        static final String DETAIL = "detail";
        static final String NAME = "name";
        static final String FILTERED_TEXT = "filtered_text";
        static final String CUSTOM_ANALYZER = "custom_analyzer";
        static final String ANALYZER = "analyzer";
        static final String CHARFILTERS = "charfilters";
        static final String TOKENIZER = "tokenizer";
        static final String TOKENFILTERS = "tokenfilters";
    }

    @SuppressWarnings("unchecked")
    private static <T> T[] fromList(final Class<T> clazz, final List<T> list) {
        if (list == null) {
            return null;
        }
        return list.toArray((T[]) Array.newInstance(clazz, 0));
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<AnalyzeTokenList, Void> ATL_PARSER = new ConstructingObjectParser<>("token_list", true,
            args -> new AnalyzeTokenList((String) args[0], fromList(AnalyzeToken.class, (List<AnalyzeToken>) args[1])));
    static {
        ATL_PARSER.declareString(constructorArg(), new ParseField(Fields.NAME));
        ATL_PARSER.declareObjectArray(constructorArg(), (p, c) -> getAnalyzeTokenFromXContent(p), new ParseField(Fields.TOKENS));
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<CharFilteredText, Void> CFT_PARSER = new ConstructingObjectParser<>("char_filtered_text",
            true, args -> new CharFilteredText((String) args[0], ((List<String>) args[1]).toArray(new String[0])));
    static {
        CFT_PARSER.declareString(constructorArg(), new ParseField(Fields.NAME));
        CFT_PARSER.declareStringArray(constructorArg(), new ParseField(Fields.FILTERED_TEXT));
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<DetailAnalyzeResponse, Void> DETAIL_PARSER = new ConstructingObjectParser<>("detail", true,
            args -> createDetailAnalyzeResponse((boolean) args[0], (AnalyzeTokenList) args[1],
                    fromList(CharFilteredText.class, (List<CharFilteredText>) args[2]), (AnalyzeTokenList) args[3],
                    fromList(AnalyzeTokenList.class, (List<AnalyzeTokenList>) args[4])));
    static {
        DETAIL_PARSER.declareBoolean(constructorArg(), new ParseField(Fields.CUSTOM_ANALYZER));
        DETAIL_PARSER.declareObject(optionalConstructorArg(), ATL_PARSER, new ParseField(Fields.ANALYZER));
        DETAIL_PARSER.declareObjectArray(optionalConstructorArg(), CFT_PARSER, new ParseField(Fields.CHARFILTERS));
        DETAIL_PARSER.declareObject(optionalConstructorArg(), ATL_PARSER, new ParseField(Fields.TOKENIZER));
        DETAIL_PARSER.declareObjectArray(optionalConstructorArg(), ATL_PARSER, new ParseField(Fields.TOKENFILTERS));
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<AnalyzeAction.Response, Void> PARSER = new ConstructingObjectParser<>("analyze_response",
            true, args -> new AnalyzeAction.Response((List<AnalyzeToken>) args[0], (DetailAnalyzeResponse) args[1]));
    static {
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> getAnalyzeTokenFromXContent(p), new ParseField(Fields.TOKENS));
        PARSER.declareObject(optionalConstructorArg(), DETAIL_PARSER, new ParseField(Fields.DETAIL));
    }

    public static AnalyzeAction.Response fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    protected static AnalyzeToken getAnalyzeTokenFromXContent(final XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        String field = null;
        String term = "";
        int position = -1;
        int startOffset = -1;
        int endOffset = -1;
        int positionLength = 1;
        String type = "";
        final Map<String, Object> attributes = new HashMap<>();
        for (XContentParser.Token t = parser.nextToken(); t != XContentParser.Token.END_OBJECT; t = parser.nextToken()) {
            if (t == XContentParser.Token.FIELD_NAME) {
                field = parser.currentName();
                continue;
            }
            if (Fields.TOKEN.equals(field)) {
                term = parser.text();
            } else if (Fields.POSITION.equals(field)) {
                position = parser.intValue();
            } else if (Fields.START_OFFSET.equals(field)) {
                startOffset = parser.intValue();
            } else if (Fields.END_OFFSET.equals(field)) {
                endOffset = parser.intValue();
            } else if (Fields.POSITION_LENGTH.equals(field)) {
                positionLength = parser.intValue();
            } else if (Fields.TYPE.equals(field)) {
                type = parser.text();
            } else if (t == XContentParser.Token.VALUE_STRING) {
                attributes.put(field, parser.text());
            } else if (t == XContentParser.Token.VALUE_NUMBER) {
                attributes.put(field, parser.numberValue());
            } else if (t == XContentParser.Token.VALUE_BOOLEAN) {
                attributes.put(field, parser.booleanValue());
            } else if (t == XContentParser.Token.START_OBJECT) {
                attributes.put(field, parser.map());
            } else if (t == XContentParser.Token.START_ARRAY) {
                attributes.put(field, parser.list());
            }
        }
        return new AnalyzeToken(term, position, startOffset, endOffset, positionLength, type, attributes);
    }

    private static DetailAnalyzeResponse createDetailAnalyzeResponse(final boolean customAnalyzer, final AnalyzeTokenList analyzer,
            final CharFilteredText[] charfilters, final AnalyzeTokenList tokenizer, final AnalyzeTokenList[] tokenfilters) {
        if (customAnalyzer) {
            return new DetailAnalyzeResponse(charfilters, tokenizer, tokenfilters);
        }
        return new DetailAnalyzeResponse(analyzer);
    }
}
