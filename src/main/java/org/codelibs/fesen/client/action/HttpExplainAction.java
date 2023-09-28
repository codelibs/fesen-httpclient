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

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.search.Explanation;
import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.OpenSearchException;
import org.opensearch.action.explain.ExplainAction;
import org.opensearch.action.explain.ExplainRequest;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.get.GetResult;

public class HttpExplainAction extends HttpAction {

    protected final ExplainAction action;

    public HttpExplainAction(final HttpClient client, final ExplainAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ExplainRequest request, final ActionListener<ExplainResponse> listener) {
        String source = null;
        try (final XContentBuilder builder =
                XContentFactory.jsonBuilder().startObject().field(QUERY_FIELD.getPreferredName(), request.query()).endObject()) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ExplainResponse explainResponse = fromXContent(parser, true);
                listener.onResponse(explainResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    // ExplainResponse.fromXContent(parser, true)
    protected ExplainResponse fromXContent(final XContentParser parser, final boolean exists) {
        final EngineType engineType = client.getEngineInfo().getType();
        if (engineType == EngineType.ELASTICSEARCH8 || engineType == EngineType.OPENSEARCH2) {
            return getResponseParser().apply(parser, exists);
        }
        return ExplainResponse.fromXContent(parser, exists);
    }

    protected ConstructingObjectParser<ExplainResponse, Boolean> getResponseParser() {
        // remove _type
        final ConstructingObjectParser<ExplainResponse, Boolean> parser = new ConstructingObjectParser<>("explain", true,
                (arg, exists) -> new ExplainResponse((String) arg[0], (String) arg[1], exists, (Explanation) arg[2], (GetResult) arg[3]));
        parser.declareString(ConstructingObjectParser.constructorArg(), new ParseField("_index"));
        parser.declareString(ConstructingObjectParser.constructorArg(), new ParseField("_id"));
        final ConstructingObjectParser<Explanation, Boolean> explanationParser =
                new ConstructingObjectParser<>("explanation", true, arg -> {
                    if ((float) arg[0] > 0) {
                        return Explanation.match((float) arg[0], (String) arg[1], (Collection<Explanation>) arg[2]);
                    }
                    return Explanation.noMatch((String) arg[1], (Collection<Explanation>) arg[2]);
                });
        explanationParser.declareFloat(ConstructingObjectParser.constructorArg(), new ParseField("value"));
        explanationParser.declareString(ConstructingObjectParser.constructorArg(), new ParseField("description"));
        explanationParser.declareObjectArray(ConstructingObjectParser.constructorArg(), explanationParser, new ParseField("details"));
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(), explanationParser, new ParseField("explanation"));
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> GetResult.fromXContentEmbedded(p),
                new ParseField("get"));
        return parser;
    }

    protected CurlRequest getCurlRequest(final ExplainRequest request) {
        // RestExplainAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_explain/" + UrlUtils.encode(request.id()), request.index());
        if (request.routing() != null) {
            curlRequest.param("routing", request.routing());
        }
        if (request.preference() != null) {
            curlRequest.param("preference", request.preference());
        }
        if (request.query() != null) {
            //
        }
        if (request.storedFields() != null) {
            curlRequest.param("stored_fields", String.join(",", request.storedFields()));
        }
        return curlRequest;
    }

}
