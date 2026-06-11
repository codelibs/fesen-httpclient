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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.action.admin.indices.view.ListViewNamesAction;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Handles the List View Names API over HTTP for OpenSearch.
 */
public class HttpListViewNamesAction extends HttpAction {

    /** The list view names action definition. */
    protected final ListViewNamesAction action;

    /**
     * Creates a new HTTP list view names action.
     *
     * @param client the HTTP client
     * @param action the list view names action definition
     */
    public HttpListViewNamesAction(final HttpClient client, final ListViewNamesAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the list view names request and notifies the listener with the response.
     *
     * @param request the list view names request
     * @param listener the listener to notify with the response or failure
     */
    public void execute(final ListViewNamesAction.Request request, final ActionListener<ListViewNamesAction.Response> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final List<String> viewNames = parseViewNames(parser);
                listener.onResponse(new ListViewNamesAction.Response(viewNames));
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Parses the view names from the response body.
     *
     * @param parser the XContent parser positioned at the response body
     * @return the list of view names
     * @throws IOException if parsing fails
     */
    protected List<String> parseViewNames(final XContentParser parser) throws IOException {
        final List<String> viewNames = new ArrayList<>();
        XContentParser.Token token = parser.nextToken();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME && "views".equals(parser.currentName())) {
                parser.nextToken(); // START_ARRAY
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    viewNames.add(parser.text());
                }
            }
        }
        return viewNames;
    }

    /**
     * Builds the curl request for the list view names API.
     *
     * @param request the list view names request
     * @return the curl request
     */
    protected CurlRequest getCurlRequest(final ListViewNamesAction.Request request) {
        // RestViewAction
        return client.getCurlRequest(GET, "/views");
    }
}
