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

import java.util.ArrayList;
import java.util.List;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.action.admin.indices.view.ListViewNamesAction;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;

public class HttpListViewNamesAction extends HttpAction {

    protected final ListViewNamesAction action;

    public HttpListViewNamesAction(final HttpClient client, final ListViewNamesAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ListViewNamesAction.Request request, final ActionListener<ListViewNamesAction.Response> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
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
                listener.onResponse(new ListViewNamesAction.Response(viewNames));
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final ListViewNamesAction.Request request) {
        // RestViewAction
        return client.getCurlRequest(GET, "/views/");
    }
}
