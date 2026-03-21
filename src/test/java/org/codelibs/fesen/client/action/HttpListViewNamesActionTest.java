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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.view.ListViewNamesAction;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

class HttpListViewNamesActionTest {

    @Test
    void test_construction_withNullClient() {
        final HttpListViewNamesAction action = new HttpListViewNamesAction(null, ListViewNamesAction.INSTANCE);
        assertNotNull(action);
    }

    @Test
    void test_parseViewNames_multipleViews() throws IOException {
        final String json = """
                {"views": ["view1", "view2", "view3"]}""";

        final List<String> viewNames = parseViewNames(json);

        assertEquals(3, viewNames.size());
        assertEquals("view1", viewNames.get(0));
        assertEquals("view2", viewNames.get(1));
        assertEquals("view3", viewNames.get(2));
    }

    @Test
    void test_parseViewNames_emptyViews() throws IOException {
        final String json = """
                {"views": []}""";

        final List<String> viewNames = parseViewNames(json);

        assertNotNull(viewNames);
        assertTrue(viewNames.isEmpty());
    }

    @Test
    void test_parseViewNames_singleView() throws IOException {
        final String json = """
                {"views": ["only-view"]}""";

        final List<String> viewNames = parseViewNames(json);

        assertEquals(1, viewNames.size());
        assertEquals("only-view", viewNames.get(0));
    }

    @Test
    void test_parseViewNames_viewNamesWithSpecialCharacters() throws IOException {
        final String json = """
                {"views": ["my-view-1", "view_with_underscores", "view.with.dots"]}""";

        final List<String> viewNames = parseViewNames(json);

        assertEquals(3, viewNames.size());
        assertEquals("my-view-1", viewNames.get(0));
        assertEquals("view_with_underscores", viewNames.get(1));
        assertEquals("view.with.dots", viewNames.get(2));
    }

    @Test
    void test_parseViewNames_manyViews() throws IOException {
        final String json = """
                {"views": ["v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10"]}""";

        final List<String> viewNames = parseViewNames(json);

        assertEquals(10, viewNames.size());
        assertEquals("v1", viewNames.get(0));
        assertEquals("v10", viewNames.get(9));
    }

    /**
     * Reproduces the parsing logic from HttpListViewNamesAction.execute()
     * to verify the custom fromXContent parser without needing an HTTP connection.
     */
    private List<String> parseViewNames(final String json) throws IOException {
        try (final XContentParser parser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, json)) {
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
    }
}
