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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.opensearch.action.admin.indices.view.CreateViewAction;
import org.codelibs.fesen.opensearch.action.admin.indices.view.DeleteViewAction;
import org.codelibs.fesen.opensearch.action.admin.indices.view.GetViewAction;
import org.codelibs.fesen.opensearch.action.admin.indices.view.ListViewNamesAction;
import org.codelibs.fesen.opensearch.action.admin.indices.view.SearchViewAction;
import org.codelibs.fesen.opensearch.action.admin.indices.view.UpdateViewAction;
import org.codelibs.fesen.opensearch.action.search.SearchRequest;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.junit.jupiter.api.Test;

/**
 * The Views API exists in OpenSearch 3.x up to 3.9 and was removed in 3.10, so the view actions
 * must fail on 3.10 or later without sending the request.
 */
class HttpViewActionsVersionTest {

    private static final class RecordingListener<T> implements ActionListener<T> {
        final List<Exception> failures = new ArrayList<>();

        @Override
        public void onResponse(final T response) {
            throw new AssertionError("unexpected response");
        }

        @Override
        public void onFailure(final Exception e) {
            failures.add(e);
        }
    }

    private static <T> void assertRejected(final RecordingListener<T> listener) {
        assertEquals(1, listener.failures.size());
        assertInstanceOf(UnsupportedOperationException.class, listener.failures.get(0));
    }

    @Test
    void test_allViewActionsRejectedOnOpenSearch310() {
        final HttpClient client = ActionTestUtils.openSearchClient("3.10.0");

        final RecordingListener<GetViewAction.Response> create = new RecordingListener<>();
        new HttpCreateViewAction(client, CreateViewAction.INSTANCE).execute(new CreateViewAction.Request("v", "d", List.of()), create);
        assertRejected(create);

        final RecordingListener<GetViewAction.Response> update = new RecordingListener<>();
        new HttpUpdateViewAction(client, UpdateViewAction.INSTANCE).execute(new CreateViewAction.Request("v", "d", List.of()), update);
        assertRejected(update);

        final RecordingListener<GetViewAction.Response> get = new RecordingListener<>();
        new HttpGetViewAction(client, GetViewAction.INSTANCE).execute(new GetViewAction.Request("v"), get);
        assertRejected(get);

        final RecordingListener<org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse> delete =
                new RecordingListener<>();
        new HttpDeleteViewAction(client, DeleteViewAction.INSTANCE).execute(new DeleteViewAction.Request("v"), delete);
        assertRejected(delete);

        final RecordingListener<ListViewNamesAction.Response> list = new RecordingListener<>();
        new HttpListViewNamesAction(client, ListViewNamesAction.INSTANCE).execute(new ListViewNamesAction.Request(), list);
        assertRejected(list);

        final RecordingListener<org.codelibs.fesen.opensearch.action.search.SearchResponse> search = new RecordingListener<>();
        new HttpSearchViewAction(client, SearchViewAction.INSTANCE).execute(new SearchViewAction.Request("v", new SearchRequest()), search);
        assertRejected(search);
    }

    @Test
    void test_versionBoundary() {
        for (final String number : new String[] { "3.10.0", "3.10.0-SNAPSHOT", "3.11.0", "4.0.0" }) {
            final RecordingListener<GetViewAction.Response> listener = new RecordingListener<>();
            assertTrue(rejects(ActionTestUtils.openSearchClient(number), listener), number);
        }
        for (final String number : new String[] { "3.0.0", "3.9.0", "3.9.1", "2.19.7", "1.3.20" }) {
            final RecordingListener<GetViewAction.Response> listener = new RecordingListener<>();
            assertFalse(rejects(ActionTestUtils.openSearchClient(number), listener), number);
            assertEquals(0, listener.failures.size(), number);
        }
    }

    private static boolean rejects(final HttpClient client, final RecordingListener<GetViewAction.Response> listener) {
        return new HttpGetViewAction(client, GetViewAction.INSTANCE).rejectRemovedViews(listener);
    }
}
