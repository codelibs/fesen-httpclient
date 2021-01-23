package org.codelibs.fesen.client.action;

import java.io.IOException;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.codelibs.fesen.FesenException;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.codelibs.fesen.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.xcontent.ToXContent;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentParser;
import org.codelibs.fesen.common.xcontent.json.JsonXContent;

public class HttpRestoreSnapshotAction extends HttpAction {

    protected final RestoreSnapshotAction action;

    public HttpRestoreSnapshotAction(final HttpClient client, final RestoreSnapshotAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final RestoreSnapshotRequest request, final ActionListener<RestoreSnapshotResponse> listener) {
        String source = null;
        try (final XContentBuilder builder = request.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new FesenException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final RestoreSnapshotResponse restoreSnapshotResponse = RestoreSnapshotResponse.fromXContent(parser);
                listener.onResponse(restoreSnapshotResponse);
            } catch (final Exception e) {
                listener.onFailure(toFesenException(response, e));
            }
        }, e -> unwrapFesenException(listener, e));
    }

    protected CurlRequest getCurlRequest(final RestoreSnapshotRequest request) {
        // RestRestoreSnapshotAction
        final StringBuilder pathBuf = new StringBuilder(100).append("/_snapshot");
        if (request.repository() != null) {
            pathBuf.append('/').append(UrlUtils.encode(request.repository()));
        }
        if (request.snapshot() != null) {
            pathBuf.append('/').append(UrlUtils.encode(request.snapshot()));
        }
        pathBuf.append('/').append("_restore");
        final CurlRequest curlRequest = client.getCurlRequest(POST, pathBuf.toString());
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        if (request.waitForCompletion()) {
            curlRequest.param("wait_for_completion", Boolean.toString(request.waitForCompletion()));
        }
        return curlRequest;
    }

}
