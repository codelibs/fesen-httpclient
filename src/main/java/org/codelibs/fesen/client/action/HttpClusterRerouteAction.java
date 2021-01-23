package org.codelibs.fesen.client.action;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.admin.cluster.reroute.ClusterRerouteAction;
import org.codelibs.fesen.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.codelibs.fesen.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.codelibs.fesen.action.support.master.AcknowledgedResponse;
import org.codelibs.fesen.common.xcontent.XContentParser;

public class HttpClusterRerouteAction extends HttpAction {

    protected final ClusterRerouteAction action;

    public HttpClusterRerouteAction(final HttpClient client, final ClusterRerouteAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ClusterRerouteRequest request, final ActionListener<AcknowledgedResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final AcknowledgedResponse clusterRerouteResponse = ClusterRerouteResponse.fromXContent(parser);
                listener.onResponse(clusterRerouteResponse);
            } catch (final Exception e) {
                listener.onFailure(toFesenException(response, e));
            }
        }, e -> unwrapFesenException(listener, e));
    }

    protected CurlRequest getCurlRequest(final ClusterRerouteRequest request) {
        // RestClusterRerouteAction
        final CurlRequest curlRequest = client.getCurlRequest(POST, "/_cluster/reroute");
        if (request.dryRun()) {
            curlRequest.param("dry_run", Boolean.toString(request.dryRun()));
        }
        if (request.explain()) {
            curlRequest.param("explain", Boolean.toString(request.explain()));
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.isRetryFailed()) {
            curlRequest.param("retry_failed", Boolean.toString(request.isRetryFailed()));
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
