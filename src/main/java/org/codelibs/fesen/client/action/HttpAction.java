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
import java.util.Locale;
import java.util.function.Function;

import org.codelibs.curl.Curl;
import org.codelibs.curl.CurlRequest;
import org.codelibs.curl.CurlResponse;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.io.stream.ByteArrayStreamOutput;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BytesRestResponse;

/**
 * Base class for HTTP-based action implementations that invoke OpenSearch/Elasticsearch
 * REST APIs. Provides shared parse fields, HTTP method factories, and helper methods for
 * parsing responses and converting errors to exceptions.
 */
public class HttpAction {

    /** Parse field for the "shard" response property. */
    protected static final ParseField SHARD_FIELD = new ParseField("shard");

    /** Parse field for the "index" response property. */
    protected static final ParseField INDEX_FIELD = new ParseField("index");

    /** Parse field for the "query" response property. */
    protected static final ParseField QUERY_FIELD = new ParseField("query");

    /** Parse field for the "reason" response property. */
    protected static final ParseField REASON_FIELD = new ParseField("reason");

    /** Parse field for the "aliases" response property. */
    protected static final ParseField ALIASES_FIELD = new ParseField("aliases");

    /** Parse field for the "mappings" response property. */
    protected static final ParseField MAPPINGS_FIELD = new ParseField("mappings");

    /** Parse field for the "type" response property. */
    protected static final ParseField TYPE_FIELD = new ParseField("type");

    /** Parse field for the "details" response property. */
    protected static final ParseField DETAILS_FIELD = new ParseField("details");

    /** Parse field for the "_shards" response property. */
    protected static final ParseField _SHARDS_FIELD = new ParseField("_shards");

    /** Parse field for the "tasks" response property. */
    protected static final ParseField TASKS_FIELD = new ParseField("tasks");

    /** Parse field for the "insert_order" response property. */
    protected static final ParseField INSERT_ORDER_FIELD = new ParseField("insert_order");

    /** Parse field for the "priority" response property. */
    protected static final ParseField PRIORITY_FIELD = new ParseField("priority");

    /** Parse field for the "source" response property. */
    protected static final ParseField SOURCE_FIELD = new ParseField("source");

    /** Parse field for the "time_in_queue_millis" response property. */
    protected static final ParseField TIME_IN_QUEUE_MILLIS_FIELD = new ParseField("time_in_queue_millis");

    /** Parse field for the "time_in_execution_millis" response property. */
    protected static final ParseField TIME_IN_EXECUTION_MILLIS_FIELD = new ParseField("time_in_execution_millis");

    /** Parse field for the "executing" response property. */
    protected static final ParseField EXECUTING_FIELD = new ParseField("executing");

    /** Parse field for the "total" response property. */
    protected static final ParseField TOTAL_FIELD = new ParseField("total");

    /** Parse field for the "successful" response property. */
    protected static final ParseField SUCCESSFUL_FIELD = new ParseField("successful");

    /** Parse field for the "failed" response property. */
    protected static final ParseField FAILED_FIELD = new ParseField("failed");

    /** Parse field for the "failures" response property. */
    protected static final ParseField FAILURES_FIELD = new ParseField("failures");

    /** Parse field for the "state" response property. */
    protected static final ParseField STATE_FIELD = new ParseField("state");

    /** Parse field for the "primary" response property. */
    protected static final ParseField PRIMARY_FIELD = new ParseField("primary");

    /** Parse field for the "node" response property. */
    protected static final ParseField NODE_FIELD = new ParseField("node");

    /** Parse field for the "relocating_node" response property. */
    protected static final ParseField RELOCATING_NODE_FIELD = new ParseField("relocating_node");

    /** Parse field for the "expected_shard_size_in_bytes" response property. */
    protected static final ParseField EXPECTED_SHARD_SIZE_IN_BYTES_FIELD = new ParseField("expected_shard_size_in_bytes");

    /** Parse field for the "routing" response property. */
    protected static final ParseField ROUTING_FIELD = new ParseField("routing");

    /** Parse field for the "full_name" response property. */
    protected static final ParseField FULL_NAME_FIELD = new ParseField("full_name");

    /** Parse field for the "mapping" response property. */
    protected static final ParseField MAPPING_FIELD = new ParseField("mapping");

    /** Parse field for the "unassigned_info" response property. */
    protected static final ParseField UNASSIGNED_INFO_FIELD = new ParseField("unassigned_info");

    /** Parse field for the "allocation_id" response property. */
    protected static final ParseField ALLOCATION_ID_FIELD = new ParseField("allocation_id");

    /** Parse field for the "recovery_source" response property. */
    protected static final ParseField RECOVERY_SOURCE_FIELD = new ParseField("recovery_source");

    /** Parse field for the "at" response property. */
    protected static final ParseField AT_FIELD = new ParseField("at");

    /** Parse field for the "failed_attempts" response property. */
    protected static final ParseField FAILED_ATTEMPTS_FIELD = new ParseField("failed_attempts");

    /** Parse field for the "allocation_status" response property. */
    protected static final ParseField ALLOCATION_STATUS_FIELD = new ParseField("allocation_status");

    /** Parse field for the "delayed" response property. */
    protected static final ParseField DELAYED_FIELD = new ParseField("delayed");

    /** Factory that creates a GET request for a URL. */
    protected static final Function<String, CurlRequest> GET = Curl::get;

    /** Factory that creates a POST request for a URL. */
    protected static final Function<String, CurlRequest> POST = Curl::post;

    /** Factory that creates a PUT request for a URL. */
    protected static final Function<String, CurlRequest> PUT = Curl::put;

    /** Factory that creates a DELETE request for a URL. */
    protected static final Function<String, CurlRequest> DELETE = Curl::delete;

    /** Factory that creates a HEAD request for a URL. */
    protected static final Function<String, CurlRequest> HEAD = Curl::head;

    /** The HTTP client used to send requests. */
    protected final HttpClient client;

    /**
     * Creates a new action bound to the given HTTP client.
     *
     * @param client the HTTP client used to send requests
     */
    public HttpAction(final HttpClient client) {
        this.client = client;
    }

    /**
     * Creates an XContent parser for the body of the given HTTP response, choosing the
     * content type from the response's Content-Type header.
     *
     * @param response the HTTP response to parse
     * @return a parser over the response body
     * @throws IOException if the parser cannot be created
     */
    protected XContentParser createParser(final CurlResponse response) throws IOException {
        final String contentType = response.getHeaderValue("Content-Type");
        final MediaType mediaType = fromMediaTypeOrFormat(contentType);
        final XContent xContent = mediaType.xContent();
        return xContent.createParser(client.getNamedXContentRegistry(), LoggingDeprecationHandler.INSTANCE, response.getContentAsStream());
    }

    /**
     * Converts an error HTTP response into an {@link OpenSearchStatusException}, parsing the
     * error body if possible and falling back to the raw content and HTTP status code.
     *
     * @param response the HTTP response containing the error
     * @param t the original throwable to attach as a suppressed exception
     * @return the exception describing the error response
     */
    protected OpenSearchStatusException toOpenSearchException(final CurlResponse response, final Throwable t) {
        OpenSearchStatusException fesenException;
        try (final XContentParser parser = createParser(response)) {
            fesenException = BytesRestResponse.errorFromXContent(parser);
            fesenException.addSuppressed(t);
            fesenException.addSuppressed(new CurlResponseException(response.getContentAsString()));
        } catch (final Exception ex) {
            fesenException =
                    new OpenSearchStatusException(response.getContentAsString(), RestStatus.fromCode(response.getHttpStatusCode()), t);
            fesenException.addSuppressed(t);
            fesenException.addSuppressed(ex);
        }
        return fesenException;
    }

    /**
     * Notifies the listener of a failure, unwrapping the cause if it is an
     * {@link OpenSearchException}.
     *
     * @param <T> the response type of the listener
     * @param listener the listener to notify
     * @param e the exception that occurred
     */
    protected <T> void unwrapOpenSearchException(final ActionListener<T> listener, final Exception e) {
        if (e.getCause() instanceof OpenSearchException) {
            listener.onFailure((OpenSearchException) e.getCause());
        } else {
            listener.onFailure(e);
        }
    }

    /**
     * Extracts the numeric value from an {@link ActiveShardCount} via its serialized form.
     *
     * @param activeShardCount the active shard count to convert
     * @return the numeric active shard count value
     */
    protected int getActiveShardsCountValue(final ActiveShardCount activeShardCount) {
        try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
            activeShardCount.writeTo(out);
            return out.toStreamInput().readInt();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
    }

    /**
     * Serializes an {@link ActiveShardCount} to its {@code wait_for_active_shards} query-parameter form:
     * {@code "all"} for {@link ActiveShardCount#ALL}, otherwise the numeric value (e.g. {@code "0"} for NONE, {@code "2"} for a count).
     *
     * @param activeShardCount the active shard count to serialize
     * @return the query-parameter string representation
     */
    protected String getActiveShardsCountString(final ActiveShardCount activeShardCount) {
        if (ActiveShardCount.ALL.equals(activeShardCount)) {
            return "all";
        }
        return Integer.toString(getActiveShardsCountValue(activeShardCount));
    }

    /**
     * Appends the standard indices options query parameters ({@code ignore_unavailable},
     * {@code allow_no_indices}, and {@code expand_wildcards}) to the given curl request.
     *
     * @param curlRequest the curl request to append the parameters to
     * @param indicesOptions the indices options to serialize
     */
    protected void appendIndicesOptions(final CurlRequest curlRequest, final IndicesOptions indicesOptions) {
        curlRequest.param("ignore_unavailable", Boolean.toString(indicesOptions.ignoreUnavailable()));
        curlRequest.param("allow_no_indices", Boolean.toString(indicesOptions.allowNoIndices()));
        curlRequest.param("expand_wildcards", expandWildcards(indicesOptions));
    }

    /**
     * Serializes the wildcard expansion flags of the given indices options into the
     * comma-separated {@code expand_wildcards} query parameter value.
     *
     * @param indicesOptions the indices options to serialize
     * @return the {@code expand_wildcards} value ({@code none} when no wildcard state is enabled)
     */
    protected static String expandWildcards(final IndicesOptions indicesOptions) {
        final StringBuilder buf = new StringBuilder();
        if (indicesOptions.expandWildcardsOpen()) {
            buf.append("open");
        }
        if (indicesOptions.expandWildcardsClosed()) {
            if (buf.length() > 0) {
                buf.append(',');
            }
            buf.append("closed");
        }
        if (indicesOptions.expandWildcardsHidden()) {
            if (buf.length() > 0) {
                buf.append(',');
            }
            buf.append("hidden");
        }
        if (buf.length() == 0) {
            buf.append("none");
        }
        return buf.toString();
    }

    /**
     * Exception that carries the raw content of an HTTP response, attached as a suppressed
     * exception for diagnostic purposes.
     */
    protected static class CurlResponseException extends Exception {
        private static final long serialVersionUID = 1L;

        CurlResponseException(final String msg) {
            super(msg, null, false, false);
        }
    }

    /**
     * Resolves a media type string (such as a Content-Type header value) to a known
     * {@link XContentType}, defaulting to JSON if no match is found.
     *
     * @param mediaType the media type string, may be {@code null}
     * @return the matching media type, or JSON if none matches
     */
    protected static MediaType fromMediaTypeOrFormat(final String mediaType) {
        if (mediaType != null) {
            for (final XContentType type : XContentType.values()) {
                if (isSameMediaTypeOrFormatAs(mediaType, type)) {
                    return type;
                }
            }
        }
        return XContentType.JSON;
    }

    private static boolean isSameMediaTypeOrFormatAs(final String stringType, final XContentType type) {
        return type.mediaTypeWithoutParameters().equalsIgnoreCase(stringType)
                || stringType.toLowerCase(Locale.ROOT).startsWith(type.mediaTypeWithoutParameters().toLowerCase(Locale.ROOT) + ";")
                || type.subtype().equalsIgnoreCase(stringType);
    }

}
