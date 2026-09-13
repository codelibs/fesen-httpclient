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
package org.codelibs.fesen.client;

import static java.util.stream.Collectors.toList;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import org.codelibs.curl.Curl;
import org.codelibs.curl.CurlException;
import org.codelibs.curl.CurlRequest;
import org.codelibs.curl.CurlResponse;
import org.codelibs.fesen.client.action.HttpAnalyzeAction;
import org.codelibs.fesen.client.action.HttpBulkAction;
import org.codelibs.fesen.client.action.HttpCloseIndexAction;
import org.codelibs.fesen.client.action.HttpClusterHealthAction;
import org.codelibs.fesen.client.action.HttpCreateIndexAction;
import org.codelibs.fesen.client.action.HttpCreatePitAction;
import org.codelibs.fesen.client.action.HttpDeleteAction;
import org.codelibs.fesen.client.action.HttpDeleteIndexAction;
import org.codelibs.fesen.client.action.HttpDeletePitAction;
import org.codelibs.fesen.client.action.HttpFlushAction;
import org.codelibs.fesen.client.action.HttpGetAction;
import org.codelibs.fesen.client.action.HttpGetAliasesAction;
import org.codelibs.fesen.client.action.HttpGetIndexAction;
import org.codelibs.fesen.client.action.HttpGetMappingsAction;
import org.codelibs.fesen.client.action.HttpGetSettingsAction;
import org.codelibs.fesen.client.action.HttpIndexAction;
import org.codelibs.fesen.client.action.HttpIndicesAliasesAction;
import org.codelibs.fesen.client.action.HttpIndicesExistsAction;
import org.codelibs.fesen.client.action.HttpNodesHotThreadsAction;
import org.codelibs.fesen.client.action.HttpNodesStatsAction;
import org.codelibs.fesen.client.action.HttpOpenIndexAction;
import org.codelibs.fesen.client.action.HttpPutMappingAction;
import org.codelibs.fesen.client.action.HttpRefreshAction;
import org.codelibs.fesen.client.action.HttpSearchAction;
import org.codelibs.fesen.client.action.HttpUpdateAction;
import org.codelibs.fesen.client.curl.FesenRequest;
import org.codelibs.fesen.client.node.NodeManager;
import org.codelibs.fesen.client.util.UrlUtils;
import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.action.ActionRequest;
import org.codelibs.fesen.opensearch.action.ActionType;
import org.codelibs.fesen.opensearch.action.admin.cluster.health.ClusterHealthAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.IndicesAliasesAction;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.get.GetAliasesAction;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.analyze.AnalyzeAction;
import org.codelibs.fesen.opensearch.action.admin.indices.close.CloseIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.close.CloseIndexRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.create.CreateIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.delete.DeleteIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.flush.FlushAction;
import org.codelibs.fesen.opensearch.action.admin.indices.flush.FlushRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.flush.FlushResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.get.GetIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.get.GetIndexRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.get.GetIndexResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.put.PutMappingAction;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.open.OpenIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.open.OpenIndexRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.refresh.RefreshAction;
import org.codelibs.fesen.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.settings.get.GetSettingsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.codelibs.fesen.opensearch.action.bulk.BulkAction;
import org.codelibs.fesen.opensearch.action.bulk.BulkRequest;
import org.codelibs.fesen.opensearch.action.bulk.BulkResponse;
import org.codelibs.fesen.opensearch.action.delete.DeleteAction;
import org.codelibs.fesen.opensearch.action.delete.DeleteRequest;
import org.codelibs.fesen.opensearch.action.delete.DeleteResponse;
import org.codelibs.fesen.opensearch.action.explain.ExplainAction;
import org.codelibs.fesen.opensearch.action.explain.ExplainRequest;
import org.codelibs.fesen.opensearch.action.explain.ExplainResponse;
import org.codelibs.fesen.opensearch.action.fieldcaps.FieldCapabilitiesAction;
import org.codelibs.fesen.opensearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.codelibs.fesen.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.codelibs.fesen.opensearch.action.get.GetAction;
import org.codelibs.fesen.opensearch.action.get.GetRequest;
import org.codelibs.fesen.opensearch.action.get.GetResponse;
import org.codelibs.fesen.opensearch.action.get.MultiGetAction;
import org.codelibs.fesen.opensearch.action.get.MultiGetRequest;
import org.codelibs.fesen.opensearch.action.get.MultiGetResponse;
import org.codelibs.fesen.opensearch.action.index.IndexAction;
import org.codelibs.fesen.opensearch.action.index.IndexRequest;
import org.codelibs.fesen.opensearch.action.index.IndexResponse;
import org.codelibs.fesen.opensearch.action.search.ClearScrollAction;
import org.codelibs.fesen.opensearch.action.search.ClearScrollRequest;
import org.codelibs.fesen.opensearch.action.search.ClearScrollResponse;
import org.codelibs.fesen.opensearch.action.search.CreatePitAction;
import org.codelibs.fesen.opensearch.action.search.CreatePitRequest;
import org.codelibs.fesen.opensearch.action.search.CreatePitResponse;
import org.codelibs.fesen.opensearch.action.search.DeletePitAction;
import org.codelibs.fesen.opensearch.action.search.DeletePitRequest;
import org.codelibs.fesen.opensearch.action.search.DeletePitResponse;
import org.codelibs.fesen.opensearch.action.search.GetAllPitNodesRequest;
import org.codelibs.fesen.opensearch.action.search.GetAllPitNodesResponse;
import org.codelibs.fesen.opensearch.action.search.MultiSearchAction;
import org.codelibs.fesen.opensearch.action.search.MultiSearchRequest;
import org.codelibs.fesen.opensearch.action.search.MultiSearchResponse;
import org.codelibs.fesen.opensearch.action.search.SearchAction;
import org.codelibs.fesen.opensearch.action.search.SearchRequest;
import org.codelibs.fesen.opensearch.action.search.SearchRequestBuilder;
import org.codelibs.fesen.opensearch.action.search.SearchResponse;
import org.codelibs.fesen.opensearch.action.search.SearchScrollAction;
import org.codelibs.fesen.opensearch.action.search.SearchScrollRequest;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.action.termvectors.MultiTermVectorsAction;
import org.codelibs.fesen.opensearch.action.termvectors.MultiTermVectorsRequest;
import org.codelibs.fesen.opensearch.action.termvectors.MultiTermVectorsResponse;
import org.codelibs.fesen.opensearch.action.termvectors.TermVectorsAction;
import org.codelibs.fesen.opensearch.action.termvectors.TermVectorsRequest;
import org.codelibs.fesen.opensearch.action.termvectors.TermVectorsResponse;
import org.codelibs.fesen.opensearch.action.update.UpdateAction;
import org.codelibs.fesen.opensearch.action.update.UpdateRequest;
import org.codelibs.fesen.opensearch.action.update.UpdateResponse;
import org.codelibs.fesen.opensearch.common.action.ActionFuture;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.codelibs.fesen.opensearch.common.xcontent.json.JsonXContent;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.action.ActionResponse;
import org.codelibs.fesen.opensearch.core.xcontent.ContextParser;
import org.codelibs.fesen.opensearch.core.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.opensearch.index.reindex.BulkByScrollResponse;
import org.codelibs.fesen.opensearch.index.reindex.UpdateByQueryRequest;
import org.codelibs.fesen.opensearch.plugins.spi.NamedXContentProvider;
import org.codelibs.fesen.opensearch.search.aggregations.Aggregation;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.adjacency.ParsedAdjacencyMatrix;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.composite.ParsedComposite;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.filter.ParsedFilter;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.filter.ParsedFilters;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.global.ParsedGlobal;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.histogram.ParsedHistogram;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.missing.ParsedMissing;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.nested.ParsedNested;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.nested.ParsedReverseNested;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.range.ParsedBinaryRange;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.range.ParsedDateRange;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.range.ParsedGeoDistance;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.range.ParsedRange;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.sampler.InternalSampler;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.sampler.ParsedSampler;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.ParsedSignificantLongTerms;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.ParsedSignificantStringTerms;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.SignificantLongTerms;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.SignificantStringTerms;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ParsedAvg;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ParsedCardinality;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ParsedExtendedStats;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ParsedGeoCentroid;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ParsedHDRPercentileRanks;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ParsedHDRPercentiles;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ParsedMax;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ParsedMin;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ParsedScriptedMetric;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ParsedStats;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ParsedSum;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ParsedTDigestPercentileRanks;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ParsedTDigestPercentiles;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ParsedTopHits;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ParsedValueCount;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.pipeline.ExtendedStatsBucketPipelineAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.pipeline.InternalBucketMetricValue;
import org.codelibs.fesen.opensearch.search.aggregations.pipeline.InternalSimpleValue;
import org.codelibs.fesen.opensearch.search.aggregations.pipeline.ParsedBucketMetricValue;
import org.codelibs.fesen.opensearch.search.aggregations.pipeline.ParsedDerivative;
import org.codelibs.fesen.opensearch.search.aggregations.pipeline.ParsedExtendedStatsBucket;
import org.codelibs.fesen.opensearch.search.aggregations.pipeline.ParsedPercentilesBucket;
import org.codelibs.fesen.opensearch.search.aggregations.pipeline.ParsedSimpleValue;
import org.codelibs.fesen.opensearch.search.aggregations.pipeline.ParsedStatsBucket;
import org.codelibs.fesen.opensearch.search.aggregations.pipeline.PercentilesBucketPipelineAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.client.AdminClient;

/**
 * An OpenSearch/Elasticsearch client implementation that communicates with the
 * search engine over HTTP. It maps OpenSearch action types to HTTP-based action
 * implementations and supports features such as basic authentication, SSL,
 * compression, and HTTP proxies.
 */
public class HttpClient extends HttpAbstractClient {

    /** A factory that creates a curl request using the HTTP GET method. */
    protected static final Function<String, CurlRequest> GET = Curl::get;

    /** A factory that creates a curl request using the HTTP POST method. */
    protected static final Function<String, CurlRequest> POST = Curl::post;

    /** A factory that creates a curl request using the HTTP PUT method. */
    protected static final Function<String, CurlRequest> PUT = Curl::put;

    /** A factory that creates a curl request using the HTTP DELETE method. */
    protected static final Function<String, CurlRequest> DELETE = Curl::delete;

    /** A factory that creates a curl request using the HTTP HEAD method. */
    protected static final Function<String, CurlRequest> HEAD = Curl::head;

    /** The manager that tracks the availability of the configured engine nodes. */
    protected NodeManager nodeManager;

    /** A mapping from action types to their HTTP-based action executors. */
    protected final Map<ActionType<?>, BiConsumer<ActionRequest, ActionListener<?>>> actions = new HashMap<>();

    /** The registry of named XContent parsers used to parse responses. */
    protected final NamedXContentRegistry namedXContentRegistry;

    /** The thread pool used to execute HTTP requests. */
    protected final ForkJoinPool threadPool;

    /** The Basic authentication header value, or null if not configured. */
    protected final String basicAuth;

    private final SSLSocketFactory sslSocketFactory;

    /** Whether gzip compression is enabled for HTTP requests. */
    protected final boolean compression;

    /** The connect timeout in milliseconds; 0 or negative means unlimited (not set). */
    protected final int connectionTimeout;

    /** The socket (read) timeout in milliseconds; 0 or negative means unlimited (not set). */
    protected final int socketTimeout;

    /** The HTTP proxy to use for requests, or null if not configured. */
    protected final Proxy proxy;

    /** The Proxy-Authorization header value, or null if not configured. */
    protected final String proxyAuth;

    /** Operators applied to each curl request before it is sent, allowing request customization. */
    protected final List<UnaryOperator<CurlRequest>> requestBuilderList = new ArrayList<>();

    private EngineInfo engineInfo;

    /**
     * The content type of an HTTP request body.
     */
    public enum ContentType {
        /** The application/json content type. */
        JSON("application/json"),
        /** The application/x-ndjson content type, used for bulk requests. */
        X_NDJSON("application/x-ndjson");

        private final String value;

        ContentType(final String value) {
            this.value = value;
        }

        /**
         * Returns the MIME type string of this content type.
         *
         * @return the MIME type string
         */
        public String getString() {
            return this.value;
        }
    }

    /**
     * Creates a new HTTP client with the given settings and thread pool.
     *
     * @param settings the client settings, including http.hosts and authentication options
     * @param threadPool the thread pool passed to the underlying client
     */
    public HttpClient(final Settings settings, final ThreadPool threadPool) {
        this(settings, threadPool, Collections.emptyList());
    }

    /**
     * Creates a new HTTP client with the given settings, thread pool, and
     * additional named XContent entries used to parse responses.
     *
     * @param settings the client settings, including http.hosts and authentication options
     * @param threadPool the thread pool passed to the underlying client
     * @param namedXContentEntries additional named XContent entries to register
     * @throws OpenSearchException if http.hosts is empty
     */
    public HttpClient(final Settings settings, final ThreadPool threadPool, final List<NamedXContentRegistry.Entry> namedXContentEntries) {
        super(settings, threadPool);
        final String[] hosts = settings.getAsList("http.hosts").stream().map(s -> {
            if (!s.startsWith("http:") && !s.startsWith("https:")) {
                return "http://" + s;
            }
            return s;
        }).toArray(n -> new String[n]);
        if (hosts.length == 0) {
            throw new OpenSearchException("http.hosts is empty.");
        }
        nodeManager = new NodeManager(hosts, this);
        nodeManager.setHeartbeatInterval(settings.getAsLong("http.heartbeat_interval", 10000L));

        compression = settings.getAsBoolean("http.compression", true);
        connectionTimeout = settings.getAsInt("http.connection_timeout", 0);
        socketTimeout = settings.getAsInt("http.socket_timeout", 0);
        basicAuth = createBasicAuthentication(settings);
        sslSocketFactory = createSSLSocketFactory(settings);
        proxy = createProxy(settings);
        proxyAuth = createProxyAuthentication(settings);
        this.threadPool = createThreadPool(settings);

        namedXContentRegistry = new NamedXContentRegistry(
                Stream.of(getDefaultNamedXContents().stream(), getProvidedNamedXContents().stream(), namedXContentEntries.stream())
                        .flatMap(Function.identity()).collect(toList()));

        actions.put(SearchAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.search.SearchAction
            @SuppressWarnings("unchecked")
            final ActionListener<SearchResponse> actionListener = (ActionListener<SearchResponse>) listener;
            new HttpSearchAction(this, SearchAction.INSTANCE).execute((SearchRequest) request, actionListener);
        });
        actions.put(RefreshAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.refresh.RefreshAction
            @SuppressWarnings("unchecked")
            final ActionListener<RefreshResponse> actionListener = (ActionListener<RefreshResponse>) listener;
            new HttpRefreshAction(this, RefreshAction.INSTANCE).execute((RefreshRequest) request, actionListener);
        });
        actions.put(CreateIndexAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.create.CreateIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<CreateIndexResponse> actionListener = (ActionListener<CreateIndexResponse>) listener;
            new HttpCreateIndexAction(this, CreateIndexAction.INSTANCE).execute((CreateIndexRequest) request, actionListener);
        });
        actions.put(DeleteIndexAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.delete.DeleteIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpDeleteIndexAction(this, DeleteIndexAction.INSTANCE).execute((DeleteIndexRequest) request, actionListener);
        });
        actions.put(GetIndexAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.get.GetIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetIndexResponse> actionListener = (ActionListener<GetIndexResponse>) listener;
            new HttpGetIndexAction(this, GetIndexAction.INSTANCE).execute((GetIndexRequest) request, actionListener);
        });
        actions.put(OpenIndexAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.open.OpenIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<OpenIndexResponse> actionListener = (ActionListener<OpenIndexResponse>) listener;
            new HttpOpenIndexAction(this, OpenIndexAction.INSTANCE).execute((OpenIndexRequest) request, actionListener);
        });
        actions.put(CloseIndexAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.close.CloseIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<CloseIndexResponse> actionListener = (ActionListener<CloseIndexResponse>) listener;
            new HttpCloseIndexAction(this, CloseIndexAction.INSTANCE).execute((CloseIndexRequest) request, actionListener);
        });
        actions.put(IndicesExistsAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.exists.indices.IndicesExistsAction
            @SuppressWarnings("unchecked")
            final ActionListener<IndicesExistsResponse> actionListener = (ActionListener<IndicesExistsResponse>) listener;
            new HttpIndicesExistsAction(this, IndicesExistsAction.INSTANCE).execute((IndicesExistsRequest) request, actionListener);
        });
        actions.put(IndicesAliasesAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.alias.IndicesAliasesAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpIndicesAliasesAction(this, IndicesAliasesAction.INSTANCE).execute((IndicesAliasesRequest) request, actionListener);
        });
        actions.put(PutMappingAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.mapping.put.PutMappingAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpPutMappingAction(this, PutMappingAction.INSTANCE).execute((PutMappingRequest) request, actionListener);
        });
        actions.put(GetMappingsAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.mapping.get.GetMappingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetMappingsResponse> actionListener = (ActionListener<GetMappingsResponse>) listener;
            new HttpGetMappingsAction(this, GetMappingsAction.INSTANCE).execute((GetMappingsRequest) request, actionListener);
        });
        actions.put(FlushAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.flush.FlushAction
            @SuppressWarnings("unchecked")
            final ActionListener<FlushResponse> actionListener = (ActionListener<FlushResponse>) listener;
            new HttpFlushAction(this, FlushAction.INSTANCE).execute((FlushRequest) request, actionListener);
        });
        actions.put(IndexAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.index.IndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<IndexResponse> actionListener = (ActionListener<IndexResponse>) listener;
            new HttpIndexAction(this, IndexAction.INSTANCE).execute((IndexRequest) request, actionListener);
        });
        actions.put(GetAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.get.GetAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetResponse> actionListener = (ActionListener<GetResponse>) listener;
            new HttpGetAction(this, GetAction.INSTANCE).execute((GetRequest) request, actionListener);
        });
        actions.put(UpdateAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.update.UpdateAction
            @SuppressWarnings("unchecked")
            final ActionListener<UpdateResponse> actionListener = (ActionListener<UpdateResponse>) listener;
            new HttpUpdateAction(this, UpdateAction.INSTANCE).execute((UpdateRequest) request, actionListener);
        });
        actions.put(BulkAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.bulk.BulkAction
            @SuppressWarnings("unchecked")
            final ActionListener<BulkResponse> actionListener = (ActionListener<BulkResponse>) listener;
            new HttpBulkAction(this, BulkAction.INSTANCE).execute((BulkRequest) request, actionListener);
        });
        actions.put(DeleteAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.delete.DeleteAction
            @SuppressWarnings("unchecked")
            final ActionListener<DeleteResponse> actionListener = (ActionListener<DeleteResponse>) listener;
            new HttpDeleteAction(this, DeleteAction.INSTANCE).execute((DeleteRequest) request, actionListener);
        });
        actions.put(GetSettingsAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.settings.get.GetSettingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetSettingsResponse> actionListener = (ActionListener<GetSettingsResponse>) listener;
            new HttpGetSettingsAction(this, GetSettingsAction.INSTANCE).execute((GetSettingsRequest) request, actionListener);
        });
        actions.put(ClusterHealthAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.health.ClusterHealthAction
            @SuppressWarnings("unchecked")
            final ActionListener<ClusterHealthResponse> actionListener = (ActionListener<ClusterHealthResponse>) listener;
            new HttpClusterHealthAction(this, ClusterHealthAction.INSTANCE).execute((ClusterHealthRequest) request, actionListener);
        });
        actions.put(GetAliasesAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.alias.get.GetAliasesAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetAliasesResponse> actionListener = (ActionListener<GetAliasesResponse>) listener;
            new HttpGetAliasesAction(this, GetAliasesAction.INSTANCE).execute((GetAliasesRequest) request, actionListener);
        });
        actions.put(AnalyzeAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.analyze.AnalyzeAction
            @SuppressWarnings("unchecked")
            final ActionListener<AnalyzeAction.Response> actionListener = (ActionListener<AnalyzeAction.Response>) listener;
            new HttpAnalyzeAction(this, AnalyzeAction.INSTANCE).execute((AnalyzeAction.Request) request, actionListener);
        });
        actions.put(NodesStatsAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.node.stats.NodesStatsAction
            @SuppressWarnings("unchecked")
            final ActionListener<NodesStatsResponse> actionListener = (ActionListener<NodesStatsResponse>) listener;
            new HttpNodesStatsAction(this, NodesStatsAction.INSTANCE).execute((NodesStatsRequest) request, actionListener);
        });
        actions.put(NodesHotThreadsAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.node.hotthreads.NodesHotThreadsAction
            @SuppressWarnings("unchecked")
            final ActionListener<NodesHotThreadsResponse> actionListener = (ActionListener<NodesHotThreadsResponse>) listener;
            new HttpNodesHotThreadsAction(this, NodesHotThreadsAction.INSTANCE).execute((NodesHotThreadsRequest) request, actionListener);
        });

        // View API

        // Streaming Ingestion API

        // Scale (Search-Only) API

        // Remote Store Metadata API

        // Reindex / Update-By-Query / Delete-By-Query APIs

        // Point-in-Time (PIT) APIs
        actions.put(CreatePitAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.opensearch.action.search.CreatePitAction
            @SuppressWarnings("unchecked")
            final ActionListener<CreatePitResponse> actionListener = (ActionListener<CreatePitResponse>) listener;
            new HttpCreatePitAction(this, CreatePitAction.INSTANCE).execute((CreatePitRequest) request, actionListener);
        });
        actions.put(DeletePitAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.opensearch.action.search.DeletePitAction
            @SuppressWarnings("unchecked")
            final ActionListener<DeletePitResponse> actionListener = (ActionListener<DeletePitResponse>) listener;
            new HttpDeletePitAction(this, DeletePitAction.INSTANCE).execute((DeletePitRequest) request, actionListener);
        });

        // Resolve Index API
    }

    @Override
    public AdminClient admin() {
        return new HttpAdminClient(super.admin());
    }

    /**
     * Returns information about the backend engine by accessing the root
     * endpoint. The result is cached after the first successful call.
     *
     * @return the engine information
     * @throws OpenSearchException if the engine information cannot be retrieved
     */
    public EngineInfo getEngineInfo() {
        if (engineInfo != null) {
            return engineInfo;
        }

        synchronized (this) {
            if (engineInfo != null) {
                return engineInfo;
            }
            try (final CurlResponse response = getCurlRequest(Curl::get, "/").execute()) {
                if (response.getHttpStatusCode() == 200) {
                    final Map<String, Object> content = response.getContent(res -> {
                        try (InputStream is = res.getContentAsStream()) {
                            return JsonXContent.jsonXContent
                                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, is).map();
                        } catch (final Exception e) {
                            throw new CurlException("Failed to access the content.", e);
                        }
                    });
                    engineInfo = new EngineInfo(content);
                    return engineInfo;
                }
            } catch (final Exception e) {
                logger.warn("Failed to access status.", e);
            }
        }
        throw new OpenSearchException("Unknown server info: {}", nodeManager.toNodeString());
    }

    /**
     * Creates a Basic authentication header value from the fesen.username and
     * fesen.password settings.
     *
     * @param settings the client settings
     * @return the Basic authentication header value, or null if the username or password is not set
     */
    protected String createBasicAuthentication(final Settings settings) {
        final String username = settings.get("fesen.username");
        final String password = settings.get("fesen.password");
        if (username != null && password != null) {
            final String value = username + ":" + password;
            return "Basic " + java.util.Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
        }
        return null;
    }

    /**
     * Creates an SSL socket factory that trusts the certificate specified by
     * the http.ssl.certificate_authorities setting.
     *
     * @param settings the client settings
     * @return the SSL socket factory, or null if the setting is not configured or the certificate cannot be loaded
     */
    protected SSLSocketFactory createSSLSocketFactory(final Settings settings) {
        final String certificateAuthorities = settings.get("http.ssl.certificate_authorities");
        if (logger.isDebugEnabled()) {
            logger.debug("http.ssl.certificate_authorities: {}", certificateAuthorities);
        }
        if (certificateAuthorities == null) {
            return null;
        }
        try (final InputStream in = new FileInputStream(certificateAuthorities)) {
            final Certificate certificate = CertificateFactory.getInstance("X.509").generateCertificate(in);

            final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null, null);
            keyStore.setCertificateEntry("server", certificate);

            final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(keyStore);

            final SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
            return sslContext.getSocketFactory();
        } catch (final Exception e) {
            logger.warn("Failed to load {}", certificateAuthorities, e);
        }
        return null;
    }

    /**
     * Creates an HTTP proxy from the https.proxy_host/https.proxy_port or
     * http.proxy_host/http.proxy_port settings.
     *
     * @param settings the client settings
     * @return the proxy, or null if the proxy host or port is not configured
     */
    protected Proxy createProxy(final Settings settings) {
        final String host = getFromSettings(settings, "https.proxy_host", "http.proxy_host");
        final String port = getFromSettings(settings, "https.proxy_port", "http.proxy_port");
        if (host != null && port != null) {
            return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(host, Integer.parseInt(port)));
        }
        return null;
    }

    /**
     * Creates a Proxy-Authorization header value from the
     * https.proxy_username/https.proxy_password or
     * http.proxy_username/http.proxy_password settings.
     *
     * @param settings the client settings
     * @return the proxy authentication header value, or null if the username or password is not set
     */
    protected String createProxyAuthentication(final Settings settings) {
        final String username = getFromSettings(settings, "https.proxy_username", "http.proxy_username");
        final String password = getFromSettings(settings, "https.proxy_password", "http.proxy_password");
        if (username != null && password != null) {
            final String value = username + ":" + password;
            return "Basic " + java.util.Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
        }
        return null;
    }

    /**
     * Returns the value of the first setting key if present, otherwise the
     * value of the second setting key.
     *
     * @param settings the client settings
     * @param key1 the preferred setting key
     * @param key2 the fallback setting key
     * @return the setting value, or null if neither key is set
     */
    protected String getFromSettings(final Settings settings, final String key1, final String key2) {
        final String value1 = settings.get(key1);
        if (value1 != null) {
            return value1;
        }
        return settings.get(key2);
    }

    @Override
    public void close() {
        try {
            nodeManager.close();
        } catch (final Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to close node manager.", e);
            }
        }
        if (!threadPool.isShutdown()) {
            try {
                threadPool.shutdown();
                if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Thread pool did not terminate within 60 seconds. Forcing shutdown.");
                    }
                }
            } catch (final InterruptedException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Thread pool shutdown interrupted.", e);
                }
                Thread.currentThread().interrupt();
            } finally {
                threadPool.shutdownNow();
            }
        }
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(final ActionType<Response> action,
            final Request request, final ActionListener<Response> listener) {
        final BiConsumer<ActionRequest, ActionListener<?>> httpAction = actions.get(action);
        if (httpAction == null) {
            throw new UnsupportedOperationException("Action: " + action.name());
        }
        httpAction.accept(request, listener);
    }

    /**
     * Creates a curl request for the given HTTP method, path, and indices,
     * using the JSON content type.
     *
     * @param method the factory that creates a curl request for an HTTP method
     * @param path the request path appended after the indices
     * @param indices the target index names
     * @return the configured curl request
     */
    public CurlRequest getCurlRequest(final Function<String, CurlRequest> method, final String path, final String... indices) {
        return getCurlRequest(method, ContentType.JSON, path, indices);
    }

    /**
     * Creates a curl request for the given HTTP method, content type, path,
     * and indices. The request is bound to the node manager for failover.
     *
     * @param method the factory that creates a curl request for an HTTP method
     * @param contentType the content type of the request body
     * @param path the request path appended after the indices
     * @param indices the target index names
     * @return the configured curl request
     */
    public CurlRequest getCurlRequest(final Function<String, CurlRequest> method, final ContentType contentType, final String path,
            final String... indices) {
        return getPlainCurlRequest(s -> new FesenRequest(method.apply(null), nodeManager, s), contentType, path, indices);
    }

    /**
     * Creates a curl request from the given request creator and applies the
     * configured options such as content type, authentication, SSL,
     * compression, proxy, and registered request builders.
     *
     * @param requestCreator the function that creates a curl request from the built path
     * @param contentType the content type of the request body
     * @param path the request path appended after the indices
     * @param indices the target index names
     * @return the configured curl request
     */
    public CurlRequest getPlainCurlRequest(final Function<String, CurlRequest> requestCreator, final ContentType contentType,
            final String path, final String... indices) {
        final StringBuilder buf = new StringBuilder(100);
        if (indices.length > 0) {
            buf.append('/').append(UrlUtils.joinAndEncode(",", indices));
        }
        if (path != null) {
            buf.append(path);
        }
        CurlRequest request = requestCreator.apply(buf.toString()).header("Content-Type", contentType.getString()).threadPool(threadPool);
        if (connectionTimeout > 0 || socketTimeout > 0) {
            request.timeout(connectionTimeout, socketTimeout);
        }
        if (basicAuth != null) {
            request.header("Authorization", basicAuth);
        }
        if (sslSocketFactory != null) {
            request.sslSocketFactory(sslSocketFactory);
        }
        if (compression) {
            request.compression("gzip");
        }
        if (proxy != null) {
            request.proxy(proxy);
            if (proxyAuth != null) {
                request.header("Proxy-Authorization", proxyAuth);
            }
        }
        for (final UnaryOperator<CurlRequest> builder : requestBuilderList) {
            request = builder.apply(request);
        }
        return request;
    }

    /**
     * Creates the thread pool used to execute HTTP requests. The parallelism
     * is taken from the thread_pool.http.size or processors setting, and the
     * async mode from the thread_pool.http.async setting.
     *
     * @param settings the client settings
     * @return the created thread pool
     */
    protected ForkJoinPool createThreadPool(final Settings settings) {
        final int parallelism =
                settings.getAsInt("thread_pool.http.size", settings.getAsInt("processors", Runtime.getRuntime().availableProcessors()));
        final boolean asyncMode = settings.getAsBoolean("thread_pool.http.async", false);
        return new ForkJoinPool(parallelism, WorkerThread::new, (t, e) -> logger.warn("An exception has been raised by {}", t.getName(), e),
                asyncMode);
    }

    /**
     * Returns the default named XContent entries used to parse aggregation
     * responses.
     *
     * @return the default named XContent entries
     */
    protected List<NamedXContentRegistry.Entry> getDefaultNamedXContents() {
        // SearchModule.getNamedXContents() requires too many dependencies to instantiate.
        // Maintain the aggregation parser mappings manually.
        final Map<String, ContextParser<Object, ? extends Aggregation>> map = new HashMap<>();
        map.put(CardinalityAggregationBuilder.NAME, (p, c) -> ParsedCardinality.fromXContent(p, (String) c));
        map.put("hdr_percentiles", (p, c) -> ParsedHDRPercentiles.fromXContent(p, (String) c));
        map.put("hdr_percentile_ranks", (p, c) -> ParsedHDRPercentileRanks.fromXContent(p, (String) c));
        map.put("tdigest_percentiles", (p, c) -> ParsedTDigestPercentiles.fromXContent(p, (String) c));
        map.put("tdigest_percentile_ranks", (p, c) -> ParsedTDigestPercentileRanks.fromXContent(p, (String) c));
        map.put(PercentilesBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedPercentilesBucket.fromXContent(p, (String) c));
        map.put(MinAggregationBuilder.NAME, (p, c) -> ParsedMin.fromXContent(p, (String) c));
        map.put(MaxAggregationBuilder.NAME, (p, c) -> ParsedMax.fromXContent(p, (String) c));
        map.put(SumAggregationBuilder.NAME, (p, c) -> ParsedSum.fromXContent(p, (String) c));
        map.put(AvgAggregationBuilder.NAME, (p, c) -> ParsedAvg.fromXContent(p, (String) c));
        map.put(ValueCountAggregationBuilder.NAME, (p, c) -> ParsedValueCount.fromXContent(p, (String) c));
        map.put(InternalSimpleValue.NAME, (p, c) -> ParsedSimpleValue.fromXContent(p, (String) c));
        map.put(DerivativePipelineAggregationBuilder.NAME, (p, c) -> ParsedDerivative.fromXContent(p, (String) c));
        map.put(InternalBucketMetricValue.NAME, (p, c) -> ParsedBucketMetricValue.fromXContent(p, (String) c));
        map.put(StatsAggregationBuilder.NAME, (p, c) -> ParsedStats.fromXContent(p, (String) c));
        map.put(StatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedStatsBucket.fromXContent(p, (String) c));
        map.put(ExtendedStatsAggregationBuilder.NAME, (p, c) -> ParsedExtendedStats.fromXContent(p, (String) c));
        map.put(ExtendedStatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedExtendedStatsBucket.fromXContent(p, (String) c));
        map.put(GeoCentroidAggregationBuilder.NAME, (p, c) -> ParsedGeoCentroid.fromXContent(p, (String) c));
        map.put(HistogramAggregationBuilder.NAME, (p, c) -> ParsedHistogram.fromXContent(p, (String) c));
        map.put(DateHistogramAggregationBuilder.NAME, (p, c) -> ParsedDateHistogram.fromXContent(p, (String) c));
        //map.put(AutoDateHistogramAggregationBuilder.NAME, (p, c) -> ParsedAutoDateHistogram.fromXContent(p, (String) c));
        map.put(StringTerms.NAME, (p, c) -> ParsedStringTerms.fromXContent(p, (String) c));
        map.put(LongTerms.NAME, (p, c) -> ParsedLongTerms.fromXContent(p, (String) c));
        map.put(DoubleTerms.NAME, (p, c) -> ParsedDoubleTerms.fromXContent(p, (String) c));
        map.put(MissingAggregationBuilder.NAME, (p, c) -> ParsedMissing.fromXContent(p, (String) c));
        map.put(NestedAggregationBuilder.NAME, (p, c) -> ParsedNested.fromXContent(p, (String) c));
        map.put(ReverseNestedAggregationBuilder.NAME, (p, c) -> ParsedReverseNested.fromXContent(p, (String) c));
        map.put(GlobalAggregationBuilder.NAME, (p, c) -> ParsedGlobal.fromXContent(p, (String) c));
        map.put(FilterAggregationBuilder.NAME, (p, c) -> ParsedFilter.fromXContent(p, (String) c));
        map.put(InternalSampler.PARSER_NAME, (p, c) -> ParsedSampler.fromXContent(p, (String) c));
        map.put(RangeAggregationBuilder.NAME, (p, c) -> ParsedRange.fromXContent(p, (String) c));
        map.put(DateRangeAggregationBuilder.NAME, (p, c) -> ParsedDateRange.fromXContent(p, (String) c));
        map.put(GeoDistanceAggregationBuilder.NAME, (p, c) -> ParsedGeoDistance.fromXContent(p, (String) c));
        map.put(FiltersAggregationBuilder.NAME, (p, c) -> ParsedFilters.fromXContent(p, (String) c));
        map.put(AdjacencyMatrixAggregationBuilder.NAME, (p, c) -> ParsedAdjacencyMatrix.fromXContent(p, (String) c));
        map.put(SignificantLongTerms.NAME, (p, c) -> ParsedSignificantLongTerms.fromXContent(p, (String) c));
        map.put(SignificantStringTerms.NAME, (p, c) -> ParsedSignificantStringTerms.fromXContent(p, (String) c));
        map.put(ScriptedMetricAggregationBuilder.NAME, (p, c) -> ParsedScriptedMetric.fromXContent(p, (String) c));
        map.put(IpRangeAggregationBuilder.NAME, (p, c) -> ParsedBinaryRange.fromXContent(p, (String) c));
        map.put(TopHitsAggregationBuilder.NAME, (p, c) -> ParsedTopHits.fromXContent(p, (String) c));
        map.put(CompositeAggregationBuilder.NAME, (p, c) -> ParsedComposite.fromXContent(p, (String) c));

        //        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(TermSuggestion.NAME),
        //                (parser, context) -> TermSuggestion.fromXContent(parser, (String) context)));
        //        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(PhraseSuggestion.NAME),
        //                (parser, context) -> PhraseSuggestion.fromXContent(parser, (String) context)));
        //        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(CompletionSuggestion.NAME),
        //                (parser, context) -> CompletionSuggestion.fromXContent(parser, (String) context)));
        return map.entrySet().stream()
                .map(entry -> new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(entry.getKey()), entry.getValue()))
                .toList();
    }

    /**
     * Returns the named XContent entries provided by
     * {@link NamedXContentProvider} services discovered via the service loader.
     *
     * @return the named XContent entries from service providers
     */
    protected List<NamedXContentRegistry.Entry> getProvidedNamedXContents() {
        final List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        for (final NamedXContentProvider service : ServiceLoader.load(NamedXContentProvider.class)) {
            entries.addAll(service.getNamedXContentParsers());
        }
        return entries;
    }

    /**
     * Returns the registry of named XContent parsers used by this client.
     *
     * @return the named XContent registry
     */
    public NamedXContentRegistry getNamedXContentRegistry() {
        return namedXContentRegistry;
    }

    /**
     * Adds an operator that customizes each curl request before it is sent.
     *
     * @param builder the operator applied to each curl request
     */
    public void addRequestBuilder(final UnaryOperator<CurlRequest> builder) {
        requestBuilderList.add(builder);
    }

    /**
     * A worker thread for the HTTP request thread pool, named "eshttp".
     */
    protected static class WorkerThread extends ForkJoinWorkerThread {
        /**
         * Creates a new worker thread for the given pool.
         *
         * @param pool the fork-join pool this thread works in
         */
        protected WorkerThread(final ForkJoinPool pool) {
            super(pool);
            setName("eshttp");
        }
    }

    @Override
    public SearchRequestBuilder prepareStreamSearch(final String... indices) {
        return new SearchRequestBuilder(this, SearchAction.INSTANCE).setIndices(indices);
    }

}
