/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.search;

import org.codelibs.fesen.opensearch.action.FailedNodeException;
import org.codelibs.fesen.opensearch.action.support.nodes.BaseNodesResponse;
import org.codelibs.fesen.opensearch.cluster.ClusterName;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * This class transforms active PIT objects from all nodes to unique PIT objects
 *
 * @opensearch.api
 */
@PublicApi(since = "2.3.0")
public class GetAllPitNodesResponse extends BaseNodesResponse<GetAllPitNodeResponse> implements ToXContentObject {

    /**
     * List of unique PITs across all nodes
     */
    private final Set<ListPitInfo> pitInfos = new HashSet<>();

    /**
     * Creates a new GetAllPitNodesResponse by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public GetAllPitNodesResponse(StreamInput in) throws IOException {
        super(in);
        Set<String> uniquePitIds = new HashSet<>();
        pitInfos.addAll(
            getNodes().stream()
                .flatMap(p -> p.getPitInfos().stream().filter(t -> uniquePitIds.add(t.getPitId())))
                .collect(Collectors.toList())
        );
    }

    /**
     * Creates a new GetAllPitNodesResponse.
     *
     * @param listPitInfos the list pit infos
     * @param clusterName the cluster name
     * @param getAllPitNodeResponseList the get all pit node response list
     * @param failures the failures
     */
    public GetAllPitNodesResponse(
        List<ListPitInfo> listPitInfos,
        ClusterName clusterName,
        List<GetAllPitNodeResponse> getAllPitNodeResponseList,
        List<FailedNodeException> failures
    ) {
        super(clusterName, getAllPitNodeResponseList, failures);
        pitInfos.addAll(listPitInfos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("pits");
        for (ListPitInfo pit : pitInfos) {
            pit.toXContent(builder, params);
        }
        builder.endArray();
        if (!failures().isEmpty()) {
            builder.startArray("failures");
            for (FailedNodeException e : failures()) {
                e.toXContent(builder, params);
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public List<GetAllPitNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(GetAllPitNodeResponse::new);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<GetAllPitNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    /**
     * Returns the pit infos.
     *
     * @return the pit infos
     */
    public List<ListPitInfo> getPitInfos() {
        return Collections.unmodifiableList(new ArrayList<>(pitInfos));
    }

    private static final ConstructingObjectParser<GetAllPitNodesResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_all_pits_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            List<ListPitInfo> listPitInfos = (List<ListPitInfo>) parsedObjects[0];
            List<FailedNodeException> failures = null;
            if (parsedObjects.length > 1) {
                failures = (List<FailedNodeException>) parsedObjects[1];
            }
            if (failures == null) {
                failures = new ArrayList<>();
            }
            return new GetAllPitNodesResponse(listPitInfos, new ClusterName(""), new ArrayList<>(), failures);
        }
    );
    static {
        PARSER.declareObjectArray(constructorArg(), ListPitInfo.PARSER, new ParseField("pits"));
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @return the new XContent
     * @throws IOException if an I/O error occurs
     */
    public static GetAllPitNodesResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
