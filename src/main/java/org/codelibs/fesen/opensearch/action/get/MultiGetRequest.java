/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.codelibs.fesen.opensearch.action.get;

import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.ActionRequest;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.CompositeIndicesRequest;
import org.codelibs.fesen.opensearch.action.IndicesRequest;
import org.codelibs.fesen.opensearch.action.RealtimeRequest;
import org.codelibs.fesen.opensearch.action.ValidateActions;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.lucene.uid.Versions;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser.Token;
import org.codelibs.fesen.opensearch.index.VersionType;
import org.codelibs.fesen.opensearch.index.mapper.MapperService;
import org.codelibs.fesen.opensearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

/**
 * Transport request for a multi get.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class MultiGetRequest extends ActionRequest
    implements
        Iterable<MultiGetRequest.Item>,
        CompositeIndicesRequest,
        RealtimeRequest,
        ToXContentObject {

    private static final ParseField DOCS = new ParseField("docs");
    private static final ParseField INDEX = new ParseField("_index");
    private static final ParseField ID = new ParseField("_id");
    private static final ParseField ROUTING = new ParseField("routing");
    private static final ParseField VERSION = new ParseField("version");
    private static final ParseField VERSION_TYPE = new ParseField("version_type");
    private static final ParseField FIELDS = new ParseField("fields");
    private static final ParseField STORED_FIELDS = new ParseField("stored_fields");
    private static final ParseField SOURCE = new ParseField("_source");

    /**
     * A single get item.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Item implements Writeable, IndicesRequest, ToXContentObject {

        private String index;
        private String id;
        private String routing;
        private String[] storedFields;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;
        private FetchSourceContext fetchSourceContext;

        /**
         * Creates a new Item.
         */
        public Item() {

        }

        /**
         * Creates a new Item.
         *
         * @param index the index
         * @param id the identifier
         */
        public Item(String index, String id) {
            this.index = index;
            this.id = id;
        }

        /**
         * Indexes this instance.
         *
         * @return this instance
         */
        public String index() {
            return this.index;
        }

        @Override
        public String[] indices() {
            return new String[] { index };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return GetRequest.INDICES_OPTIONS;
        }

        /**
         * Returns the identifier.
         *
         * @return the identifier
         */
        public String id() {
            return this.id;
        }

        /**
         * Returns the routing.
         *
         * @return the routing
         */
        public String routing() {
            return this.routing;
        }

        /**
         * Returns the stored fields.
         *
         * @return the stored fields
         */
        public String[] storedFields() {
            return this.storedFields;
        }

        /**
         * Allows setting the {@link FetchSourceContext} for this request, controlling if and how _source should be returned.
         *
         * @param fetchSourceContext the fetch source context
         * @return this instance
         */
        public Item fetchSourceContext(FetchSourceContext fetchSourceContext) {
            this.fetchSourceContext = fetchSourceContext;
            return this;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            if (out.getVersion().before(Version.V_2_0_0)) {
                out.writeOptionalString(MapperService.SINGLE_MAPPING_NAME);
            }
            out.writeString(id);
            out.writeOptionalString(routing);
            out.writeOptionalStringArray(storedFields);
            out.writeLong(version);
            out.writeByte(versionType.getValue());

            out.writeOptionalWriteable(fetchSourceContext);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INDEX.getPreferredName(), index);
            builder.field(ID.getPreferredName(), id);
            builder.field(ROUTING.getPreferredName(), routing);
            builder.field(STORED_FIELDS.getPreferredName(), storedFields);
            builder.field(VERSION.getPreferredName(), version);
            builder.field(VERSION_TYPE.getPreferredName(), VersionType.toString(versionType));
            builder.field(SOURCE.getPreferredName(), fetchSourceContext);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Item)) return false;

            Item item = (Item) o;

            if (version != item.version) return false;
            if (fetchSourceContext != null ? !fetchSourceContext.equals(item.fetchSourceContext) : item.fetchSourceContext != null)
                return false;
            if (!Arrays.equals(storedFields, item.storedFields)) return false;
            if (!id.equals(item.id)) return false;
            if (!index.equals(item.index)) return false;
            if (routing != null ? !routing.equals(item.routing) : item.routing != null) return false;
            if (versionType != item.versionType) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = index.hashCode();
            result = 31 * result + id.hashCode();
            result = 31 * result + (routing != null ? routing.hashCode() : 0);
            result = 31 * result + (storedFields != null ? Arrays.hashCode(storedFields) : 0);
            result = 31 * result + Long.hashCode(version);
            result = 31 * result + versionType.hashCode();
            result = 31 * result + (fetchSourceContext != null ? fetchSourceContext.hashCode() : 0);
            return result;
        }

        public String toString() {
            return Strings.toString(MediaTypeRegistry.JSON, this);
        }

    }

    String preference;
    boolean realtime = true;
    boolean refresh;
    List<Item> items = new ArrayList<>();

    /**
     * Creates a new MultiGetRequest.
     */
    public MultiGetRequest() {}

    /**
     * Adds this instance.
     *
     * @param item the item
     * @return this instance
     */
    public MultiGetRequest add(Item item) {
        items.add(item);
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (items.isEmpty()) {
            validationException = ValidateActions.addValidationError("no documents to get", validationException);
        } else {
            for (int i = 0; i < items.size(); i++) {
                Item item = items.get(i);
                if (item.index() == null) {
                    validationException = ValidateActions.addValidationError("index is missing for doc " + i, validationException);
                }
                if (item.id() == null) {
                    validationException = ValidateActions.addValidationError("id is missing for doc " + i, validationException);
                }
            }
        }
        return validationException;
    }

    /**
     * Returns the preference.
     *
     * @return the preference
     */
    public String preference() {
        return this.preference;
    }

    /**
     * Returns the realtime.
     *
     * @return the realtime
     */
    public boolean realtime() {
        return this.realtime;
    }

    @Override
    public MultiGetRequest realtime(boolean realtime) {
        this.realtime = realtime;
        return this;
    }

    /**
     * Refreshes this instance.
     *
     * @return this instance
     */
    public boolean refresh() {
        return this.refresh;
    }

    @Override
    public Iterator<Item> iterator() {
        return Collections.unmodifiableCollection(items).iterator();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(preference);
        out.writeBoolean(refresh);
        out.writeBoolean(realtime);
        out.writeList(items);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(DOCS.getPreferredName());
        for (Item item : items) {
            builder.value(item);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "MultiGetRequest{"
            + "preference='"
            + preference
            + '\''
            + ", realtime="
            + realtime
            + ", refresh="
            + refresh
            + ", items="
            + items
            + '}';
    }

}
