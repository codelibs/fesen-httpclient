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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.codelibs.fesen.opensearch.index.engine;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.lucene.Lucene;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeValue;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * A segment in the engine
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class Segment implements Writeable {

    private String name;
    private long generation;
    /**
     * The committed.
     */
    public boolean committed;
    /**
     * The search.
     */
    public boolean search;
    /**
     * The size in bytes.
     */
    public long sizeInBytes = -1;
    /**
     * The doc count.
     */
    public int docCount = -1;
    /**
     * The del doc count.
     */
    public int delDocCount = -1;
    /**
     * The version.
     */
    public org.apache.lucene.util.Version version = null;
    /**
     * The compound.
     */
    public Boolean compound = null;
    /**
     * The merge identifier.
     */
    public String mergeId;
    /**
     * The segment sort.
     */
    public Sort segmentSort;
    /**
     * The attributes.
     */
    public Map<String, String> attributes;

    private static final ByteSizeValue ZERO_BYTE_SIZE_VALUE = new ByteSizeValue(0L);

    /**
     * Creates a new Segment by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public Segment(StreamInput in) throws IOException {
        name = in.readString();
        generation = Long.parseLong(name.substring(1), Character.MAX_RADIX);
        committed = in.readBoolean();
        search = in.readBoolean();
        docCount = in.readInt();
        delDocCount = in.readInt();
        sizeInBytes = in.readLong();
        version = Lucene.parseVersionLenient(in.readOptionalString(), null);
        compound = in.readOptionalBoolean();
        mergeId = in.readOptionalString();
        // the following was removed in Lucene 9 (https://issues.apache.org/jira/browse/LUCENE-9387)
        // retain for bwc only (todo: remove in OpenSearch 3)
        if (in.getVersion().before(Version.V_2_0_0)) {
            in.readLong();  // estimated memory
        }
        if (in.readBoolean()) {
            // verbose mode
            readRamTree(in);
        }
        segmentSort = readSegmentSort(in);
        if (in.readBoolean()) {
            attributes = in.readMap(StreamInput::readString, StreamInput::readString);
        }
    }

    /**
     * Returns the name.
     *
     * @return the name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Returns the generation.
     *
     * @return the generation
     */
    public long getGeneration() {
        return this.generation;
    }

    /**
     * Returns the committed flag.
     *
     * @return the committed flag
     */
    public boolean isCommitted() {
        return this.committed;
    }

    /**
     * Returns the search flag.
     *
     * @return the search flag
     */
    public boolean isSearch() {
        return this.search;
    }

    /**
     * Returns the num docs.
     *
     * @return the num docs
     */
    public int getNumDocs() {
        return this.docCount;
    }

    /**
     * Returns the deleted docs.
     *
     * @return the deleted docs
     */
    public int getDeletedDocs() {
        return this.delDocCount;
    }

    /**
     * Returns the size.
     *
     * @return the size
     */
    public ByteSizeValue getSize() {
        return new ByteSizeValue(sizeInBytes);
    }

    /**
     * Returns the version.
     *
     * @return the version
     */
    public org.apache.lucene.util.Version getVersion() {
        return version;
    }

    /**
     * Returns the compound flag.
     *
     * @return the compound flag
     */
    @Nullable
    public Boolean isCompound() {
        return compound;
    }

    /**
     * If set, a string representing that the segment is part of a merge, with the value representing the
     * group of segments that represent this merge.
     *
     * @return the merge identifier
     */
    @Nullable
    public String getMergeId() {
        return this.mergeId;
    }

    /**
     * Estimation of the memory usage was removed in Lucene 9 (https://issues.apache.org/jira/browse/LUCENE-9387)
     * retain for bwc only (todo: remove in OpenSearch 3).
     * @return the zero memory
     * @deprecated
     */
    @Deprecated
    public ByteSizeValue getZeroMemory() {
        return ZERO_BYTE_SIZE_VALUE;
    }

    /**
     * Return the sort order of this segment, or null if the segment has no sort.
     *
     * @return the segment sort
     */
    public Sort getSegmentSort() {
        return segmentSort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Segment segment = (Segment) o;

        return Objects.equals(name, segment.name);

    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeBoolean(committed);
        out.writeBoolean(search);
        out.writeInt(docCount);
        out.writeInt(delDocCount);
        out.writeLong(sizeInBytes);
        out.writeOptionalString(version.toString());
        out.writeOptionalBoolean(compound);
        out.writeOptionalString(mergeId);
        // the following was removed in Lucene 9 (https://issues.apache.org/jira/browse/LUCENE-9387)
        // retain for bwc only (todo: remove in OpenSearch 3)
        if (out.getVersion().before(Version.V_2_0_0)) {
            out.writeLong(0L);
        }
        out.writeBoolean(false);
        writeSegmentSort(out, segmentSort);
        boolean hasAttributes = attributes != null;
        out.writeBoolean(hasAttributes);
        if (hasAttributes) {
            out.writeMap(attributes, StreamOutput::writeString, StreamOutput::writeString);
        }
    }

    private Sort readSegmentSort(StreamInput in) throws IOException {
        int size = in.readVInt();
        if (size == 0) {
            return null;
        }
        SortField[] fields = new SortField[size];
        for (int i = 0; i < size; i++) {
            String field = in.readString();
            byte type = in.readByte();
            if (type == 0) {
                Boolean missingFirst = in.readOptionalBoolean();
                boolean max = in.readBoolean();
                boolean reverse = in.readBoolean();
                fields[i] = new SortedSetSortField(field, reverse, max ? SortedSetSelector.Type.MAX : SortedSetSelector.Type.MIN);
                if (missingFirst != null) {
                    fields[i].setMissingValue(missingFirst ? SortedSetSortField.STRING_FIRST : SortedSetSortField.STRING_LAST);
                }
            } else {
                Object missing = in.readGenericValue();
                boolean max = in.readBoolean();
                boolean reverse = in.readBoolean();
                final SortField.Type numericType;
                switch (type) {
                    case 1:
                        numericType = SortField.Type.INT;
                        break;
                    case 2:
                        numericType = SortField.Type.FLOAT;
                        break;
                    case 3:
                        numericType = SortField.Type.DOUBLE;
                        break;
                    case 4:
                        numericType = SortField.Type.LONG;
                        break;
                    default:
                        throw new IOException("invalid index sort type:[" + type + "] for numeric field:[" + field + "]");
                }
                fields[i] = new SortedNumericSortField(
                    field,
                    numericType,
                    reverse,
                    max ? SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN
                );
                if (missing != null) {
                    fields[i].setMissingValue(missing);
                }
            }
        }
        return new Sort(fields);
    }

    private void writeSegmentSort(StreamOutput out, Sort sort) throws IOException {
        if (sort == null) {
            out.writeVInt(0);
            return;
        }
        out.writeVInt(sort.getSort().length);
        for (SortField field : sort.getSort()) {
            out.writeString(field.getField());
            if (field instanceof SortedSetSortField sortedSetSortField) {
                out.writeByte((byte) 0);
                out.writeOptionalBoolean(field.getMissingValue() == null ? null : field.getMissingValue() == SortField.STRING_FIRST);
                out.writeBoolean(sortedSetSortField.getSelector() == SortedSetSelector.Type.MAX);
                out.writeBoolean(field.getReverse());
            } else if (field instanceof SortedNumericSortField sortedNumericSortField) {
                switch (sortedNumericSortField.getNumericType()) {
                    case INT:
                        out.writeByte((byte) 1);
                        break;
                    case FLOAT:
                        out.writeByte((byte) 2);
                        break;
                    case DOUBLE:
                        out.writeByte((byte) 3);
                        break;
                    case LONG:
                        out.writeByte((byte) 4);
                        break;
                    default:
                        throw new IOException("invalid index sort field:" + field);
                }
                out.writeGenericValue(field.getMissingValue());
                out.writeBoolean(sortedNumericSortField.getSelector() == SortedNumericSelector.Type.MAX);
                out.writeBoolean(field.getReverse());
            } else {
                throw new IOException("invalid index sort field:" + field);
            }
        }
    }

    private static void readRamTree(StreamInput in) throws IOException {
        in.readString();
        in.readVLong();
        int numChildren = in.readVInt();
        for (int i = 0; i < numChildren; i++) {
            readRamTree(in);
        }
    }

    @Override
    public String toString() {
        return "Segment{"
            + "name='"
            + name
            + '\''
            + ", generation="
            + generation
            + ", committed="
            + committed
            + ", search="
            + search
            + ", sizeInBytes="
            + sizeInBytes
            + ", docCount="
            + docCount
            + ", delDocCount="
            + delDocCount
            + ", version='"
            + version
            + '\''
            + ", compound="
            + compound
            + ", mergeId='"
            + mergeId
            + '\''
            + (segmentSort != null ? ", sort=" + segmentSort : "")
            + ", attributes="
            + attributes
            + '}';
    }
}
