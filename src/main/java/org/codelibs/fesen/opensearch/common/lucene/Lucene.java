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

package org.codelibs.fesen.opensearch.common.lucene;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.codelibs.fesen.opensearch.ExceptionsHelper;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.SuppressForbidden;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.search.sort.SortedWiderNumericSortField;

import java.io.IOException;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Main lucene class.
 *
 * @opensearch.internal
 */
public class Lucene {

    /**
     * The EMPTY_SCORE_DOCS constant.
     */
    public static final ScoreDoc[] EMPTY_SCORE_DOCS = new ScoreDoc[0];

    /**
     * The EMPTY_TOP_DOCS constant.
     */
    public static final TopDocs EMPTY_TOP_DOCS = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), EMPTY_SCORE_DOCS);

    private Lucene() {}

    /**
     * Returns the number of documents in the index referenced by this {@link SegmentInfos}
     *
     * @param info the info
     * @return the num docs
     */
    public static int getNumDocs(SegmentInfos info) {
        int numDocs = 0;
        for (SegmentCommitInfo si : info) {
            numDocs += si.info.maxDoc() - si.getDelCount() - si.getSoftDelCount();
        }
        return numDocs;
    }

    /**
     * Reads the total hits.
     *
     * @param in the input to read from
     * @return the total hits
     * @throws IOException if an I/O error occurs
     */
    public static TotalHits readTotalHits(StreamInput in) throws IOException {
        long totalHits = in.readVLong();
        TotalHits.Relation totalHitsRelation = in.readEnum(TotalHits.Relation.class);
        return new TotalHits(totalHits, totalHitsRelation);
    }

    /**
     * Reads the sort value.
     *
     * @param in the input to read from
     * @return the sort value
     * @throws IOException if an I/O error occurs
     */
    public static Comparable readSortValue(StreamInput in) throws IOException {
        return readTypedValue(in);
    }

    /**
     * Reads a typed value from the stream based on a type byte prefix.
     *
     * @param in the input stream
     * @return the deserialized Comparable value
     * @throws IOException if reading fails or type is unknown
     */
    private static Comparable readTypedValue(StreamInput in) throws IOException {
        byte type = in.readByte();
        return switch (type) {
            case 0 -> null;
            case 1 -> in.readString();
            case 2 -> in.readInt();
            case 3 -> in.readLong();
            case 4 -> in.readFloat();
            case 5 -> in.readDouble();
            case 6 -> in.readByte();
            case 7 -> in.readShort();
            case 8 -> in.readBoolean();
            case 9 -> in.readBytesRef();
            case 10 -> new BigInteger(in.readString());
            default -> throw new IOException("Can't match type [" + type + "]");
        };
    }

    private static final Class<?> GEO_DISTANCE_SORT_TYPE_CLASS = LatLonDocValuesField.newDistanceSort("some_geo_field", 0, 0).getClass();

    /**
     * Writes the total hits.
     *
     * @param out the output to write to
     * @param totalHits the total hits
     * @throws IOException if an I/O error occurs
     */
    public static void writeTotalHits(StreamOutput out, TotalHits totalHits) throws IOException {
        out.writeVLong(totalHits.value());
        out.writeEnum(totalHits.relation());
    }

    private static void writeMissingValue(StreamOutput out, Object missingValue) throws IOException {
        if (missingValue == SortField.STRING_FIRST) {
            out.writeByte((byte) 1);
        } else if (missingValue == SortField.STRING_LAST) {
            out.writeByte((byte) 2);
        } else {
            out.writeByte((byte) 0);
            out.writeGenericValue(missingValue);
        }
    }

    private static Object readMissingValue(StreamInput in) throws IOException {
        final byte id = in.readByte();
        switch (id) {
            case 0:
                return in.readGenericValue();
            case 1:
                return SortField.STRING_FIRST;
            case 2:
                return SortField.STRING_LAST;
            default:
                throw new IOException("Unknown missing value id: " + id);
        }
    }

    /**
     * Writes the sort value.
     *
     * @param out the output to write to
     * @param field the field
     * @throws IOException if an I/O error occurs
     */
    public static void writeSortValue(StreamOutput out, Object field) throws IOException {
        if (field == null) {
            out.writeByte((byte) 0);
        } else {
            Class type = field.getClass();
            if (type == String.class) {
                out.writeByte((byte) 1);
                out.writeString((String) field);
            } else if (type == Integer.class) {
                out.writeByte((byte) 2);
                out.writeInt((Integer) field);
            } else if (type == Long.class) {
                out.writeByte((byte) 3);
                out.writeLong((Long) field);
            } else if (type == Float.class) {
                out.writeByte((byte) 4);
                out.writeFloat((Float) field);
            } else if (type == Double.class) {
                out.writeByte((byte) 5);
                out.writeDouble((Double) field);
            } else if (type == Byte.class) {
                out.writeByte((byte) 6);
                out.writeByte((Byte) field);
            } else if (type == Short.class) {
                out.writeByte((byte) 7);
                out.writeShort((Short) field);
            } else if (type == Boolean.class) {
                out.writeByte((byte) 8);
                out.writeBoolean((Boolean) field);
            } else if (type == BytesRef.class) {
                out.writeByte((byte) 9);
                out.writeBytesRef((BytesRef) field);
            } else if (type == BigInteger.class) {
                // TODO: improve serialization of BigInteger
                out.writeByte((byte) 10);
                out.writeString(field.toString());
            } else {
                throw new IOException("Can't handle sort field value of type [" + type + "]");
            }
        }
    }

    // LUCENE 4 UPGRADE: We might want to maintain our own ordinal, instead of Lucene's ordinal
    /**
     * Reads the sort type.
     *
     * @param in the input to read from
     * @return the sort type
     * @throws IOException if an I/O error occurs
     */
    public static SortField.Type readSortType(StreamInput in) throws IOException {
        return SortField.Type.values()[in.readVInt()];
    }

    /**
     * Reads the sort field.
     *
     * @param in the input to read from
     * @return the sort field
     * @throws IOException if an I/O error occurs
     */
    public static SortField readSortField(StreamInput in) throws IOException {
        String field = null;
        if (in.readBoolean()) {
            field = in.readString();
        }
        SortField.Type sortType = readSortType(in);
        Object missingValue = readMissingValue(in);
        boolean reverse = in.readBoolean();
        SortField sortField = new SortField(field, sortType, reverse);
        if (missingValue != null) {
            sortField.setMissingValue(missingValue);
        }
        return sortField;
    }

    /**
     * Writes the sort type.
     *
     * @param out the output to write to
     * @param sortType the sort type
     * @throws IOException if an I/O error occurs
     */
    public static void writeSortType(StreamOutput out, SortField.Type sortType) throws IOException {
        out.writeVInt(sortType.ordinal());
    }

    /**
     * Writes the sort field.
     *
     * @param out the output to write to
     * @param sortField the sort field
     * @throws IOException if an I/O error occurs
     */
    public static void writeSortField(StreamOutput out, SortField sortField) throws IOException {
        if (sortField.getClass() == GEO_DISTANCE_SORT_TYPE_CLASS) {
            // for geo sorting, we replace the SortField with a SortField that assumes a double field.
            // this works since the SortField is only used for merging top docs
            SortField newSortField = new SortField(sortField.getField(), SortField.Type.DOUBLE);
            newSortField.setMissingValue(sortField.getMissingValue());
            sortField = newSortField;
        } else if (sortField.getClass() == SortedSetSortField.class) {
            // for multi-valued sort field, we replace the SortedSetSortField with a simple SortField.
            // It works because the sort field is only used to merge results from different shards.
            SortField newSortField = new SortField(sortField.getField(), SortField.Type.STRING, sortField.getReverse());
            newSortField.setMissingValue(sortField.getMissingValue());
            sortField = newSortField;
        } else if (sortField.getClass() == SortedNumericSortField.class || sortField.getClass() == SortedWiderNumericSortField.class) {
            // for multi-valued sort field, we replace the SortedNumericSortField/SortedWiderNumericSortField with a simple SortField.
            // It works because the sort field is only used to merge results from different shards.
            SortField newSortField = new SortField(
                sortField.getField(),
                ((SortedNumericSortField) sortField).getNumericType(),
                sortField.getReverse()
            );
            newSortField.setMissingValue(sortField.getMissingValue());
            sortField = newSortField;
        }

        if (sortField.getClass() != SortField.class) {
            throw new IllegalArgumentException("Cannot serialize SortField impl [" + sortField + "]");
        }
        if (sortField.getField() == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(sortField.getField());
        }
        writeSortType(out, sortField.getType());
        writeMissingValue(out, sortField.getMissingValue());
        out.writeBoolean(sortField.getReverse());
    }

    private static Number readExplanationValue(StreamInput in) throws IOException {
        final int numberType = in.readByte();
        switch (numberType) {
            case 0:
                return in.readFloat();
            case 1:
                return in.readDouble();
            case 2:
                return in.readZLong();
            default:
                throw new IOException("Unexpected number type: " + numberType);
        }
    }

    /**
     * Reads the explanation.
     *
     * @param in the input to read from
     * @return the explanation
     * @throws IOException if an I/O error occurs
     */
    public static Explanation readExplanation(StreamInput in) throws IOException {
        boolean match = in.readBoolean();
        String description = in.readString();
        final Explanation[] subExplanations = new Explanation[in.readVInt()];
        for (int i = 0; i < subExplanations.length; ++i) {
            subExplanations[i] = readExplanation(in);
        }
        if (match) {
            return Explanation.match(readExplanationValue(in), description, subExplanations);
        } else {
            return Explanation.noMatch(description, subExplanations);
        }
    }

    private static void writeExplanationValue(StreamOutput out, Number value) throws IOException {
        if (value instanceof Float) {
            out.writeByte((byte) 0);
            out.writeFloat(value.floatValue());
        } else if (value instanceof Double) {
            out.writeByte((byte) 1);
            out.writeDouble(value.doubleValue());
        } else {
            out.writeByte((byte) 2);
            out.writeZLong(value.longValue());
        }
    }

    /**
     * Writes the explanation.
     *
     * @param out the output to write to
     * @param explanation the explanation
     * @throws IOException if an I/O error occurs
     */
    public static void writeExplanation(StreamOutput out, Explanation explanation) throws IOException {
        out.writeBoolean(explanation.isMatch());
        out.writeString(explanation.getDescription());
        Explanation[] subExplanations = explanation.getDetails();
        out.writeVInt(subExplanations.length);
        for (Explanation subExp : subExplanations) {
            writeExplanation(out, subExp);
        }
        if (explanation.isMatch()) {
            writeExplanationValue(out, explanation.getValue());
        }
    }

    /**
     * Parses the version string lenient and returns the default value if the given string is null or empty
     *
     * @param toParse the to parse
     * @param defaultValue the default value
     * @return this instance
     */
    public static Version parseVersionLenient(String toParse, Version defaultValue) {
        return LenientParser.parse(toParse, defaultValue);
    }

    @SuppressForbidden(reason = "Version#parseLeniently() used in a central place")
    private static final class LenientParser {
        public static Version parse(String toParse, Version defaultValue) {
            if (Strings.hasLength(toParse)) {
                try {
                    return Version.parseLeniently(toParse);
                } catch (ParseException e) {
                    // pass to default
                }
            }
            return defaultValue;
        }
    }

}
