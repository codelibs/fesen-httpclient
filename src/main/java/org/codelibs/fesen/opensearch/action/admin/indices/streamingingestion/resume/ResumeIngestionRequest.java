/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.resume;

import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.IndicesRequest;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.common.util.CollectionUtils;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;

/**
 * A request to resume ingestion.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.6.0")
public class ResumeIngestionRequest extends AcknowledgedRequest<ResumeIngestionRequest> implements IndicesRequest.Replaceable {
    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();
    private ResetSettings[] resetSettings;

    public ResumeIngestionRequest(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        this.resetSettings = in.readArray(ResetSettings::new, ResetSettings[]::new);
    }

    /**
     * Constructs a new resume ingestion request.
     */
    public ResumeIngestionRequest(String[] indices) {
        this(indices, new ResetSettings[0]);
    }

    /**
     * Constructs a new resume ingestion request with reset settings.
     */
    public ResumeIngestionRequest(String[] indices, ResetSettings[] resetSettings) {
        this.indices = indices;
        this.resetSettings = resetSettings;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (CollectionUtils.isEmpty(indices)) {
            validationException = addValidationError("index is missing", validationException);
        }

        if (resetSettings.length > 0) {
            boolean invalidResetSettingsFound = Arrays.stream(resetSettings)
                .anyMatch(
                    resetSettings -> resetSettings.getShard() < 0 || resetSettings.getMode() == null || resetSettings.getValue() == null
                );
            if (invalidResetSettingsFound) {
                validationException = addValidationError("ResetSettings is missing either shard, mode or value", validationException);
            }
        }
        return validationException;
    }

    /**
     * The indices to be resumed
     */
    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * Sets the indices to be resumed
     */
    @Override
    public ResumeIngestionRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @return the desired behaviour regarding indices to ignore and wildcard indices expressions
     */
    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeArray(resetSettings);
    }

    public ResetSettings[] getResetSettings() {
        return resetSettings;
    }

    /**
     * Represents reset settings for a given shard to be applied as part of resume operation.
     * @opensearch.api
     */
    @PublicApi(since = "3.6.0")
    public static class ResetSettings implements Writeable {
        private final int shard;
        private final ResetMode mode;
        private final String value;

        public ResetSettings(int shard, ResetMode mode, String value) {
            this.shard = shard;
            this.mode = mode;
            this.value = value;
        }

        public ResetSettings(StreamInput in) throws IOException {
            this.shard = in.readVInt();
            this.mode = in.readEnum(ResetMode.class);
            this.value = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(shard);
            out.writeEnum(mode);
            out.writeString(value);
        }

        public int getShard() {
            return shard;
        }

        public ResetMode getMode() {
            return mode;
        }

        public String getValue() {
            return value;
        }

        /**
         * Reset options for Resume API. Offset mode supports kafka offsets or Kinesis sequence numbers and timestamp
         * mode supports a timestamp in milliseconds that will be used to retrieve corresponding offset.
         */
        @PublicApi(since = "3.6.0")
        public enum ResetMode {
            OFFSET,
            TIMESTAMP
        }

    }
}
