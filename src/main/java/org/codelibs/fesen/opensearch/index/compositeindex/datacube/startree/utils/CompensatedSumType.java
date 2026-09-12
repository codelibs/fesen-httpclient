/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.index.compositeindex.datacube.startree.utils;

import org.codelibs.fesen.opensearch.index.mapper.FieldValueConverter;

import static org.codelibs.fesen.opensearch.index.mapper.NumberFieldMapper.NumberType.DOUBLE;

/**
 * Field value converter for CompensatedSum - it's just a wrapper over Double
 *
 * @opensearch.internal
 */
public class CompensatedSumType implements FieldValueConverter {

    public CompensatedSumType() {}

    @Override
    public double toDoubleValue(long value) {
        return DOUBLE.toDoubleValue(value);
    }
}
