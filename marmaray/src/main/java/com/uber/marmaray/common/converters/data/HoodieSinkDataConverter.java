/*
 * Copyright (c) 2018 Uber Technologies, Inc.
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions
 * of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */
package com.uber.marmaray.common.converters.data;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.HoodieConfiguration;
import com.uber.marmaray.common.converters.converterresult.ConverterResult;
import com.uber.marmaray.common.exceptions.InvalidDataException;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.sinks.hoodie.HoodieSink;
import com.uber.marmaray.utilities.ErrorExtractor;

import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Optional;
import org.apache.hudi.common.util.Option;

/**
 * {@link HoodieSinkDataConverter} extends {@link SinkDataConverter}
 * This class is used by {@link HoodieSink} to generate {@link org.apache.hudi.common.model.HoodieRecord} from
 * {@link com.uber.marmaray.common.AvroPayload}.
 */
public class HoodieSinkDataConverter extends SinkDataConverter<Schema, HoodieRecord<HoodieRecordPayload>> {

    // store the schema as a string since Schema doesn't serialize. Used in extended classes.
    protected String schema;
    private final ErrorExtractor errorExtractor;
    private final HoodieConfiguration hoodieConfiguration;

    public HoodieSinkDataConverter(@NonNull final Configuration conf, @NonNull final ErrorExtractor errorExtractor,
                                   @NonNull final HoodieConfiguration hoodieConfiguration) {
        super(conf, errorExtractor);
        this.errorExtractor = errorExtractor;
        this.hoodieConfiguration = hoodieConfiguration;
    }

    public HoodieSinkDataConverter(@NonNull final Configuration conf, final String schema,
                                   @NonNull final ErrorExtractor errorExtractor,
                                   HoodieConfiguration hoodieConfiguration) {
        super(conf, errorExtractor);
        this.schema = schema;
        this.errorExtractor = errorExtractor;
        this.hoodieConfiguration = hoodieConfiguration;
    }

    @Override
    public void setDataFeedMetrics(final DataFeedMetrics dataFeedMetrics) {
        //ignored
    }

    @Override
    public void setJobMetrics(final JobMetrics jobMetrics) {
        // ignored
    }

    @Override
    protected final List<ConverterResult<AvroPayload, HoodieRecord<HoodieRecordPayload>>> convert(
            @NonNull final AvroPayload payload) throws Exception {
        final HoodieKey hoodieKey = new HoodieKey(getRecordKey(payload), getPartitionPath(payload));
        final HoodieRecordPayload hoodiePayload = getPayload(payload);
        return Collections.singletonList(new ConverterResult<>((new HoodieRecord<>(hoodieKey, hoodiePayload))));
    }

    /**
     * The implementation of it should use fields from {@link GenericRecord} to generate record key which is needed for
     * {@link HoodieKey}.
     *
     * @param payload {@link AvroPayload}.
     */
    protected String getRecordKey(@NonNull final AvroPayload payload) throws Exception {
        Optional<String> hoodieRecordKey = hoodieConfiguration.getHoodieRecordKey();
        if (hoodieRecordKey.isPresent()) {
            final Object recordKeyFieldVal = payload.getData().get(hoodieRecordKey.get());
            if (recordKeyFieldVal == null) {
                throw new InvalidDataException("required field is missing:" + hoodieRecordKey.get());
            }
            return recordKeyFieldVal.toString();
        }
        throw new Exception("Hoodie Record Key missing");
    }

    /**
     * The implementation of it should use fields from {@link AvroPayload} to generate partition path which is needed
     * for {@link HoodieKey}.
     *
     * @param payload {@link AvroPayload}.
     */
    protected String getPartitionPath(@NonNull final AvroPayload payload) throws Exception {
        Optional<String> hoodiePartitionPath = hoodieConfiguration.getHoodiePartitionPath();
        if (hoodiePartitionPath.isPresent()) {
            final Object partitionFieldVal = payload.getData().get(hoodiePartitionPath.get());
            if (partitionFieldVal == null) {
                throw new InvalidDataException("required field is missing:" + hoodiePartitionPath.get());
            }
            return partitionFieldVal.toString();
        }
        throw new Exception("Hoodie Partition Path missing");
    }

    protected HoodieRecordPayload getPayload(@NonNull final AvroPayload payload) {
        return new HoodieAvroPayload(Option.of(payload.getData()));
    }
}
