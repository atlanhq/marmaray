package com.uber.marmaray.examples.job;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Optional;
import com.uber.marmaray.common.configuration.*;
import com.uber.marmaray.common.converters.data.*;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.job.JobDag;
import com.uber.marmaray.common.job.JobManager;
import com.uber.marmaray.common.metadata.HoodieBasedMetadataManager;
import com.uber.marmaray.common.metadata.IMetadataManager;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.ErrorCauseTagNames;
import com.uber.marmaray.common.metrics.JobMetricNames;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.metrics.LongMetric;
import com.uber.marmaray.common.metrics.ModuleTagNames;
import com.uber.marmaray.common.metrics.TimerMetric;
import com.uber.marmaray.common.reporters.ConsoleReporter;
import com.uber.marmaray.common.reporters.PrometheusReporter;
import com.uber.marmaray.common.reporters.Reporters;
import com.uber.marmaray.common.sinks.hoodie.HoodieSink;
import com.uber.marmaray.common.sources.ISource;
import com.uber.marmaray.common.sources.IWorkUnitCalculator;
import com.uber.marmaray.common.sources.kafka.KafkaSource;
import com.uber.marmaray.common.sources.kafka.KafkaWorkUnitCalculator;
import com.uber.marmaray.common.spark.SparkArgs;
import com.uber.marmaray.common.spark.SparkFactory;
import com.uber.marmaray.utilities.SparkUtil;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.FSUtils;
import com.uber.marmaray.utilities.JobUtil;
import com.uber.marmaray.utilities.listener.TimeoutManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.hibernate.validator.constraints.NotEmpty;
import parquet.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.data.HoodieSinkDataConverter;
import com.uber.marmaray.common.exceptions.InvalidDataException;
import com.uber.marmaray.common.schema.ISchemaService;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import java.io.Serializable;


class KafkaSchemaServiceReader implements ISchemaService.ISchemaServiceReader, Serializable {

    private final String schemaString;
    private transient Schema schema;

    KafkaSchemaServiceReader(@NotEmpty final Schema schema) {
        this.schemaString = schema.toString();
        this.schema = schema;
    }

    private Schema getSchema() {
        if (this.schema == null) {
            this.schema = new Schema.Parser().parse(this.schemaString);
        }
        return this.schema;
    }

    @Override
    public GenericRecord read(final byte[] buffer) throws InvalidDataException {

        HashMap<String, byte[]> obj = null;
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try {
            bis = new ByteArrayInputStream(buffer);
            ois = new ObjectInputStream(bis);
            obj = (HashMap<String, byte[]>) ois.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        Map.Entry<String, byte[]> entry = obj.entrySet().iterator().next();
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(getSchema());
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(entry.getValue(), null);

        try {
            GenericRecord rec = datumReader.read(null, decoder);
            Schema schema = new Schema.Parser().parse("{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"Record\", \"fields\": [{\"name\": \"Region\", \"type\": \"string\"}, {\"name\":              \"Country\", \"type\": \"string\"}, {\"name\": \"_atlan_table_group_key\", \"type\": \"string\", \"default\" : \"okay\"}] }");
            GenericRecord gr = new GenericData.Record(schema);
            for(int i = 0; i< ((GenericData.Record) rec).getSchema().getFields().size(); i++) {
                gr.put(((GenericData.Record) rec).getSchema().getFields().get(i).name(), ((GenericData.Record) rec).get(((GenericData.Record) rec).getSchema().getFields().get(i).name()));
            }
            gr.put("_atlan_table_group_key", new Utf8(entry.getKey()));
            return gr;
        } catch (IOException e) {
            throw new InvalidDataException("Error decoding data", e);
        }


        // JSON reader
//        DatumReader<GenericRecord> reader = new GenericDatumReader<>(this.getSchema());
//
//        try {
//            JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(this.getSchema(), new String(buffer));
//            return reader.read(null, jsonDecoder);
//        } catch (IOException e) {
//            throw new InvalidDataException("Error decoding data", e);
//        }
    }
}

class CustomHoodieSinkDataConverter extends HoodieSinkDataConverter {
    CustomHoodieSinkDataConverter(Configuration conf, ErrorExtractor errorExtractor) {
        super(conf, errorExtractor);
    }

    @Override
    protected String getRecordKey(AvroPayload avroPayload) {
        return "Region";
    }

    @Override
    protected String getPartitionPath(AvroPayload avroPayload) {
        return "test";
    }
}



/**
 * Job to load data from kafka to hoodie
 */
@Slf4j
public class KafkaToHoodieJob {

    /**
     * Generic entry point
     *
     * @param args arguments for the job, from the command line
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {
        new KafkaToHoodieJob().run(args);
    }

    private void _run (final String[] args) throws IOException {
        //        final String schema = "{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"Record\", \"fields\": [{\"name\": \"Region\", \"type\": \"string\"}, {\"name\": \"Country\", \"type\": \"string\"}] }";
//        final Schema schemaObj = new org.apache.avro.Schema.Parser().parse(schema);
//        final GenericData.Record record = new GenericData.Record(schemaObj);
//        record.put("Region", "Sub-Saharan Africa");
//        record.put("Country", "Chad");
//
//        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//        final GenericDatumWriter datumWriter = new GenericDatumWriter<GenericRecord>(schemaObj);
//        final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
//
//        datumWriter.write(record, encoder);
//        encoder.flush();
//
//        final String recordString = new String(outputStream.toByteArray());

        final Instant jobStartTime = Instant.now();

        final Configuration conf = getConfiguration(args);

        final Reporters reporters = new Reporters();
        reporters.addReporter(new ConsoleReporter());

        final Map<String, String> metricTags = Collections.emptyMap();
        final DataFeedMetrics dataFeedMetrics = new DataFeedMetrics("kafka to hoodie ingestion", metricTags);

        log.info("Initializing configurations for job");
        final TimerMetric confInitMetric = new TimerMetric(DataFeedMetricNames.INIT_CONFIG_LATENCY_MS,
                metricTags);

        final KafkaSourceConfiguration kafkaSourceConf;
        final HoodieConfiguration hoodieConf;
        try {
            kafkaSourceConf = new KafkaSourceConfiguration(conf);
            hoodieConf = new HoodieConfiguration(conf, "test_hoodie");
        } catch (final Exception e) {
            final LongMetric configError = new LongMetric(DataFeedMetricNames.DISPERSAL_CONFIGURATION_INIT_ERRORS, 1);
            configError.addTags(metricTags);
            configError.addTags(DataFeedMetricNames
                    .getErrorModuleCauseTags(ModuleTagNames.CONFIGURATION, ErrorCauseTagNames.CONFIG_ERROR));
            reporters.report(configError);
            reporters.getReporters().forEach(dataFeedMetrics::gauageFailureMetric);
            throw e;
        }
        confInitMetric.stop();
        reporters.report(confInitMetric);

        log.info("Reading schema");
        final TimerMetric convertSchemaLatencyMs =
                new TimerMetric(DataFeedMetricNames.CONVERT_SCHEMA_LATENCY_MS, metricTags);

//        final StructType inputSchema = DataTypes.createStructType(new StructField[]{
//                DataTypes.createStructField("Region", DataTypes.StringType, true),
//                DataTypes.createStructField("Country", DataTypes.StringType, true)
//        });
//
//        final DataFrameSchemaConverter schemaConverter = new DataFrameSchemaConverter();
//        final Schema outputSchema = schemaConverter.convertToCommonSchema(inputSchema);

        final String schema = "{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"Record\", \"fields\": [{\"name\": \"Region\", \"type\": \"string\"}, {\"name\": \"Country\", \"type\": \"string\"}, {\"name\": \"_atlan_table_group_key\", \"type\": \"null\", \"default\" : \"okay\"}] }";
        final Schema outputSchema = new org.apache.avro.Schema.Parser().parse(schema);
        convertSchemaLatencyMs.stop();
        reporters.report(convertSchemaLatencyMs);

        final SparkArgs sparkArgs = new SparkArgs(
                Arrays.asList(outputSchema),
                SparkUtil.getSerializationClasses(),
                conf);
        final SparkFactory sparkFactory = new SparkFactory(sparkArgs);
        final JobManager jobManager = JobManager.createJobManager(conf, "marmaray",
                "frequency", sparkFactory, reporters);

        final JavaSparkContext jsc = sparkFactory.getSparkContext();

        log.info("Initializing metadata manager for job");
        final TimerMetric metadataManagerInitMetric =
                new TimerMetric(DataFeedMetricNames.INIT_METADATAMANAGER_LATENCY_MS, metricTags);
        final IMetadataManager metadataManager;
        try {
            metadataManager = initMetadataManager(hoodieConf, jsc);
        } catch (final JobRuntimeException e) {
            final LongMetric configError = new LongMetric(DataFeedMetricNames.DISPERSAL_CONFIGURATION_INIT_ERRORS, 1);
            configError.addTags(metricTags);
            configError.addTags(DataFeedMetricNames
                    .getErrorModuleCauseTags(ModuleTagNames.METADATA_MANAGER, ErrorCauseTagNames.CONFIG_ERROR));
            reporters.report(configError);
            reporters.getReporters().forEach(dataFeedMetrics::gauageFailureMetric);
            throw e;
        }
        metadataManagerInitMetric.stop();
        reporters.report(metadataManagerInitMetric);

        try {
            log.info("Initializing converters & schemas for job");
            final SQLContext sqlContext = SQLContext.getOrCreate(jsc.sc());

            log.info("Common schema is: {}", outputSchema.toString());

            // Schema
            log.info("Initializing source data converter");
            KafkaSchemaServiceReader serviceReader = new KafkaSchemaServiceReader(outputSchema);
            final KafkaSourceDataConverter dataConverter = new KafkaSourceDataConverter(serviceReader, conf, new ErrorExtractor());

            log.info("Initializing source & sink for job");
            final ISource kafkaSource = new KafkaSource(kafkaSourceConf, Optional.of(jsc), dataConverter, Optional.absent(), Optional.absent());

            // Sink
            HoodieSinkDataConverter hoodieSinkDataConverter = new CustomHoodieSinkDataConverter(conf, new ErrorExtractor());
            HoodieSink hoodieSink = new HoodieSink(hoodieConf, hoodieSinkDataConverter, jsc, HoodieSink.HoodieSinkOp.INSERT, metadataManager, Optional.absent());

            log.info("Initializing work unit calculator for job");
            final IWorkUnitCalculator workUnitCalculator = new KafkaWorkUnitCalculator(kafkaSourceConf);

            log.info("Initializing job dag");
            final JobDag jobDag = new JobDag(kafkaSource, hoodieSink, metadataManager, workUnitCalculator,
                    "test", "test", new JobMetrics("marmaray"), dataFeedMetrics,
                    reporters);

            jobManager.addJobDag(jobDag);

            log.info("Running dispersal job");
            try {
                jobManager.run();
                JobUtil.raiseExceptionIfStatusFailed(jobManager.getJobManagerStatus());
            } catch (final Throwable t) {
                if (TimeoutManager.getTimedOut()) {
                    final LongMetric runTimeError = new LongMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1);
                    runTimeError.addTags(metricTags);
                    runTimeError.addTags(DataFeedMetricNames.getErrorModuleCauseTags(
                            ModuleTagNames.JOB_MANAGER, ErrorCauseTagNames.TIME_OUT));
                    reporters.report(runTimeError);
                }
                final LongMetric configError = new LongMetric(JobMetricNames.RUN_JOB_ERROR_COUNT, 1);
                configError.addTags(metricTags);
                reporters.report(configError);
                throw t;
            }
            log.info("Dispersal job has been completed");

            final TimerMetric jobLatencyMetric =
                    new TimerMetric(JobMetricNames.RUN_JOB_DAG_LATENCY_MS, metricTags, jobStartTime);
            jobLatencyMetric.stop();
            reporters.report(jobLatencyMetric);
            reporters.finish();
        } finally {
            jsc.stop();
            JobManager.reset();
        }
    }

    /**
     * Main execution method for the job.
     *
     * @param args command line arguments
     * @throws IOException
     */
    private void run(final String[] args) throws IOException {
        while (true) {
            try {
                this._run(args);
            }
            catch (Exception e){
                log.error(e.toString());
            }
            try {
                log.info("=========================sleeping======================");
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Get configuration from command line
     *
     * @param args command line arguments passed in
     * @return configuration populated from them
     */
    private Configuration getConfiguration(@NotEmpty final String[] args) {
        final KafkaToHoodieCommandLineOptions options = new KafkaToHoodieCommandLineOptions(args);
        if (options.getConfFile() != null) {
            return getFileConfiguration(options.getConfFile());
        } else if (options.getJsonConf() != null) {
            return getJsonConfiguration(options.getJsonConf());
        } else {
            throw new JobRuntimeException("Unable to find conf; this shouldn't be possible");
        }
    }

    /**
     * Get configuration from JSON-based configuration
     *
     * @param jsonConf JSON string of configuration
     * @return configuration populated from it
     */
    private Configuration getJsonConfiguration(@NotEmpty final String jsonConf) {
        final Configuration conf = new Configuration();
        conf.loadYamlStream(IOUtils.toInputStream(jsonConf), Optional.absent());
        return conf;
    }

    /**
     * Load configuration from a file on HDFS
     *
     * @param filePath path to the HDFS file to load
     * @return configuration populated from it
     */
    private Configuration getFileConfiguration(@NotEmpty final String filePath) {
        final Configuration conf = new Configuration();
        try {
            final FileSystem fs = FSUtils.getFs(conf, Optional.absent());
            final Path dataFeedConfFile = new Path(filePath);
            log.info("Loading configuration from {}", dataFeedConfFile.toString());
            conf.loadYamlStream(fs.open(dataFeedConfFile), Optional.absent());
        } catch (IOException e) {
            final String errorMessage = String.format("Unable to find configuration for %s", filePath);
            log.error(errorMessage);
            throw new JobRuntimeException(errorMessage, e);
        }
        return conf;

    }

    /**
     * Initialize the metadata store system
     *
     * @param conf configuration to use
     * @param jsc  Java spark context
     * @return metadata manager
     */
    private static IMetadataManager initMetadataManager(@NonNull final HoodieConfiguration conf, @NonNull final JavaSparkContext jsc) {
        log.info("Create metadata manager");
        try {
            return new HoodieBasedMetadataManager(conf, new AtomicBoolean(true), jsc);
        } catch (IOException e) {
            throw new JobRuntimeException("Unable to create metadata manager", e);
        }
    }

    private static final class KafkaToHoodieCommandLineOptions {
        @Getter
        @Parameter(names = {"--configurationFile", "-c"}, description = "path to configuration file")
        private String confFile;

        @Getter
        @Parameter(names = {"--jsonConfiguration", "-j"}, description = "json configuration")
        private String jsonConf;

        private KafkaToHoodieCommandLineOptions(@NonNull final String[] args) {
            final JCommander commander = new JCommander(this);
            commander.parse(args);
            Preconditions.checkState(this.confFile != null || this.jsonConf != null,
                    "One of jsonConfiguration or configurationFile must be specified");
        }
    }

}
