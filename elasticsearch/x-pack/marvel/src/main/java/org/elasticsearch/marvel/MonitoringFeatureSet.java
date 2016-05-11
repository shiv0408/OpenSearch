/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.Exporters;
import org.elasticsearch.marvel.agent.exporter.http.HttpExporter;
import org.elasticsearch.marvel.agent.exporter.local.LocalExporter;
import org.elasticsearch.xpack.XPackFeatureSet;

import java.io.IOException;

/**
 *
 */
public class MonitoringFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final MonitoringLicensee licensee;
    private final Exporters exporters;

    @Inject
    public MonitoringFeatureSet(Settings settings, @Nullable MonitoringLicensee licensee, Exporters exporters,
                                NamedWriteableRegistry namedWriteableRegistry) {
        this.enabled = MonitoringSettings.ENABLED.get(settings);
        this.licensee = licensee;
        this.exporters = exporters;
        namedWriteableRegistry.register(Usage.class, Usage.WRITEABLE_NAME, Usage::new);
    }

    @Override
    public String name() {
        return Monitoring.NAME;
    }

    @Override
    public String description() {
        return "Monitoring for the Elastic Stack";
    }

    @Override
    public boolean available() {
        return licensee != null && licensee.isAvailable();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Usage usage() {

        int enabledLocalExporters = 0;
        int enabledHttpExporters = 0;
        int enabledUnknownExporters = 0;
        for (Exporter exporter : exporters) {
            if (exporter.config().enabled()) {
                switch (exporter.type()) {
                    case LocalExporter.TYPE:
                        enabledLocalExporters++;
                        break;
                    case HttpExporter.TYPE:
                        enabledHttpExporters++;
                        break;
                    default:
                        enabledUnknownExporters++;
                }
            }
        }

        return new Usage(available(), enabled(), enabledLocalExporters, enabledHttpExporters, enabledUnknownExporters);
    }

    static class Usage extends XPackFeatureSet.Usage {

        private static String WRITEABLE_NAME = writeableName(Monitoring.NAME);

        private final int enabledLocalExporters;
        private final int enabledHttpExporters;
        private final int enabledUnknownExporters;

        public Usage(StreamInput in) throws IOException {
            super(in);
            this.enabledLocalExporters = in.readVInt();
            this.enabledHttpExporters = in.readVInt();
            this.enabledUnknownExporters = in.readVInt();
        }

        public Usage(boolean available, boolean enabled, int enabledLocalExporters, int enabledHttpExporters, int enabledUnknownExporters) {
            super(Monitoring.NAME, available, enabled);
            this.enabledLocalExporters = enabledLocalExporters;
            this.enabledHttpExporters = enabledHttpExporters;
            this.enabledUnknownExporters = enabledUnknownExporters;
        }

        @Override
        public boolean available() {
            return available;
        }

        @Override
        public boolean enabled() {
            return enabled;
        }

        @Override
        public String getWriteableName() {
            return WRITEABLE_NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(enabledLocalExporters);
            out.writeVInt(enabledHttpExporters);
            out.writeVInt(enabledUnknownExporters);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Field.AVAILABLE, available);
            builder.field(Field.ENABLED, enabled);

            builder.startObject(Field.ENABLED_EXPORTERS);
            builder.field(Field.LOCAL, enabledLocalExporters);
            builder.field(Field.HTTP, enabledHttpExporters);
            if (enabledUnknownExporters > 0) {
                builder.field(Field.UNKNOWN, enabledUnknownExporters);
            }
            builder.endObject();

            return builder.endObject();
        }

        interface Field extends XPackFeatureSet.Usage.Field {
            String ENABLED_EXPORTERS = "enabled_exporters";
            String LOCAL = "_local";
            String HTTP = "http";
            String UNKNOWN = "_unknown";
        }
    }
}
