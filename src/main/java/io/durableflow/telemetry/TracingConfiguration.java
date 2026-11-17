package io.durableflow.telemetry;

import io.durableflow.config.RuntimeSettings;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.api.common.AttributeKey;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TracingConfiguration {
    @Bean(destroyMethod = "close")
    SdkTracerProvider tracerProvider(RuntimeSettings settings) {
        var builder = SdkTracerProvider.builder().setResource(Resource.getDefault().toBuilder()
                .put(AttributeKey.stringKey("service.name"), System.getenv().getOrDefault("SERVICE_NAME", "durableflow-" + settings.role())).build());
        String endpoint = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
        if (endpoint != null && !endpoint.isBlank()) {
            String target = endpoint.endsWith("/") ? endpoint + "v1/traces" : endpoint + "/v1/traces";
            builder.addSpanProcessor(BatchSpanProcessor.builder(OtlpHttpSpanExporter.builder().setEndpoint(target).build()).build());
        }
        SdkTracerProvider provider = builder.build();
        GlobalOpenTelemetry.set(OpenTelemetrySdk.builder().setTracerProvider(provider).build());
        return provider;
    }

    @Bean
    Tracer tracer(SdkTracerProvider provider, RuntimeSettings settings) {
        return provider.get("durableflow-" + settings.role());
    }
}
