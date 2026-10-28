package com.sdh.healthcare.connector.file.route;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Directory discovery (design §5). Spike version: hands each settled file to the pipeline endpoint.
 * Camel never moves or deletes files; the pipeline does that once archiving has been committed.
 */
@Component
public class InboundRoute extends RouteBuilder {
    @Value("${sdh.file.inbound-dir}")
    String inboundDir;

    @Value("${sdh.file.min-age:30s}")
    String minAge;

    @Override
    public void configure() {
        fromF("file:%s?noop=true&recursive=false&sortBy=file:modified&maxMessagesPerPoll=1"
                        + "&readLock=changed&readLockMinAge=%s&readLockCheckInterval=1000"
                        + "&exclude=^\\..*&antExclude=completed/**,failed/**",
                        inboundDir, minAge)
                .routeId("inbound")
                .to("direct:pipeline");
    }
}
