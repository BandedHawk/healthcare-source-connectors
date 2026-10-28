package com.sdh.healthcare.connector.file.route;

import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.file.ingest.InboundService;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import java.nio.file.Path;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.file.GenericFile;

/**
 * Directory discovery (design §5): one settled file at a time, oldest first, handed to {@link InboundService}.
 *
 * <ul>
 *   <li>Oldest first: {@code eagerMaxMessagesPerPoll=false} makes Camel scan and sort every file before taking one;
 *       by default the one-file limit applies during the scan, before sorting.</li>
 *   <li>Camel never moves or deletes files ({@code noop}); the processor deletes an original once it is archived.</li>
 *   <li>{@code idempotent=false}: a file that couldn't be handled is offered again on the next poll.</li>
 *   <li>Hidden files, {@code completed/} and {@code failed/} are never picked up.</li>
 *   <li>The route doesn't auto-start: it starts after startup recovery has finished any in-progress file.</li>
 * </ul>
 */
public class InboundRoute extends RouteBuilder {
    private final ConnectorProperties.File props;
    private final InboundService service;

    public InboundRoute(ConnectorProperties.File props, InboundService service) {
        this.props = props;
        this.service = service;
    }

    public String uri() {
        String lock = switch (props.readLock()) {
            case "changed" -> "&readLock=changed&readLockMinAge=" + props.minAge().toMillis()
                    + "&readLockCheckInterval=1000";
            case "marker" -> "&doneFileName=${file:name}.done";
            default -> throw new IllegalArgumentException("sdh.file.read-lock must be 'changed' or 'marker'");
        };
        return "file:" + props.inboundDir() + "?noop=true&idempotent=false&recursive=false&sortBy=file:modified"
                + "&maxMessagesPerPoll=1&eagerMaxMessagesPerPoll=false&delay=" + props.pollInterval().toMillis()
                + "&exclude=^\\..*&antExclude=completed/**,failed/**&antInclude=" + props.include() + lock;
    }

    @Override
    public void configure() {
        from(uri())
                .routeId(ConnectorRuntime.InboundRouteIds.INBOUND)
                .autoStartup(false)
                .process(e -> service.handle(Path.of(e.getIn().getBody(GenericFile.class).getAbsoluteFilePath())));
    }
}
