/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.web;

import com.sdh.healthcare.connector.landing.config.HttpSettings;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.intake.PushService;
import com.sdh.healthcare.connector.landing.runtime.LandingRuntime;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The HTTP push endpoint (design §5.3.15): {@code POST
 * /intakes/<intake>/items}, behind Spring Security.
 * <p>
 * The body is read from the request's own stream, straight into landing's
 * encrypted chunks: never whole into memory and never to a temporary file.
 * That is why this is a plain Spring MVC endpoint and not Camel's
 * platform-http, which reads the whole body before its route runs. Form
 * and multipart bodies are refused, since the server would parse those.
 * <p>
 * Answers:
 * <ul>
 *   <li>{@code 202} with the receipt, once the item is committed; the same
 *       receipt again for a repeated {@code Idempotency-Key};</li>
 *   <li>{@code 400} for a malformed key, or a body that broke off or
 *       didn't match its {@code Content-Length};</li>
 *   <li>{@code 404} for another intake's path;</li>
 *   <li>{@code 413} for a body over {@code max-item-bytes};</li>
 *   <li>{@code 415} for a form or multipart body;</li>
 *   <li>{@code 503} with {@code Retry-After} while another push holds the
 *       intake past {@code max-wait}, or storage or the broker is down.
 *       </li>
 * </ul>
 * Answers are never cached, request headers are never echoed, and nothing
 * about a push's content or sender is logged.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@RestController
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class PushController
{

    /** The header carrying the sender's key for a push. */
    public static final String IDEMPOTENCY_KEY = "Idempotency-Key";

    /**
     * The header carrying the producer's own reference for a push, sent
     * back in its receipt and result (design §5.3.8).
     */
    public static final String CORRELATION_ID = "X-Correlation-Id";

    /**
     * The header naming the earlier item a push replaces, by its item ID.
     * It is recorded as a link only: both items are kept in full.
     */
    public static final String REPLACES = "Replaces";

    /** What an intake's name may hold, so its path needs no escaping. */
    private static final Pattern INTAKE_NAME =
            Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,63}");

    /** What an idempotency key may hold: 1 to 255 visible ASCII characters. */
    private static final Pattern KEY = Pattern.compile("[\\x21-\\x7e]{1,255}");

    /** The intake's name. */
    private final String intake;

    /** Lands the pushes. */
    private final PushService service;

    /** The {@code Retry-After} sent with a 503, in seconds. */
    private final long retryAfterSeconds;

    /**
     * Creates the endpoint.
     *
     * @param properties the node's settings
     * @param settings   the endpoint's settings
     * @param runtime    the runtime, holding the push service
     */
    public PushController(LandingProperties properties, HttpSettings settings,
            LandingRuntime runtime)
    {
        this.intake = properties.landing().intake();
        this.service = runtime.pushService();
        this.retryAfterSeconds = settings.retryAfter().toSeconds();
    }

    /**
     * The endpoint's path for an intake.
     *
     * @param intake the intake's name
     * @return {@code /intakes/<intake>/items}
     * @throws IllegalArgumentException if the name has characters a path
     *                                  would need to escape
     */
    public static String path(String intake)
    {
        if (intake == null || !INTAKE_NAME.matcher(intake).matches())
        {
            throw new IllegalArgumentException("sdh.landing.intake must be"
                    + " letters, digits, '.', '_' or '-' on an http intake,"
                    + " at most 64, starting with a letter or digit");
        }
        return "/intakes/" + intake + "/items";
    }

    /**
     * Lands a push.
     *
     * @param name    the intake named in the path
     * @param request the request, whose body is read as a stream
     * @return the answer
     * @throws IOException if the body can't be opened
     */
    @PostMapping("/intakes/{intake}/items")
    public ResponseEntity<String> push(@PathVariable("intake") String name,
            HttpServletRequest request) throws IOException
    {
        if (!intake.equals(name))
        {
            return answer(HttpStatus.NOT_FOUND, null);
        }
        String sender = sender(
                SecurityContextHolder.getContext().getAuthentication());
        if (sender == null)
        {
            return answer(HttpStatus.FORBIDDEN, null);
        }
        String type = request.getContentType() == null ? ""
                : request.getContentType().toLowerCase(Locale.ROOT);
        if (type.startsWith("application/x-www-form-urlencoded")
                || type.startsWith("multipart/"))
        {
            return answer(HttpStatus.UNSUPPORTED_MEDIA_TYPE, null);
        }
        String key = request.getHeader(IDEMPOTENCY_KEY);
        String correlationId = request.getHeader(CORRELATION_ID);
        if ((key != null && !KEY.matcher(key).matches())
                || (correlationId != null
                        && !KEY.matcher(correlationId).matches()))
        {
            return answer(HttpStatus.BAD_REQUEST, null);
        }
        UUID replaces = null;
        if (request.getHeader(REPLACES) != null)
        {
            try
            {
                replaces = UUID.fromString(request.getHeader(REPLACES)
                        .trim());
            }
            catch (IllegalArgumentException notAnId)
            {
                return answer(HttpStatus.BAD_REQUEST, null);
            }
        }
        PushService.Result result = service.receive(new PushService.Push(
                sender, key, request.getContentLengthLong(), correlationId,
                replaces), request.getInputStream());
        return switch (result.status())
        {
            case LANDED, REPLAYED -> answer(HttpStatus.ACCEPTED,
                    result.receipt().toJson());
            case TOO_LARGE -> answer(HttpStatus.CONTENT_TOO_LARGE, null);
            case BROKEN -> answer(HttpStatus.BAD_REQUEST, null);
            case UNKNOWN_REPLACES -> answer(HttpStatus.BAD_REQUEST,
                    "{\"error\":\"unknown-replaces\"}");
            case BUSY, UNAVAILABLE -> ResponseEntity
                    .status(HttpStatus.SERVICE_UNAVAILABLE)
                    .cacheControl(CacheControl.noStore())
                    .header("Retry-After", Long.toString(retryAfterSeconds))
                    .build();
        };
    }

    /**
     * Who an authentication names, to scope idempotency keys: the token's
     * issuer and subject, or the client certificate's canonical subject.
     *
     * @param authentication the authentication; may be null
     * @return the sender; null if not authenticated
     */
    static String sender(Authentication authentication)
    {
        if (authentication == null || !authentication.isAuthenticated())
        {
            return null;
        }
        if (authentication instanceof JwtAuthenticationToken token)
        {
            Jwt jwt = token.getToken();
            String issuer = jwt.getIssuer() == null ? ""
                    : jwt.getIssuer().toString();
            return "oauth2\u0000" + issuer + "\u0000" + jwt.getSubject();
        }
        return "mtls\u0000" + authentication.getName();
    }

    /**
     * An answer, never cached.
     *
     * @param status the HTTP status
     * @param json   the body; null for none
     * @return the answer
     */
    private static ResponseEntity<String> answer(HttpStatus status,
            String json)
    {
        ResponseEntity.BodyBuilder answer = ResponseEntity.status(status)
                .cacheControl(CacheControl.noStore());
        return json == null ? answer.build()
                : answer.contentType(MediaType.APPLICATION_JSON).body(json);
    }
}
