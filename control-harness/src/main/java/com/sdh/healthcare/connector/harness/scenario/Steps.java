/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.scenario;

import static com.sdh.healthcare.connector.harness.Harness.OPERATOR;
import static com.sdh.healthcare.connector.harness.Harness.VIEWER;

import com.sdh.healthcare.connector.harness.Harness;
import com.sdh.healthcare.connector.harness.HarnessFailure;
import com.sdh.healthcare.connector.harness.Operator;
import com.sdh.healthcare.connector.harness.Reply;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Steps the scenarios share: pushing an item, following it through a
 * stage, and the calls every action needs.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class Steps
{

    /** How long an item may take to pass a stage. */
    static final Duration ITEM = Duration.ofMinutes(3);

    /** Not instantiated. */
    private Steps()
    {
    }

    /**
     * Pushes an item and checks the intake took it.
     *
     * @param harness the harness
     * @param body    the item's bytes
     * @return its item ID
     * @throws Exception if the intake refuses it
     */
    static String push(Harness harness, byte[] body) throws Exception
    {
        return push(harness, body, Map.of());
    }

    /**
     * Pushes an item with headers and checks the intake took it.
     *
     * @param harness the harness
     * @param body    the item's bytes
     * @param headers the request's headers
     * @return its item ID
     * @throws Exception if the intake refuses it
     */
    static String push(Harness harness, byte[] body,
            Map<String, String> headers) throws Exception
    {
        Reply reply = harness.target().push(body, headers).expect(202);
        String itemId = reply.field("itemId");
        Harness.say("Pushed item " + itemId + " (" + body.length
                + " bytes)");
        return itemId;
    }

    /**
     * An item's progress at a stage.
     *
     * @param harness the harness
     * @param stage   the stage
     * @param itemId  the item
     * @return its progress, from {@code /items/{id}/status}; null if the
     *         stage hasn't seen it
     * @throws Exception if the stage can't be asked
     */
    @SuppressWarnings("unchecked")
    static Map<String, Object> progress(Harness harness, String stage,
            String itemId) throws Exception
    {
        Reply status = harness.stage(stage).get(VIEWER, "/items/" + itemId
                + "/status");
        if (status.status() != 200)
        {
            return null;
        }
        Map<String, Object> answer = status.json();
        if (answer.get("item") instanceof Map<?, ?> detail
                && detail.get("progress") instanceof Map<?, ?> progress)
        {
            return (Map<String, Object>) progress;
        }
        if (answer.get("checkpoint") instanceof Map<?, ?> checkpoint)
        {
            return split(itemId, (Map<String, Object>) checkpoint);
        }
        return null;
    }

    /**
     * Pushes an item once the intake takes pushes, as after a resume or an
     * outage: a {@code 503} is tried again, anything else refused.
     *
     * @param harness the harness
     * @param body    the item
     * @return its ID
     * @throws Exception if the intake doesn't take it in time
     */
    static String pushOpen(Harness harness, byte[] body) throws Exception
    {
        String[] itemId = new String[1];
        Harness.await("the intake taking pushes", ITEM, () ->
        {
            Reply reply = harness.target().push(body, Map.of());
            if (reply.status() == 202)
            {
                itemId[0] = reply.field("itemId");
                return true;
            }
            HarnessFailure.check(reply.status() == 503,
                    "The intake refused a push: " + reply);
            return false;
        });
        Harness.say("Pushed item " + itemId[0] + " (" + body.length
                + " bytes)");
        return itemId[0];
    }

    /**
     * Waits for every record of an item to have been dealt with at a
     * stage, sent or failed, as an item with an open failure is never
     * complete.
     *
     * @param harness the harness
     * @param stage   the stage
     * @param itemId  the item
     * @return its progress
     * @throws Exception if it isn't in time
     */
    static Map<String, Object> settled(Harness harness, String stage,
            String itemId) throws Exception
    {
        Map<String, Map<String, Object>> last = new HashMap<>();
        Harness.await("item " + itemId + " settled at " + stage, ITEM, () ->
        {
            Map<String, Object> progress = progress(harness, stage, itemId);
            last.put("progress", progress);
            HarnessFailure.check(progress != null
                    && number(progress, "expected") > 0
                    && number(progress, "done") == number(progress,
                            "expected"), "not settled: "
                                    + statusOf(harness, stage, itemId));
            return true;
        });
        return last.get("progress");
    }

    /**
     * The split stage's checkpoint of an item, as a later stage's progress:
     * the records it split are those expected; those sent, done; a failed
     * item, one failed; and it is complete once it ended either way.
     *
     * @param itemId     the item
     * @param checkpoint its checkpoint
     * @return its progress
     */
    static Map<String, Object> split(String itemId,
            Map<String, Object> checkpoint)
    {
        String state = String.valueOf(checkpoint.get("state"));
        boolean ended = "COMPLETED".equals(state) || "FAILED".equals(state);
        Map<String, Object> progress = new HashMap<>();
        progress.put("itemId", itemId);
        progress.put("state", state);
        progress.put("expected", checkpoint.get("records"));
        progress.put("done", checkpoint.get("sent"));
        progress.put("failed", "FAILED".equals(state) ? 1 : 0);
        progress.put("completedAt", ended ? checkpoint.get("endedAt") : null);
        return progress;
    }

    /**
     * An item's status answer, for a message: counts and states, never a
     * document's values.
     *
     * @param harness the harness
     * @param stage   the stage
     * @param itemId  the item
     * @return the answer's status and body
     * @throws Exception if it can't be asked
     */
    static String statusOf(Harness harness, String stage, String itemId)
            throws Exception
    {
        Reply status = harness.stage(stage).get(VIEWER, "/items/" + itemId
                + "/status");
        return status.status() + " " + status.body();
    }

    /**
     * Waits until a stage has completed an item.
     *
     * @param harness the harness
     * @param stage   the stage
     * @param itemId  the item
     * @return its progress once complete
     * @throws Exception if it isn't complete in time
     */
    static Map<String, Object> complete(Harness harness, String stage,
            String itemId) throws Exception
    {
        Map<String, Map<String, Object>> last = new HashMap<>();
        Harness.await("item " + itemId + " complete at " + stage, ITEM, () ->
        {
            Map<String, Object> progress = progress(harness, stage, itemId);
            last.put("progress", progress);
            HarnessFailure.check(progress != null
                    && progress.get("completedAt") != null,
                    "not complete: " + statusOf(harness, stage, itemId));
            return true;
        });
        return last.get("progress");
    }

    /**
     * A number from an answer.
     *
     * @param answer the answer
     * @param field  the field
     * @return its value; 0 if absent
     */
    static long number(Map<String, Object> answer, String field)
    {
        return answer.get(field) instanceof Number number
                ? number.longValue() : 0;
    }

    /**
     * An action's body: its reason, with fields.
     *
     * @param reason why
     * @param fields further fields, name then value
     * @return the body
     */
    static Map<String, Object> body(String reason, Object... fields)
    {
        Map<String, Object> body = new HashMap<>();
        body.put("reason", reason);
        for (int index = 0; index + 1 < fields.length; index += 2)
        {
            body.put(fields[index].toString(), fields[index + 1]);
        }
        return body;
    }

    /**
     * Asks for an operation that needs four eyes, checks it waits for
     * approval, that its requester can't approve it, and has another
     * approve it.
     *
     * @param harness   the harness
     * @param stage     the stage
     * @param requester who asks
     * @param approver  who approves
     * @param path      the action's path
     * @param body      its body
     * @return the operation's ID
     * @throws Exception if a step doesn't hold
     */
    static String fourEyes(Harness harness, String stage, Operator requester,
            Operator approver, String path, Map<String, Object> body)
            throws Exception
    {
        Reply asked = harness.stage(stage).action(requester, "POST", path,
                body).expect(202);
        HarnessFailure.check("awaiting-approval".equals(asked.field("state")),
                "Not awaiting approval: " + asked);
        String id = asked.field("id");
        Reply own = harness.stage(stage).post(requester, "/operations/" + id
                + "/approve", body("approving my own"));
        HarnessFailure.check(own.status() == 403,
                "The requester approved their own: " + own);
        harness.stage(stage).post(approver, "/operations/" + id + "/approve",
                body("approved by a second person")).expect(200);
        return id;
    }

    /**
     * Pauses or resumes a stage as the operator.
     *
     * @param harness the harness
     * @param stage   the stage
     * @param paused  whether to pause it
     * @throws Exception if it is refused
     */
    static void pause(Harness harness, String stage, boolean paused)
            throws Exception
    {
        Reply reply = harness.stage(stage).action(OPERATOR, "POST", paused
                ? "/pause" : "/resume", body(paused ? "harness pause"
                        : "harness resume")).expect(200);
        HarnessFailure.check(Boolean.valueOf(paused).equals(
                reply.json().get("paused")), "Not " + (paused ? "paused"
                        : "resumed") + ": " + reply);
    }

    /**
     * The items a scenario pushed earlier.
     *
     * @param harness the harness
     * @return their IDs; empty if none
     */
    @SuppressWarnings("unchecked")
    static List<String> loaded(Harness harness)
    {
        Object items = harness.note("loaded");
        return items == null ? List.of() : (List<String>) items;
    }
}
