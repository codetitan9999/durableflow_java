package io.durableflow.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WorkflowDefinitionsTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void acceptsLinearChainAndFindsNextTask() throws Exception {
        var spec = WorkflowDefinitions.parse(mapper.readTree("""
                {"entry_task":"validate-order","tasks":[
                  {"name":"validate-order","handler_key":"sample.echo","next_task":"send-confirmation","max_attempts":3,"backoff_seconds":15},
                  {"name":"send-confirmation","handler_key":"notifications.send"}]}
                """));
        assertEquals("validate-order", WorkflowDefinitions.entry(spec).name());
        assertEquals("send-confirmation", WorkflowDefinitions.next(spec, "validate-order").name());
        assertNull(WorkflowDefinitions.next(spec, "send-confirmation"));
        assertEquals(3, WorkflowDefinitions.entry(spec).maxAttempts());
    }

    @Test
    void rejectsMissingOrSelfReferencingNextTask() throws Exception {
        for (var next : new String[]{"missing", "first"}) {
            var raw = mapper.readTree("{" +
                    "\"entry_task\":\"first\",\"tasks\":[{\"name\":\"first\",\"handler_key\":\"sample.echo\",\"next_task\":\"" + next + "\"}]}");
            assertThrows(IllegalArgumentException.class, () -> WorkflowDefinitions.parse(raw));
        }
    }

    @Test
    void rejectsNegativeRetrySettingsAndMissingFields() throws Exception {
        for (var raw : new String[]{
                "{}",
                "{\"entry_task\":\"first\",\"tasks\":[]}",
                "{\"entry_task\":\"first\",\"tasks\":[{\"name\":\"first\",\"handler_key\":\"sample.echo\",\"max_attempts\":-1}]}",
                "{\"entry_task\":\"first\",\"tasks\":[{\"name\":\"first\",\"handler_key\":\"sample.echo\",\"backoff_seconds\":-5}]}"
        }) {
            assertThrows(IllegalArgumentException.class, () -> WorkflowDefinitions.parse(mapper.readTree(raw)));
        }
    }

    @Test
    void matchesDeadLetterLimitBounds() {
        assertEquals(100, WorkflowDefinitions.deadLetterLimit(0));
        assertEquals(100, WorkflowDefinitions.deadLetterLimit(-1));
        assertEquals(25, WorkflowDefinitions.deadLetterLimit(25));
        assertEquals(500, WorkflowDefinitions.deadLetterLimit(501));
    }
}
