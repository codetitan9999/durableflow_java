package io.durableflow.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.durableflow.domain.Models;
import io.durableflow.orchestrator.WorkflowService;
import io.durableflow.persistence.StoreExceptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class WorkflowControllerTest {
    private WorkflowService service;
    private MockMvc mvc;

    @BeforeEach
    void setup() {
        service = mock(WorkflowService.class);
        mvc = MockMvcBuilders.standaloneSetup(new WorkflowController(service))
                .setControllerAdvice(new ApiErrorAdvice()).build();
    }

    @Test
    void malformedWorkflowBodyReturnsGoErrorShape() throws Exception {
        mvc.perform(post("/api/workflows").contentType(MediaType.APPLICATION_JSON).content("{"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.error").value("invalid JSON body"));
    }

    @Test
    void wrongMethodReturnsPlainText405() throws Exception {
        mvc.perform(get("/api/workflows"))
                .andExpect(status().isMethodNotAllowed()).andExpect(content().string("method not allowed\n"));
    }

    @Test
    void deadLetterAliasesAndLimitValidationMatch() throws Exception {
        when(service.deadLettered(0)).thenReturn(java.util.List.of());
        mvc.perform(get("/api/dead-letter-tasks"))
                .andExpect(status().isOk()).andExpect(content().string("null"));
        mvc.perform(get("/api/dead-lettered-tasks?limit=oops"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.error").value("limit must be a valid integer"));
        mvc.perform(get("/api/dead-lettered-tasks?limit=0"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.error").value("limit must be greater than 0"));
    }

    @Test
    void replayRejectsNonDeadLetteredTask() throws Exception {
        when(service.replay("task-1")).thenThrow(new StoreExceptions.NotReplayable());
        mvc.perform(post("/api/tasks/task-1/replay"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.error").value("only dead-lettered tasks can be replayed"));
    }

    @Test
    void triggerReportsUnknownDefinition() throws Exception {
        when(service.trigger(eq("missing"), any())).thenThrow(new StoreExceptions.NotFound());
        mvc.perform(post("/api/executions").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"workflow_definition_id\":\"missing\",\"input\":{}}"))
                .andExpect(status().isNotFound()).andExpect(jsonPath("$.error").value("workflow definition not found"));
    }
}
