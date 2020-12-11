package uk.gov.homeoffice.borders.workflow.process;

import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.BpmnError;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ProcessStartDelegate implements JavaDelegate {

    @Override
    public void execute(DelegateExecution execution) {
        try {

            Object payload = execution.getVariable("payload");
            String variableName = execution.getVariable("variableName").toString();
            String businessKey = execution.getVariable("businessKey").toString();
            String processKey = execution.getVariable("processKey").toString();

            execution.getProcessEngineServices()
                    .getRuntimeService()
                    .createProcessInstanceByKey(processKey)
                    .businessKey(businessKey)
                    .setVariable("type", "non-notification")
                    .setVariable(variableName, payload)
                    .execute();

        } catch (Exception e) {
            log.error("Failed to start workflow", e);
            throw new BpmnError("FAILED_TO_START_EVENT", e.getMessage());
        }

    }
}
