package com.hdfc.service;

import com.hdfc.events.CreditDecisionEvent;
import com.hdfc.events.LoanApplicationSubmitEvent;
import com.hdfc.exception.InSufficientCreditScoreException;
import com.hdfc.util.CreditScoreUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class CreditRiskConsumer {


    public static final String TOPIC = "credit-decision-topic";
    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;


    @KafkaListener(topics = "loan-process-topic")
    public void onLoanApplicationReceived(LoanApplicationSubmitEvent event) {
        CreditDecisionEvent creditRiskCheckResult=null;
        log.info("Received loan application event: {}", event);

        try {
            evaluateCreditRisk(event.getUserId());
            log.info("Credit risk check PASSED for userId: {}", event.getUserId());

            creditRiskCheckResult = buildCreditDecisionEvent(event, true, "Credit risk check PASSED");
            kafkaTemplate.send(TOPIC, creditRiskCheckResult);
            log.info("Credit risk service publish events {}",creditRiskCheckResult);

        } catch (InSufficientCreditScoreException ex) {
            //update
            log.warn("Credit risk check FAILED for userId: {} | Reason: {}", event.getUserId(), ex.getMessage());
            creditRiskCheckResult =buildCreditDecisionEvent(event, false, ex.getMessage());
            kafkaTemplate.send(TOPIC, creditRiskCheckResult);
            log.warn("Credit risk service publish events {}",creditRiskCheckResult);
        }
    }

    private  CreditDecisionEvent buildCreditDecisionEvent(LoanApplicationSubmitEvent event, boolean approved, String Credit_risk_check_PASSED) {
        //needs update
        return CreditDecisionEvent.builder()
                .loanId(event.getLoanId())
                .userId(event.getUserId())
                .approved(approved)
                .message(Credit_risk_check_PASSED)
                .build();
    }


    private void evaluateCreditRisk(int userId) {
        int creditScore = CreditScoreUtils
                .creditScoreResults()
                .getOrDefault(userId, 0);
        if (creditScore < 750) {
            throw new InSufficientCreditScoreException("Credit score is too low: " + creditScore);
        }
    }


}
