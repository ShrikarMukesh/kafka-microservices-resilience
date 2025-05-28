package com.hdfc.service;

import com.hdfc.constants.LoanStatus;
import com.hdfc.entity.LoanDO;
import com.hdfc.events.CreditDecisionEvent;
import com.hdfc.repository.LoanRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class CreditDecisionConsumer {

    @Autowired
    private LoanRepository repository;

    // consume the CreditDecision event and update the database


    @KafkaListener(topics = "credit-decision-topic", groupId = "credit-loan-group")
    public void consumeCreditDecisionEvent(CreditDecisionEvent event) {
        log.info("CreditDecisionConsumer::consumeCreditDecisionEvent received credit decision event {}", event);

        LoanDO loan = repository.findById(event.getLoanId()).orElse(null);

        if (loan != null) {
            if (event.isApproved()) {
                loan.setStatus(LoanStatus.APPROVED);
                log.info("CreditDecisionConsumer - LoanId: {} marked as APPROVED. Proceeding with disbursement logic.", loan.getLoanId());
            } else {
                loan.setStatus(LoanStatus.REJECTED);
                log.info("CreditDecisionConsumer - LoanId: {} marked as REJECTED. No further action required ", loan.getLoanId());

            }
            repository.save(loan);
        }


    }
}
