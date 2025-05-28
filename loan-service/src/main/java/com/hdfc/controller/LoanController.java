package com.hdfc.controller;

import com.hdfc.entity.LoanDO;
import com.hdfc.service.LoanService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
@RequestMapping("/api/loans")
@RequiredArgsConstructor
@Slf4j
public class LoanController {

    private final LoanService loanService;

    @PostMapping("/process")
    public ResponseEntity<String> processLoan(@RequestBody LoanDO loanDO) {
        log.info("LoanController::processLoan received loan application request for userId: {}", loanDO.getUserId());
        String loanTransactionId = loanService.processLoanApplication(loanDO);
        String responseMessage = String.format("Loan application received, credit check in progress. Transaction ID: %s", loanTransactionId);
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(responseMessage);
    }

    @GetMapping("/status")
    public ResponseEntity<?> getLoanStatus(
            @RequestParam(required = false) Long loanId) {
        Optional<LoanDO> loanStatus = loanService.getLoanStatusById(loanId);
        return loanStatus.map(loan -> ResponseEntity.ok().body(loan.getStatus()))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
