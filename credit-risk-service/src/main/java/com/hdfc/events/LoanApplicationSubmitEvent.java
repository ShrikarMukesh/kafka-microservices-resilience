package com.hdfc.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class LoanApplicationSubmitEvent {
    private long loanId;
    private int userId;
    private double amount;
    private String transactionId;
}


