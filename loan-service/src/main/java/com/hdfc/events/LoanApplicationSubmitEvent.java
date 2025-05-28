package com.hdfc.events;

import lombok.*;

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


