package com.hdfc.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CreditDecisionEvent {
    private Long loanId;
    private int userId;
    private boolean approved;
    private String message;
}