package com.hdfc.repository;

import com.hdfc.entity.LoanDO;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LoanRepository extends JpaRepository<LoanDO, Long> {
}
