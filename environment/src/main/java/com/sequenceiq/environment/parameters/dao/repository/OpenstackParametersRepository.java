package com.sequenceiq.environment.parameters.dao.repository;

import javax.transaction.Transactional;
import javax.transaction.Transactional.TxType;

import com.sequenceiq.environment.parameters.dao.domain.OpenstackParameters;

@Transactional(TxType.REQUIRED)
public interface OpenstackParametersRepository extends BaseParametersRepository<OpenstackParameters> {
}
