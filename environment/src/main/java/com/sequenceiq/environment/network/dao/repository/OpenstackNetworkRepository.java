package com.sequenceiq.environment.network.dao.repository;

import javax.transaction.Transactional;

import com.sequenceiq.environment.network.dao.domain.OpenstackNetwork;

@Transactional(Transactional.TxType.REQUIRED)
public interface OpenstackNetworkRepository extends BaseNetworkRepository<OpenstackNetwork> {
}
