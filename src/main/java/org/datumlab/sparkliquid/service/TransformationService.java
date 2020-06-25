package org.datumlab.sparkliquid.service;

import java.util.List;
import java.util.Map;

public interface TransformationService {
    public Map<String, Object> transformDataset(List<Map<String,Object>> transformationOperation, String target, String filename);
}