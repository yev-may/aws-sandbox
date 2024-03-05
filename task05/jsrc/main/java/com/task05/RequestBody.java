package com.task05;

import java.util.HashMap;
import java.util.Map;

public class RequestBody {
    private int principalId;
    private Map<String, String> content = new HashMap<>();

    public int getPrincipalId() {
        return principalId;
    }

    public void setPrincipalId(int principalId) {
        this.principalId = principalId;
    }

    public Map<String, String> getContent() {
        return content;
    }

    public void setContent(Map<String, String> content) {
        this.content = content;
    }
}
