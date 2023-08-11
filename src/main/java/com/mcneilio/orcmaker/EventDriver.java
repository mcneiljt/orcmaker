package com.example.orcmaker;

import org.json.JSONObject;

public interface EventDriver {
    void addMessage(JSONObject message);
    String flush(boolean force);
}
