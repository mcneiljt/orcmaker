package com.mcneilio.orcmaker;

import org.json.JSONObject;

public interface EventDriver {

    void addMessage(String message);
    void addMessage(JSONObject message);
    String flush(boolean force);
}
