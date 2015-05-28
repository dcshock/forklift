package forklift.consumer;

import forklift.decorators.OnMessage;
import forklift.decorators.OnValidate;
import forklift.decorators.Topic;

@Topic("test-topics")
public class MultTConsumer {
    @OnMessage
    public void handle() {
    }
    
    @OnValidate
    public boolean validate() {
    	return true;
    }
}
