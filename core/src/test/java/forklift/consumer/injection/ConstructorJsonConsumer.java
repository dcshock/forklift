package forklift.consumer.injection;

import forklift.connectors.ForkliftMessage;
import forklift.decorators.Headers;
import forklift.decorators.Message;
import forklift.decorators.Properties;
import forklift.decorators.Queue;
import forklift.message.Header;

import java.util.Map;

import javax.inject.Inject;

@Queue("constructorA")
public class ConstructorJsonConsumer {

    Map<Headers, String> headers;
    String producer;
    Map<String, Object> properties;
    String strval;
    ForkliftMessage fmsg;
    Map<String, String> kvl;
    String str;
    ConsumerTest.ExpectedMsg msg;
    Person person;

    @Inject
    public ConstructorJsonConsumer(@Headers Map<Headers, String> headers,
                                   @Headers(Header.Producer) String producer,
                                   @Properties Map<String, Object> properties,
                                   @Properties("mystrval") String strval,
                                   @Message ForkliftMessage fmsg,
                                   @Message Map<String, String> kvl,
                                   @Message String str,
                                   @Message ConsumerTest.ExpectedMsg msg,
                                   Person person) {
        this.headers = headers;
        this.producer = producer;
        this.properties = properties;
        this.strval = strval;
        this.fmsg = fmsg;
        this.kvl = kvl;
        this.str = str;
        this.msg = msg;
        this.person = person;
    }

}
