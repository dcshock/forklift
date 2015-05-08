package forklift.producers;

import javax.jms.ExceptionListener;

public interface AsyncCallback extends ExceptionListener {
    public void onSuccess();
}
