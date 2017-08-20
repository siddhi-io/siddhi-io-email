package org.wso2.extension.siddhi.io.email.source.exception;

//todo put licence files
/**
 * Created by chathurika on 7/28/17.
 */
public class EmailSourceAdaptorRuntimeException extends RuntimeException {
    public EmailSourceAdaptorRuntimeException() {
    }

    public EmailSourceAdaptorRuntimeException(String message) {
        super(message);
    }

    public EmailSourceAdaptorRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public EmailSourceAdaptorRuntimeException(Throwable cause) {
        super(cause);
    }
}
