package foo.kafka.tx.consumer.service;

public class NonRetryableProcessingException extends RuntimeException {
    public NonRetryableProcessingException(String message, Throwable cause) {
        super(message, cause);
    }

    public NonRetryableProcessingException(Throwable cause) {
        super(cause == null ? null : cause.toString(), cause);
    }
}

