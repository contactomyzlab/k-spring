package ve.zlab.k;

import org.springframework.http.HttpStatus;

public class KException extends Exception {

    private final HttpStatus status;

    public KException(final HttpStatus status) {
        this.status = status;
    }
    
    public KException(final HttpStatus status, final String message) {
        super(message);
        this.status = status;
    }

    public HttpStatus getStatus() {
        return status;
    }
}
