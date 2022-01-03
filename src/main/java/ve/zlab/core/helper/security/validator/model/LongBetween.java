package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class LongBetween extends KRule {
    
    private Long from;
    private Long to;

    public LongBetween(final Long from, final Long to, final String message) {
        super(message, KRule.LONG_BETWEEN);
        
        this.from = from;
        this.to = to;
    }
    
    public void validate(final Long value) throws KException {
        if (value == null) {
            return;
        }
        
        if (value < from) {
            throw KExceptionHelper.badRequest(message);
        }
        
        if (value > to) {
            throw KExceptionHelper.badRequest(message);
        }
    }

    public Long getFrom() {
        return from;
    }

    public void setFrom(Long from) {
        this.from = from;
    }

    public Long getTo() {
        return to;
    }

    public void setTo(Long to) {
        this.to = to;
    }
}
