package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class IntegerBetween extends KRule {
    
    private Integer from;
    private Integer to;

    public IntegerBetween(final Integer from, final Integer to, final String message) {
        super(message, KRule.INTEGER_BETWEEN);
        
        this.from = from;
        this.to = to;
    }
    
    public void validate(final Integer value) throws KException {
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

    public Integer getFrom() {
        return from;
    }

    public void setFrom(Integer from) {
        this.from = from;
    }

    public Integer getTo() {
        return to;
    }

    public void setTo(Integer to) {
        this.to = to;
    }
}
