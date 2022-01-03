package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class LongMax extends KRule {
    
    private Long max;

    public LongMax(final Long max, final String message) {
        super(message, KRule.LONG_MAX);
        
        this.max = max;
    }
    
    public void validate(final Long value) throws KException {
        if (value == null) {
            return;
        }
        
        if (value > max) {
            throw KExceptionHelper.badRequest(message);
        }
    }

    public Long getMax() {
        return max;
    }

    public void setMax(Long max) {
        this.max = max;
    }
}
