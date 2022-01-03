package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class StringMax extends KRule {
    
    private Integer max;

    public StringMax(final Integer max, final String message) {
        super(message, KRule.STRING_MAX);
        
        this.max = max;
    }
    
    public void validate(final String value) throws KException {
        if (value == null || value.trim().isEmpty()) {
            return;
        }
        
        if (value.length() > max) {
            throw KExceptionHelper.badRequest(message);
        }
    }

    public Integer getMax() {
        return max;
    }

    public void setMax(Integer max) {
        this.max = max;
    }
}
