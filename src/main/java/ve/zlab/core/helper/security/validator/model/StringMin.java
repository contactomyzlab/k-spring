package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class StringMin extends KRule {
    
    private Integer min;

    public StringMin(final Integer min, final String message) {
        super(message, KRule.STRING_MIN);
        
        this.min = min;
    }
    
    public void validate(final String value) throws KException {
        if (value == null || value.trim().isEmpty()) {
            return;
        }
        
        if (value.length() < min) {
            throw KExceptionHelper.badRequest(message);
        }
    }

    public Integer getMin() {
        return min;
    }

    public void setMin(Integer min) {
        this.min = min;
    }
}
