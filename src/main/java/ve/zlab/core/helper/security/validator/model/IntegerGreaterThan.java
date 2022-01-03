package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class IntegerGreaterThan extends KRule {
    
    private Integer greaterThan;

    public IntegerGreaterThan(final Integer greaterThan, final String message) {
        super(message, KRule.INTEGER_GREATER_THAN);
        
        this.greaterThan = greaterThan;
    }
    
    public void validate(final Integer value) throws KException {
        if (value == null) {
            return;
        }

        if (value <= greaterThan) {
            throw KExceptionHelper.badRequest(message);
        }
    }

    public Integer getGreaterThan() {
        return greaterThan;
    }

    public void setGreaterThan(Integer greaterThan) {
        this.greaterThan = greaterThan;
    }
}
