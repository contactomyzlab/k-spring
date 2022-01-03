package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class IntegerGreaterThanOrEqualTo extends KRule {
    
    private Integer greaterThanOrEqualTo;

    public IntegerGreaterThanOrEqualTo(final Integer greaterThanOrEqualTo, final String message) {
        super(message, KRule.INTEGER_GREATER_THAN_OR_EQUAL_TO);
        
        this.greaterThanOrEqualTo = greaterThanOrEqualTo;
    }
    
    public void validate(final Integer value) throws KException {
        if (value == null) {
            return;
        }

        if (value < greaterThanOrEqualTo) {
            throw KExceptionHelper.badRequest(message);
        }
    }

    public Integer getGreaterThanOrEqualTo() {
        return greaterThanOrEqualTo;
    }

    public void setGreaterThanOrEqualTo(Integer greaterThanOrEqualTo) {
        this.greaterThanOrEqualTo = greaterThanOrEqualTo;
    }
}
