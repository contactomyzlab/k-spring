package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class DoubleGreaterThanOrEqualTo extends KRule {
    
    private Double greaterThan;

    public DoubleGreaterThanOrEqualTo(final Double greaterThan, final String message) {
        super(message, KRule.DOUBLE_GREATER_THAN_OR_EQUAL_TO);
        
        this.greaterThan = greaterThan;
    }
    
    public void validate(final Double value) throws KException {
        if (value == null) {
            return;
        }

        if (value < greaterThan) {
            throw KExceptionHelper.badRequest(message);
        }
    }

    public Double getGreaterThan() {
        return greaterThan;
    }

    public void setGreaterThan(Double greaterThan) {
        this.greaterThan = greaterThan;
    }
}
