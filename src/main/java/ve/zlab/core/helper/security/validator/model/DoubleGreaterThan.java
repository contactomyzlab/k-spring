package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class DoubleGreaterThan extends KRule {
    
    private Double greaterThan;

    public DoubleGreaterThan(final Double greaterThan, final String message) {
        super(message, KRule.DOUBLE_GREATER_THAN);
        
        this.greaterThan = greaterThan;
    }
    
    public void validate(final Double value) throws KException {
        if (value == null) {
            return;
        }

        if (value <= greaterThan) {
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
