package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;

public class LongIn extends KRule {
    
    private Long[] list;

    public LongIn(final Long[] list, final String message) {
        super(message, KRule.LONG_IN);
        
        this.list = list;
    }
    
    public void validate(final Long value) throws KException {
        if (value == null) {
            return;
        }
        
        for (final Long in : list) {
            if (in.equals(value)) {
                return;
            }
        }
        
        throw KExceptionHelper.badRequest(message);
    }

    public Long[] getList() {
        return list;
    }

    public void setList(Long[] list) {
        this.list = list;
    }
}
