package ve.zlab.k.query;

import java.util.List;
import java.util.Map;
import ve.zlab.k.KRow;

public interface IQuery {
    
    public abstract void setParameter(final int i, final Object o);
    
    public abstract Object getSingleResult();
    
    public abstract KRow getSingleKRow(final Map<String, Integer> columns, final String table);
    
    public abstract List<Object> getResultList();
    
    public abstract int executeUpdate();
}
