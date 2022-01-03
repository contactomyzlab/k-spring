package ve.zlab.k.transaction;

import ve.zlab.k.query.IQuery;

public interface ITransaction {
    
    public abstract IQuery createNativeQuery(final String ql);
}
