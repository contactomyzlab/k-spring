package ve.zlab.k.transaction;

import javax.persistence.EntityManager;
import ve.zlab.k.query.IQuery;
import ve.zlab.k.query.QueryJPA;

public class TransactionJPA implements ITransaction {
    
    private EntityManager entityManager;

    public TransactionJPA(final EntityManager entityManager) {
        super();
        
        this.entityManager = entityManager;
    }

    @Override
    public IQuery createNativeQuery(String ql) {
        return new QueryJPA(entityManager.createNativeQuery(ql));
    }
    
    
}
