package ve.zlab.k.query;

import java.util.List;
import java.util.Map;
import javax.persistence.NoResultException;
import javax.persistence.NonUniqueResultException;
import javax.persistence.Query;
import ve.zlab.k.KRow;

public class QueryJPA implements IQuery {
    
    private Query query;
    
    public QueryJPA(final Query query) {
        super();
        
        this.query = query;
    }

    @Override
    public void setParameter(int i, Object o) {
        query.setParameter(i++, o);
    }

    @Override
    public Object getSingleResult() {
        return query.getSingleResult();
    }

    @Override
    public List<Object> getResultList() {
        return query.getResultList();
    }

    @Override
    public int executeUpdate() {
        return query.executeUpdate();
    }

    @Override
    public KRow getSingleKRow(final Map<String, Integer> columns, final String table) {
        Object result = null;
        
        try {
            result = query.getSingleResult();
            
            if (result == null) {
                return new KRow(new Object[]{
                    null
                }, columns, table);
            }

            return new KRow((Object[]) result, columns, table);
        } catch (NoResultException | NonUniqueResultException e) {
//            e.printStackTrace();
            return new KRow();
        } catch (ClassCastException e) {
            return new KRow(new Object[]{
                result
            }, columns, table);
        } catch (Exception e) {
            e.printStackTrace();

            return null;
        }
    }
}
