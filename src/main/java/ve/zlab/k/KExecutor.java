package ve.zlab.k;

import javax.ejb.Local;
import ve.zlab.k.model.KModel;
import ve.zlab.k.model.KModelDTO;

@Local
public interface KExecutor {
    
    public KQuery sequence(final String sequence);
    
    public KQuery table(final String table);
    
    public KQuery table(final Class<? extends KModel> clazz);
    
    public KQuery table(final KModelDTO kModel);
    
    public KQuery function(final String function);
    
//    public <T> T insert(final KModel kModel);
//    
//    public void insertNoReturn(final KModel kModel);
//    
//    public void updateById(final KModel kModel) throws KException;
    
    public KRaw raw(final String raw);
    
    public void deleteById(final KCollection kCollection) throws KException;
    
    public void deleteById(final KRow kRow) throws KException;
}
