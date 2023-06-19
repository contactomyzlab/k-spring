package ve.zlab.k.test;

import ve.zlab.k.KCollection;
import ve.zlab.k.KExecutor;
import ve.zlab.k.KQuery;
import ve.zlab.k.KRaw;
import ve.zlab.k.KRow;
import ve.zlab.k.KSearch;
import ve.zlab.k.model.KModelDTO;
import ve.zlab.k.model.KModel;

public class KTest {
    
    public static KExecutor K() {
        return new KExecutor() {

            @Override
            public KQuery table(String table) {
                return new KQuery(table, null);
            }
            
            @Override
            public KQuery table(Class<? extends KModel> clazz) {
                try {
                    return new KQuery(null, clazz.newInstance().getEntityClass());
                } catch (InstantiationException | IllegalAccessException ex) {
                    return null;
                }
            }
            
            @Override
            public KQuery table(final KQuery kQuery, final String alias) {
                return new KQuery(kQuery, alias, null);
            }
            
            @Override
            public KQuery table(KModelDTO kModel) {
                return new KQuery(null, kModel);
            }

            @Override
            public KRaw raw(String raw) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void deleteById(KCollection kCollection) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void deleteById(KRow kRow) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public KQuery function(String function) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public KQuery sequence(String sequence) {
                throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
            }

            @Override
            public KSearch use(String entityManagerName) {
                throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
            }
        };
    }
}