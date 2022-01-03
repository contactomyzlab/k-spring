package ve.zlab.k;

import javax.persistence.EntityManager;
import ve.zlab.k.model.KModel;
import ve.zlab.k.model.KModelDTO;
import ve.zlab.k.transaction.TransactionJPA;

/**
 * Implementation of <code>KExecutor</code>.<br><br>
 * 
 * This class provides the only method <code>{@link ve.zlab.k.KSearch#table(java.lang.String)}</code> to create a 
 * <code>{@link ve.zlab.k.KQuery}</code> and start to build any sql query you want to generate<br><br>
 * 
 * Currently only available for PostgreSQL database manager.<br><br>
 * 
 * This is an abstract class and its extends works as an interface 
 * between the persistence unit through the persistence context of your 
 * Java application and the core of KSearch.<br><br>
 *  
 * The recommended way to implement this class is as follows:<br><br>
 * 
 * <code>
 * &#32;&#32;&#32;&#32;package com.example.implementations.k;<br><br>
 * 
 * &#32;&#32;&#32;&#32;import javax.persistence.EntityManager;<br>
 * &#32;&#32;&#32;&#32;import javax.persistence.PersistenceContext;<br><br>
 * 
 * &#32;&#32;&#32;&#32;import ve.zlab.k.KExecutor;<br>
 * &#32;&#32;&#32;&#32;import ve.zlab.k.KSearch;<br><br>
 * 
 * &#32;&#32;&#32;&#32;public class K extends KSearch implements KExecutor {<br><br>
 *     
 * &#32;&#32;&#32;&#32;&#32;&#32;&#32;&#32;public K() {<br><br>
 * 		
 * &#32;&#32;&#32;&#32;&#32;&#32;&#32;&#32;}<br><br>
 * 	
 * &#32;&#32;&#32;&#32;&#32;&#32;&#32;&#32;&#64;PersistenceContext<br>
 * &#32;&#32;&#32;&#32;&#32;&#32;&#32;&#32;private EntityManager entityManager;<br><br>
 * 
 * &#32;&#32;&#32;&#32;&#32;&#32;&#32;&#32;&#64;Override<br>
 * &#32;&#32;&#32;&#32;&#32;&#32;&#32;&#32;public EntityManager getEntityManager() {<br>
 * &#32;&#32;&#32;&#32;&#32;&#32;&#32;&#32;&#32;&#32;&#32;&#32;return entityManager;<br>
 * &#32;&#32;&#32;&#32;&#32;&#32;&#32;&#32;}<br>
 * &#32;&#32;&#32;&#32;}<br>
 * </code>
 * 
 * @author contacto@myzlab.com
 * 
 */


public abstract class KSearch implements KExecutor {
    
    public abstract EntityManager getEntityManager();

    public KSearch() {
        super();
    }
    
    /**
     * Initial method for creating dynamic sql.
     * This method is always the first one you need to run to get started with dynamic sqls creation.<br><br>
     * 
     * The param <code>table</code> is used in different ways depending on the option to be executed (SELECT, INSERT, UPDATE, DELETE), as follows:<br><br>
     * 
     * <b>SELECT</b><br><br>
     * 
     * <code>SELECT <b>select_column1</b>, <b>select_column2</b>, ... FROM <b>table</b> ...</code><br><br>
     * 
     * <b>INSERT</b><br><br>
     * 
     * <code>INSERT INTO <b>table</b> ...</code><br><br>
     * 
     * <b>UPDATE</b><br><br>
     * 
     * <code>UPDATE <b>table</b> ...</code><br><br>
     * 
     * <b>DELETE</b><br><br>
     * 
     * <code>DELETE FROM <b>table</b> ...</code><br><br>
     * 
     * @param table The name of the table in the database
     * @return A {@link ve.zlab.k.KQuery} object
     */
    @Override
    public KQuery table(final String table) {
        return new KQuery(table, new TransactionJPA(getEntityManager()));
    }
    
    @Override
    public KQuery table(final Class<? extends KModel> clazz) {
        try {
            return new KQuery(new TransactionJPA(getEntityManager()), clazz.newInstance().getEntityClass());
        } catch (InstantiationException | IllegalAccessException ex) {
            return null;
        }
    }
    
    @Override
    public KQuery table(final KModelDTO kModel) {
        return new KQuery(new TransactionJPA(getEntityManager()), kModel);
    }
    
    @Override
    public KQuery function(final String function) {
        return new KQuery(function, true, new TransactionJPA(getEntityManager()));
    }
    
//    @Override
//    public <T> T insert(final KModel kModel) {
//        return KLogic.insert(kModel, getEntityManager());
//    }
//    
//    @Override
//    public void insertNoReturn(final KModel kModel) {
//        KLogic.insertNoReturn(kModel, getEntityManager());
//    }
//    
//    @Override
//    public void updateById(final KModel kModel) throws KException {
//        KLogic.updateById(kModel, getEntityManager());
//    }
    
    @Override
    public KRaw raw(final String raw) {
        return new KRaw(raw);
    }
    
    @Override
    public void deleteById(final KCollection kCollection) throws KException {
        if (kCollection.isEmpty()) {
            return;
        }
        
        new KQuery(kCollection.getTable(), new TransactionJPA(getEntityManager())).
            whereIn("id", kCollection.pluckLong("id")).
            delete();
    }
    
    @Override
    public void deleteById(final KRow kRow) throws KException {
        if (kRow.isNull()) {
            return;
        }
        
        new KQuery(kRow.getTable(), new TransactionJPA(getEntityManager())).
            where("id", kRow.getLong("id")).
            delete();
    }
}
