package ve.zlab.k.dak;

import java.util.ArrayList;
import java.util.List;
import org.springframework.http.HttpStatus;
import ve.zlab.core.helper.security.validator.model.WhereProperty;
import ve.zlab.k.KCollection;
import ve.zlab.k.KException;
import ve.zlab.k.KExecutor;
import ve.zlab.k.KQuery;
import ve.zlab.k.helper.sql.WherePropertyHelper;

public class GenericDAK {
    
    /**
     * Performs physical removal of a record from the indicated table according to the provided identification.
     * 
     * @param K KExecutor
     * @param table The name of the table from which you want to delete the record
     * @param id The id of the record to delete
     * @throws KException If any error happens through the database (Non-existent table or null id)
     * 
     */
    public static void deleteById(
        final KExecutor K,
        final String table,
        final Long id
    ) throws KException {
        
        K.
        table(table).
        where("id", id).
        delete();
    }
    
    public static void assertRecordExists(
        final KExecutor K,
        final String table,
        final Long id
    ) throws KException {
        GenericDAK.assertRecordExists(K, table, id, HttpStatus.NOT_FOUND, "Record not found");
    }
    
    public static void assertRecordExists(
        final KExecutor K,
        final String table,
        final Long id,
        final HttpStatus httpStatus,
        final String message
    ) throws KException {
        K.
        table(table).
        where("id", id).
        assertExists(httpStatus, message);
    }
    
    public static void assertRecordExists(
        final KExecutor K,
        final String table,
        final Long id,
        final List<WhereProperty> whereProperties,
        final HttpStatus httpStatus,
        final String message
    ) throws KException {
        
        final KQuery kQuery = 
            K.
            table(table).
            where("id", id);
            
        for (final WhereProperty whereProperty : whereProperties) {
            whereProperty.apply(kQuery);
        }
        
        kQuery.assertExists(httpStatus, message);
    }
    
    public static void assertRecordExists(
        final KExecutor K,
        final String table,
        final Long id,
        final WhereProperty whereProperty,
        final HttpStatus httpStatus,
        final String message
    ) throws KException {
        assertRecordExists(K, table, id, WherePropertyHelper.group(whereProperty), httpStatus, message);
    }
    
    public static void assertRecordExists(
        final KExecutor K,
        final String table,
        final Long id,
        final WhereProperty whereProperty
    ) throws KException {
        assertRecordExists(K, table, id, WherePropertyHelper.group(whereProperty), HttpStatus.NOT_FOUND, "Record not found");
    }
    
    public static void assertRecordExistsNotDeleted(
        final KExecutor K,
        final String table,
        final Long id
    ) throws KException {
        GenericDAK.assertRecordExistsNotDeleted(K, table, id, HttpStatus.NOT_FOUND, "Record not found");
    }
    
    public static void assertRecordExistsNotDeleted(
        final KExecutor K,
        final String table,
        final Long id,
        final HttpStatus httpStatus,
        final String message
    ) throws KException {
        assertRecordExists(K, table, id, WherePropertyHelper.group(WherePropertyHelper.notDeleted()), httpStatus, message);
    }
    
    public static void assertRecordExistsNotDeletedAndActive(
        final KExecutor K,
        final String table,
        final Long id
    ) throws KException {
        GenericDAK.assertRecordExistsNotDeletedAndActive(K, table, id, HttpStatus.NOT_FOUND, "Record not found");
    }
    
    public static void assertRecordExistsNotDeletedAndActive(
        final KExecutor K,
        final String table,
        final Long id,
        final HttpStatus httpStatus,
        final String message
    ) throws KException {
        assertRecordExists(K, table, id, WherePropertyHelper.group(WherePropertyHelper.notDeleted(), WherePropertyHelper.active()), httpStatus, message);
    }
    
    public static KCollection find(
        final KExecutor K,
        final String table,
        final String... columns
    ) throws KException {
        
        return 
            K.
            table(table).
            select(columns).
            multiple();
    }
    
    public static KCollection find(
        final KExecutor K,
        final String table,
        final List<WhereProperty> whereProperties,
        final String... columns
    ) throws KException {
        
        final KQuery kQuery = 
            K.
            table(table).
            select(columns);
            
        for (final WhereProperty whereProperty : whereProperties) {
            whereProperty.apply(kQuery);
        }
        
        return kQuery.multiple();
    }
    
    public static KCollection find(
        final KExecutor K,
        final String table,
        final WhereProperty whereProperty,
        final String... columns
    ) throws KException {
        return find(K, table, WherePropertyHelper.group(whereProperty), columns);
    }
    
    public static KCollection findNotDeletedAndActive(
        final KExecutor K,
        final String table,
        final List<WhereProperty> whereProperties,
        final String... columns
    ) throws KException {
        final List<WhereProperty> listWhereProperties = WherePropertyHelper.group(WherePropertyHelper.notDeleted(), WherePropertyHelper.active());
        
        if (whereProperties != null) {
            listWhereProperties.addAll(whereProperties);
        }
        
        return find(K, table, listWhereProperties, columns);
    }
    
    public static KCollection findNotDeletedAndActive(
        final KExecutor K,
        final String table, 
        final String... columns
    ) throws KException {
        return findNotDeletedAndActive(K, table, new ArrayList<>(), columns);
    }
    
    public static KCollection findNotDeletedAndActive(
        final KExecutor K,
        final String table,
        final WhereProperty whereProperty,
        final String... columns
    ) throws KException {
        return findNotDeletedAndActive(K, table, WherePropertyHelper.group(whereProperty), columns);
    }
    
    public static KCollection findActive(
        final KExecutor K,
        final String table,
        final List<WhereProperty> whereProperties,
        final String... columns
    ) throws KException {
        final List<WhereProperty> listWhereProperties = WherePropertyHelper.group(WherePropertyHelper.active());
        
        if (whereProperties != null) {
            listWhereProperties.addAll(whereProperties);
        }
        
        return find(K, table, listWhereProperties, columns);
    }
    
    public static KCollection findActive(
        final KExecutor K,
        final String table, 
        final String... columns
    ) throws KException {
        return findActive(K, table, new ArrayList<>(), columns);
    }
    
    public static KCollection findActive(
        final KExecutor K,
        final String table,
        final WhereProperty whereProperty,
        final String... columns
    ) throws KException {
        return findActive(K, table, WherePropertyHelper.group(whereProperty), columns);
    }
    
    public static KCollection findNotDeleted(
        final KExecutor K,
        final String table,
        final List<WhereProperty> whereProperties,
        final String... columns
    ) throws KException {
        final List<WhereProperty> listWhereProperties = WherePropertyHelper.group(WherePropertyHelper.notDeleted());
        
        if (whereProperties != null) {
            listWhereProperties.addAll(whereProperties);
        }
        
        return find(K, table, listWhereProperties, columns);
    }
    
    public static KCollection findNotDeleted(
        final KExecutor K,
        final String table, 
        final String... columns
    ) throws KException {
        return findNotDeleted(K, table, new ArrayList<>(), columns);
    }
    
    public static KCollection findNotDeleted(
        final KExecutor K,
        final String table,
        final WhereProperty whereProperty,
        final String... columns
    ) throws KException {
        return findNotDeleted(K, table, WherePropertyHelper.group(whereProperty), columns);
    }
}