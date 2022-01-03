package ve.zlab.core.helper.security.validator.model;

import org.springframework.http.HttpStatus;
import ve.zlab.k.KException;
import ve.zlab.k.KExecutor;

public class SqlNotExistsByColumn extends KRule {
    
    private String table;
    private String column;

    public SqlNotExistsByColumn(final String table, final String column, final String message) {
        super(message, KRule.SQL_NOT_EXISTS_BY_COLUMN);
        
        this.table = table;
        this.column = column;
    }
    
    public void validate(final KExecutor K, final Object value) throws KException {
        if (K == null) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "K reference is mandatory -- SqlNotExistsByColumn");
        }
        
        if (value == null) {
            return;
        }
        
        K.
        table(this.table).
        where(this.column, value).
        assertNotExists(HttpStatus.BAD_REQUEST, message);
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }
}
