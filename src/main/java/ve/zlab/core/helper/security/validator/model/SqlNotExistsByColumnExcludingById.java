package ve.zlab.core.helper.security.validator.model;

import org.springframework.http.HttpStatus;
import ve.zlab.k.KException;
import ve.zlab.k.KExecutor;

public class SqlNotExistsByColumnExcludingById extends KRule {
    
    private String table;
    private String column;
    private Long excluding;

    public SqlNotExistsByColumnExcludingById(final String table, final String column, final Long excluding, final String message) {
        super(message, KRule.SQL_NOT_EXISTS_BY_COLUMN_EXCLUDING_BY_ID);
        
        this.table = table;
        this.column = column;
        this.excluding = excluding;
    }
    
    public void validate(final KExecutor K, final Object value) throws KException {
        if (K == null) {
            throw new KException(HttpStatus.INTERNAL_SERVER_ERROR, "K reference is mandatory -- SqlNotExistsByColumnExcludingById");
        }
        
        if (value == null) {
            return;
        }
        
        K.
        table(this.table).
        where(this.column, value).
        whereNotEqual("id", excluding).
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

    public Long getExcluding() {
        return excluding;
    }

    public void setExcluding(Long excluding) {
        this.excluding = excluding;
    }
}
