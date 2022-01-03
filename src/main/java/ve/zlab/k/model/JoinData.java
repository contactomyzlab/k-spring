package ve.zlab.k.model;

public class JoinData {
    
    private String table;
    private String firstColumn;
    private String secondColumn;

    public JoinData(final String table, final String firstColumn, final String secondColumn) {
        this.table = table;
        this.firstColumn = firstColumn;
        this.secondColumn = secondColumn;
    }
    
    public JoinData(final Class<? extends KModelDTO> clazz, final String firstColumn, final String secondColumn) {
        try {
            this.table = clazz.newInstance().getTableWithAlias();
            this.firstColumn = firstColumn;
            this.secondColumn = secondColumn;
        } catch (InstantiationException | IllegalAccessException ex) {
            
        }
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getFirstColumn() {
        return firstColumn;
    }

    public void setFirstColumn(String firstColumn) {
        this.firstColumn = firstColumn;
    }

    public String getSecondColumn() {
        return secondColumn;
    }

    public void setSecondColumn(String secondColumn) {
        this.secondColumn = secondColumn;
    }
}
