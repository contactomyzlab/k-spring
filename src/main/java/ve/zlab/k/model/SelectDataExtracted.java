package ve.zlab.k.model;

import java.util.List;

public class SelectDataExtracted {
    
    private String columnName;
    private List<String> namesAvailablesToOrder;

    public SelectDataExtracted(final String columnName, final List<String> namesAvailablesToOrder) {
        this.columnName = columnName;
        this.namesAvailablesToOrder = namesAvailablesToOrder;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public List<String> getNamesAvailablesToOrder() {
        return namesAvailablesToOrder;
    }

    public void setNamesAvailablesToOrder(List<String> namesAvailablesToOrder) {
        this.namesAvailablesToOrder = namesAvailablesToOrder;
    }
}
