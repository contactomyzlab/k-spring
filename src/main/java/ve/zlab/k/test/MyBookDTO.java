package ve.zlab.k.test;

import lombok.Getter;
import lombok.NoArgsConstructor;
import ve.zlab.k.annotations.Column;
import ve.zlab.k.annotations.Id;
import ve.zlab.k.annotations.KClass;
import ve.zlab.k.model.KModelDTO;
import ve.zlab.k.annotations.KTable;

@Getter
@NoArgsConstructor
@KTable(name = "my_book", alias = "mb")
@KClass(entityClass = MyBookDTO.class, columnIdClass = Long.class)
public class MyBookDTO extends KModelDTO {
    
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String MY_CUSTOMER_ID = "my_customer_id";
    
    @Id(name = ID)
    @Column(name = ID)
    private Long id;
    
    @Column(name = NAME)
    private String name;
    
    @Column(name = MY_CUSTOMER_ID)
    private Long myCustomerId;

    public MyBookDTO setName(String name) {
        this.name = name;
        this.addFieldToUpdate(NAME);
        
        return this;
    }

    public MyBookDTO setId(Long id) {
        this.id = id;
        this.addFieldToUpdate(ID);
        
        return this;
    }

    public MyBookDTO setMyCustomerId(Long myCustomerId) {
        this.myCustomerId = myCustomerId;
        this.addFieldToUpdate(MY_CUSTOMER_ID);
        
        return this;
    }
    
    @Override
    public String getNameColumnId() {
        return ID;
    }
}
