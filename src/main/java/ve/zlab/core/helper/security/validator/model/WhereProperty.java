/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ve.zlab.core.helper.security.validator.model;

import ve.zlab.k.KQuery;

/**
 *
 * @author karol
 */
public abstract class WhereProperty {
    
    public abstract KQuery apply(final KQuery kQuery);
    
    public static class WhereEqual extends WhereProperty {

        private String property;
        private Object value;

        public WhereEqual(final String property, final Object value) {
            this.property = property;
            this.value = value;
        }

        public String getProperty() {
            return property;
        }

        public void setProperty(String property) {
            this.property = property;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        @Override
        public KQuery apply(final KQuery kQuery) {
            return kQuery.where(property, value);
        }
    }
    
    public static class WhereNull extends WhereProperty {

        private String property;

        public WhereNull(final String property) {
            this.property = property;
        }

        public String getProperty() {
            return property;
        }

        public void setProperty(String property) {
            this.property = property;
        }

        @Override
        public KQuery apply(final KQuery kQuery) {
            return kQuery.whereNull(property);
        }
    }
}
