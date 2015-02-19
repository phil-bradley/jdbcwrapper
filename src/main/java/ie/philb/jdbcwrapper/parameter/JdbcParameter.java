/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ie.philb.jdbcwrapper.parameter;

/**
 *
 * @author pbradley
 */
public abstract class JdbcParameter<T> {

    private final String key;
    
    public JdbcParameter(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

//    public abstract String getEscapedValue();
//    
//    public abstract String getValueForEmbedding();
//    
//    public abstract String getValueAsString();
    
    public abstract T getValue();
}
