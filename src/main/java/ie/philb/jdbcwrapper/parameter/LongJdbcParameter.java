/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ie.philb.jdbcwrapper.parameter;

public class LongJdbcParameter extends JdbcParameter<Long> {

    private final Long value;

    public LongJdbcParameter(String key, Long value) {
        super(key);
        this.value = value;
    }

    public LongJdbcParameter(String key, Integer value) {
        super(key);
        this.value = new Long(value);
    }

//    @Override
//    public String getValueAsString() {
//        return value.toString();
//    }
//
//    @Override
//    public String getEscapedValue() {
//        return getValueAsString();
//    }
//
//    @Override
//    public String getValueForEmbedding() {
//        return getValueAsString();
//    }

    @Override
    public Long getValue() {
        return value;
    }
}
