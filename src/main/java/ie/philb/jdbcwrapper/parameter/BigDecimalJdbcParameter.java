/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ie.philb.jdbcwrapper.parameter;

import java.math.BigDecimal;

public class BigDecimalJdbcParameter extends JdbcParameter<BigDecimal> {

    private final BigDecimal value;

    public BigDecimalJdbcParameter(String key, BigDecimal value) {
        super(key);
        this.value = value;
    }

//    @Override
//    public String getValueAsString() {
//        return value.toPlainString();
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
    public BigDecimal getValue() {
        return value;
    }

}
