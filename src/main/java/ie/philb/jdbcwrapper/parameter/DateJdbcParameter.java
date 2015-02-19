/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ie.philb.jdbcwrapper.parameter;

import java.util.Date;

/**
 *
 * @author philb
 */
public class DateJdbcParameter extends JdbcParameter<Date> {

    private final Date date;

    public DateJdbcParameter(String key, Date date) {
        super(key);
        this.date = new Date(date.getTime());
    }

    @Override
    public Date getValue() {
        return date;
    }

}
