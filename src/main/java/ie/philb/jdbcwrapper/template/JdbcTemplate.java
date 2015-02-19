/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ie.philb.jdbcwrapper.template;

import ie.philb.jdbcwrapper.mapping.JdbcToEntityMapper;
import ie.philb.jdbcwrapper.exception.NoSuchEntityException;
import ie.philb.jdbcwrapper.exception.JdbcWrapperException;
import ie.philb.jdbcwrapper.parameter.DateJdbcParameter;
import ie.philb.jdbcwrapper.parameter.JdbcParameter;
import ie.philb.jdbcwrapper.parameter.JdbcParameterSet;
import ie.philb.jdbcwrapper.parameter.StringJdbcParameter;
import ie.philb.jdbcwrapper.parameter.BigDecimalJdbcParameter;
import ie.philb.jdbcwrapper.parameter.LongJdbcParameter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 *
 * @author pbradley
 */
public class JdbcTemplate {

    protected static final Logger logger = Logger.getLogger(JdbcTemplate.class.getSimpleName());

    private final DataSource ds;
    private int limit = 0;
    private boolean usingPreparedStatements = false;

    public JdbcTemplate(DataSource ds) {
        this.ds = ds;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public boolean isUsingPreparedStatements() {
        return usingPreparedStatements;
    }

    public void setUsingPreparedStatements(boolean usingPreparedStatements) {
        this.usingPreparedStatements = usingPreparedStatements;
    }

    public <T> T querySingleResult(String sql, JdbcToEntityMapper<T> mapper) throws JdbcWrapperException, NoSuchEntityException {
        return querySingleResult(sql, new JdbcParameterSet(), mapper);
    }

    public <T> T querySingleResult(String sql, JdbcParameterSet parameterSet, JdbcToEntityMapper<T> mapper) throws JdbcWrapperException, NoSuchEntityException {

        T t = null;

        String normalisedSql = normalise(sql);

        Map<Integer, String> parametersNamesByIndex = getParameterNamesByIndex(normalisedSql);
        Map<Integer, JdbcParameter> parametersByIndex = new HashMap<>();

        for (Integer index : parametersNamesByIndex.keySet()) {
            String name = parametersNamesByIndex.get(index);

            for (JdbcParameter p : parameterSet.getParameters()) {
                if (p.getKey().equalsIgnoreCase(name)) {
                    parametersByIndex.put(index, p);
                }
            }
        }

        String sqlWithPlaceholders = namedParametersToPlaceholders(normalisedSql);

        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;

        try {
            conn = ds.getConnection();
            ps = conn.prepareStatement(sqlWithPlaceholders);

            if (limit > 0) {
                ps.setMaxRows(limit);
            }

            for (Integer index : parametersByIndex.keySet()) {
                JdbcParameter parameter = parametersByIndex.get(index);

                if (parameter instanceof LongJdbcParameter) {
                    LongJdbcParameter lp = (LongJdbcParameter) parameter;
                    ps.setLong(index, lp.getValue());
                }

                if (parameter instanceof BigDecimalJdbcParameter) {
                    BigDecimalJdbcParameter bdp = (BigDecimalJdbcParameter) parameter;
                    ps.setBigDecimal(index, bdp.getValue());
                }

                if (parameter instanceof StringJdbcParameter) {
                    StringJdbcParameter sdp = (StringJdbcParameter) parameter;
                    ps.setString(index, sdp.getValue());
                }
            }

            rs = ps.executeQuery();

            if (rs.next()) {
                t = mapper.mapRow(rs);
            } else {
                throw new NoSuchEntityException();
            }

        } catch (SQLException ex) {
            throw new JdbcWrapperException(sqlWithPlaceholders, ex);
        } finally {
            closeResources(conn, ps, rs);
        }

        return t;
    }

    public <T> List<T> queryResultList(String sql, JdbcToEntityMapper<T> mapper) throws JdbcWrapperException {
        return queryResultList(sql, new JdbcParameterSet(), mapper);
    }

    public <T> List<T> queryResultList(String sql, JdbcParameterSet parameterSet, JdbcToEntityMapper<T> mapper) throws JdbcWrapperException {

        String normalisedSql = normalise(sql);

        List<T> results = new ArrayList<>();

        Map<Integer, String> parametersNamesByIndex = getParameterNamesByIndex(normalisedSql);
        Map<Integer, JdbcParameter> parametersByIndex = new HashMap<>();

        for (Integer index : parametersNamesByIndex.keySet()) {
            String name = parametersNamesByIndex.get(index);

            for (JdbcParameter p : parameterSet.getParameters()) {
                if (p.getKey().equalsIgnoreCase(name)) {
                    parametersByIndex.put(index, p);
                }
            }
        }

        String sqlWithPlaceholders = namedParametersToPlaceholders(normalisedSql);

        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;

        try {
            conn = ds.getConnection();
            ps = conn.prepareStatement(sqlWithPlaceholders);

            if (limit > 0) {
                ps.setMaxRows(limit);
            }

            for (Integer index : parametersByIndex.keySet()) {
                JdbcParameter parameter = parametersByIndex.get(index);

                if (parameter instanceof LongJdbcParameter) {
                    LongJdbcParameter lp = (LongJdbcParameter) parameter;
                    ps.setLong(index, lp.getValue());
                }

                if (parameter instanceof BigDecimalJdbcParameter) {
                    BigDecimalJdbcParameter bdp = (BigDecimalJdbcParameter) parameter;
                    ps.setBigDecimal(index, bdp.getValue());
                }

                if (parameter instanceof StringJdbcParameter) {
                    StringJdbcParameter sdp = (StringJdbcParameter) parameter;
                    ps.setString(index, sdp.getValue());
                }
            }

            rs = ps.executeQuery();

            while (rs.next()) {
                T t = mapper.mapRow(rs);
                results.add(t);
            }
        } catch (SQLException ex) {
            throw new JdbcWrapperException(sqlWithPlaceholders, ex);
        } finally {
            closeResources(conn, ps, rs);
        }

        return results;
    }

    public long executeInsert(String sql, JdbcParameterSet parameterSet) throws JdbcWrapperException {

        String normalisedSql = normalise(sql);
                
        long id = 0;

        Map<Integer, String> parametersNamesByIndex = getParameterNamesByIndex(normalisedSql);
        Map<Integer, JdbcParameter> parametersByIndex = new HashMap<>();

        for (Integer index : parametersNamesByIndex.keySet()) {
            String name = parametersNamesByIndex.get(index);

            for (JdbcParameter p : parameterSet.getParameters()) {
                if (p.getKey().equalsIgnoreCase(name)) {
                    parametersByIndex.put(index, p);
                }
            }
        }

        String sqlWithPlaceholders = namedParametersToPlaceholders(normalisedSql);

        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;

        try {
            conn = ds.getConnection();
            ps = conn.prepareStatement(sqlWithPlaceholders, Statement.RETURN_GENERATED_KEYS);

            if (limit > 0) {
                ps.setMaxRows(limit);
            }

            for (Integer index : parametersByIndex.keySet()) {
                JdbcParameter parameter = parametersByIndex.get(index);

                if (parameter instanceof LongJdbcParameter) {
                    LongJdbcParameter lp = (LongJdbcParameter) parameter;
                    ps.setLong(index, lp.getValue());
                }

                if (parameter instanceof BigDecimalJdbcParameter) {
                    BigDecimalJdbcParameter bdp = (BigDecimalJdbcParameter) parameter;
                    ps.setBigDecimal(index, bdp.getValue());
                }

                if (parameter instanceof StringJdbcParameter) {
                    StringJdbcParameter sdp = (StringJdbcParameter) parameter;
                    ps.setString(index, sdp.getValue());
                }
            }

            ps.executeUpdate();

            rs = ps.getGeneratedKeys();

            if (rs.next()) {
                id = rs.getInt(1);
            }

        } catch (SQLException ex) {
            throw new JdbcWrapperException(sqlWithPlaceholders, ex);
        } finally {
            closeResources(conn, ps, rs);
        }

        return id;
    }

    public Long createEntity(String entityName, JdbcParameterSet parameterSet) throws JdbcWrapperException {
        String sql = "INSERT INTO __ENTITY__ (__FIELDS__) VALUES (__VALUES__) ";

        String fieldNames = "";
        String fieldPlaceholders = "";

        for (JdbcParameter parameter : parameterSet.getParameters()) {

            if (!fieldNames.isEmpty()) {
                fieldNames += ", ";
                fieldPlaceholders += ", ";
            }
            fieldNames += parameter.getKey();
            fieldPlaceholders += ":" + parameter.getKey();
        }

        sql = sql.replace("__FIELDS__", fieldNames);
        sql = sql.replace("__VALUES__", fieldPlaceholders);
        sql = sql.replace("__ENTITY__", entityName);

        UpdateResponseBean response = executeUpdate(sql, parameterSet, true);
        return response.generatedKey;
    }

    public int executeUpdate(String sql, JdbcParameterSet parameterSet) throws JdbcWrapperException {
        UpdateResponseBean r = executeUpdate(sql, parameterSet, false);
        return r.returnValue;
    }

    private UpdateResponseBean executeUpdate(String sql, JdbcParameterSet parameterSet, boolean returnGeneratedKey) throws JdbcWrapperException {

        String normalisedSql = normalise(sql);
        
        UpdateResponseBean response = new UpdateResponseBean();

        Map<Integer, String> parametersNamesByIndex = getParameterNamesByIndex(normalisedSql);
        Map<Integer, JdbcParameter> parametersByIndex = new HashMap<>();

        for (Integer index : parametersNamesByIndex.keySet()) {
            String name = parametersNamesByIndex.get(index);

            for (JdbcParameter p : parameterSet.getParameters()) {
                if (p.getKey().equalsIgnoreCase(name)) {
                    parametersByIndex.put(index, p);
                }
            }
        }

        String sqlWithPlaceholders = namedParametersToPlaceholders(normalisedSql);

        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;

        try {
            conn = ds.getConnection();

            int GENERATED_KEYS_BEHAVIOUR = returnGeneratedKey ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS;

            ps = conn.prepareStatement(sqlWithPlaceholders, GENERATED_KEYS_BEHAVIOUR);

            for (Integer index : parametersByIndex.keySet()) {
                JdbcParameter parameter = parametersByIndex.get(index);

                if (parameter instanceof LongJdbcParameter) {
                    LongJdbcParameter lp = (LongJdbcParameter) parameter;
                    ps.setLong(index, lp.getValue());
                }

                if (parameter instanceof BigDecimalJdbcParameter) {
                    BigDecimalJdbcParameter bdp = (BigDecimalJdbcParameter) parameter;
                    ps.setBigDecimal(index, bdp.getValue());
                }

                if (parameter instanceof StringJdbcParameter) {
                    StringJdbcParameter sdp = (StringJdbcParameter) parameter;
                    ps.setString(index, sdp.getValue());
                }
                
                if (parameter instanceof DateJdbcParameter) {
                    DateJdbcParameter ddp = (DateJdbcParameter) parameter;
                    Timestamp ts = new Timestamp(ddp.getValue().getTime());
                    ps.setTimestamp(index, ts);
                }
            }

            response.returnValue = ps.executeUpdate();

            if (returnGeneratedKey) {
                rs = ps.getGeneratedKeys();

                if (rs.next()) {
                    response.generatedKey = rs.getLong(1);
                }
            }

        } catch (SQLException ex) {
            throw new JdbcWrapperException(sqlWithPlaceholders, ex);
        } finally {
            closeResources(conn, ps, rs);
        }

        return response;
    }

    private void closeResources(Connection conn, Statement st, ResultSet rs) {
        closeResultSet(rs);
        closeStatement(st);
        closeConnection(conn);
    }

    private void closeStatement(Statement st) {
        if (st == null) {
            return;
        }

        try {
            st.close();
        } catch (Exception ex) {

        }
    }

    private void closeResultSet(ResultSet rs) {
        if (rs == null) {
            return;
        }

        try {
            rs.close();
        } catch (Exception ex) {

        }
    }

    private void closeConnection(Connection conn) {
        if (conn == null) {
            return;
        }

        try {
            conn.close();
        } catch (Exception ex) {

        }
    }

    private <T> boolean containsParameter(String sql, JdbcParameter<T> p) {
        String k = ":" + p.getKey();
        return sql.contains(k);
    }

    private Map<Integer, String> getParameterNamesByIndex(String sql) {
        Map<Integer, String> parametersByIndex = new HashMap<>();

        String[] tokens = sql.split(" ");
        int index = 1;

        for (String token : tokens) {

            if (token.startsWith(":")) {
                String field = token.substring(1);
                parametersByIndex.put(index, field);
                index++;
            }
        }

        return parametersByIndex;
    }

    private String namedParametersToPlaceholders(String sql) {

        String[] tokens = sql.split(" ");

        StringBuilder sqlWithPlaceholders = new StringBuilder();

        for (String token : tokens) {

            if (token.startsWith(":")) {
                sqlWithPlaceholders.append(" ? ");
            } else {
                sqlWithPlaceholders.append(" ").append(token);
            }
        }

        return sqlWithPlaceholders.toString().trim();
    }

    private String normalise(String sql) {
        String normalisedSql = sql;
        normalisedSql = normalisedSql.replace(",", " , ");
        normalisedSql = normalisedSql.replace(")", " ) ");
        normalisedSql = normalisedSql.replace("(", " ( ");
        return normalisedSql;
    }
    
    

    class UpdateResponseBean {

        int returnValue;
        Long generatedKey;
    }
}
