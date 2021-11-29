package com.epam.rd.autocode;

import com.epam.rd.autocode.domain.Employee;
import com.epam.rd.autocode.domain.FullName;
import com.epam.rd.autocode.domain.Position;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

public class EmployeeSetMapper implements SetMapper<Set<Employee>> {
    private static final Logger log = LogManager.getLogger();
    private static final byte COLUMN_ID = 1;
    private static final byte COLUMN_FIRST_NAME = 2;
    private static final byte COLUMN_LAST_NAME = 3;
    private static final byte COLUMN_MIDDLE_NAME = 4;
    private static final byte COLUMN_POSITION = 5;
    private static final byte COLUMN_MANAGER = 6;
    private static final byte COLUMN_HIREDATE = 7;
    private static final byte COLUMN_SALARY = 8;

    @Override
    public Set<Employee> mapSet(ResultSet resultSet) {
        Set<Employee> employeeSet = new HashSet<>();
        try {
            while (resultSet.next()) {
                BigInteger id = new BigInteger(String.valueOf(resultSet.getLong(COLUMN_ID)));
                FullName fullName = new FullName(
                    resultSet.getString(COLUMN_FIRST_NAME),
                    resultSet.getString(COLUMN_LAST_NAME),
                    resultSet.getString(COLUMN_MIDDLE_NAME));
                Position position = Position.valueOf(resultSet.getString(COLUMN_POSITION));
                LocalDate hired = resultSet.getDate(COLUMN_HIREDATE).toLocalDate();
                BigDecimal salary = resultSet.getBigDecimal(COLUMN_SALARY);
                BigInteger managerId = new BigInteger(String.valueOf(resultSet.getLong(COLUMN_MANAGER)));
                int currentRow = resultSet.getRow();
                employeeSet.add(new Employee(id, fullName, position, hired, salary,
                    createManager(resultSet, managerId)));

                resultSet.absolute(currentRow);
            }
        } catch (SQLException e) {
            log.error(e);
        }
        return employeeSet;
    }

    private Employee createManager(ResultSet resultSet, BigInteger currentManagerId) throws SQLException {
        Employee newEmployee = null;
        resultSet.first();
        while (resultSet.next()) {
            BigInteger id = new BigInteger(String.valueOf(resultSet.getLong(COLUMN_ID)));
            if (currentManagerId.equals(id)) {
                FullName fullName = new FullName(
                    resultSet.getString(COLUMN_FIRST_NAME),
                    resultSet.getString(COLUMN_LAST_NAME),
                    resultSet.getString(COLUMN_MIDDLE_NAME));
                Position position = Position.valueOf(resultSet.getString(COLUMN_POSITION));
                LocalDate hired = resultSet.getDate(COLUMN_HIREDATE).toLocalDate();
                BigDecimal salary = resultSet.getBigDecimal(COLUMN_SALARY);
                BigInteger newManagerId = new BigInteger(String.valueOf(resultSet.getLong(COLUMN_MANAGER)));

                newEmployee = new Employee(id, fullName, position, hired, salary,
                    createManager(resultSet, newManagerId));
            }
        }
        return newEmployee;
    }
}
