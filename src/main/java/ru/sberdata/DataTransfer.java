package ru.sberdata;

import java.sql.*;

public class DataTransfer {

    public static void main(String[] args) throws SQLException {

        Connection con = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/freepodb",
                "freepo",
                "123");


        copyData(con, con, "SELECT intt, dat FROM public.test;", "INSERT INTO public.test(intt, dat)");
        con.close();

    }

    public static void copyData(Connection sourceConnection, Connection targetConnection, String selectQuery, String insertQuery) throws SQLException {

        Statement sourceStatement = sourceConnection.createStatement();
        ResultSet rs = sourceStatement.executeQuery(selectQuery);

        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        String parametrString = " VALUES (";
        for (int i = 0; i < columnCount; i++) {
            parametrString += "?";
            parametrString += i < columnCount - 1 ? ", " : ")";
        }

        String dinamicInsertQuery = insertQuery + parametrString;
        PreparedStatement targetStatement = targetConnection.prepareStatement(dinamicInsertQuery);

        while (rs.next()) {

            for (int i = 1; i <= columnCount; i++) {

                Object sourceFeild = rs.getObject(i);
                targetStatement.setObject(i, sourceFeild);

            }
            targetStatement.addBatch();

        }

        sourceStatement.close();
        targetStatement.executeBatch();
        targetStatement.close();

    }

}


/*
Написать процедуру
На вход получает
Connection1 соединение с одной БД
Connection2 соединение с другой БД
Select – строка по выборке в первой БД  например
Пример 1
select name,address from my_scheme.clients
Insert для второй БД например insert into second_scheme.table2(name2,add2)

Второй пример
Select:  select timestamp_fld1, date_fld , float_number_field3 from source.table1
Insert:: insert into target.table2(timestamp_field1,date_field2,number_fld )

Написать один java код который перенесет все строки выборки из одной БД в другую и для первого и второго примера
(то есть типы данных в сорсе и таргете совпадают, но типы данных проихвольны)
 */
