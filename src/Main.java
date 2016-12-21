import java.sql.*;

/**
 * Created by andrey on 21.12.16.
 */
public class Main {
    static String[][] employee = {{"1","Sasha"},
                                  {"2","Yasha"}};
    static String[][] salary = {{"1","2016-10-15","5000","1"},
                                {"2","2016-03-11","1100","2"},
                                {"3","2016-10-28","3200","1"},
                                {"4","2016-01-25","2500","1"}};
    public static void main(String[] args) throws SQLException{
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test")){
            createTable(connection);
            insertEmployee(connection);
            insertSalary(connection);
            selectSalary(connection);
        }
    }
    private static void createTable(Connection connection)throws SQLException{
        try(Statement st = connection.createStatement()){
            st.execute("create table employee ( id integer primary key auto_increment, name varchar(100));");
            st.execute("create table salary ( id integer primary key auto_increment, date varchar(100), value integer , emp_id integer);");
        }
    }

    private static void insertEmployee(Connection connection)throws SQLException{
        try(PreparedStatement pst = connection.prepareStatement("insert into employee(id,name) values(?,?)");){
            for(int i = 0; i<employee.length;i++){
                pst.setInt(1,Integer.parseInt(employee[i][0]));
                pst.setString(2, employee[i][1]);
                pst.addBatch();
            }
            pst.executeBatch();
        }
    }

    private static void selectSalary(Connection connection)throws SQLException{
        try (PreparedStatement ps = connection.prepareStatement( "select employee.id,employee.name,sum(salary.value) from employee left join salary on employee.id = salary.emp_id group by employee.id;" )){
            try (ResultSet rs = ps.executeQuery()) {
                while ( rs.next() ) {
                    int numColumns = rs.getMetaData().getColumnCount();
                    for ( int i = 1 ; i <= numColumns ; i++ ) {
                        System.out.print( rs.getObject(i)+"   " );
                    }
                    System.out.println();
                }
            }
        }
    }

    private static void insertSalary(Connection connection)throws SQLException{
        try(PreparedStatement pst = connection.prepareStatement("insert into salary(id,date,value,emp_id) values(?,?,?,?)");) {
            for (int i = 0; i < salary.length; i++) {
                pst.setInt(3, Integer.parseInt(salary[i][2]));
                pst.setInt(4, Integer.parseInt(salary[i][3]));
                pst.addBatch();
            }
            pst.executeBatch();
        }
    }
}
