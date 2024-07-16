import java.sql.*;


public class JDBCRunner {
        private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
        private static final String DRIVER = "org.postgresql.Driver";       // Driver name
        private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер + порт по умолчанию
        private static final String DATABASE_NAME = "orders";               // FIXME имя базы
        public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
        public static final String USER_NAME = "postgres";                  // FIXME имя пользователя
        public static final String DATABASE_PASS = "postgres";              // FIXME пароль базы данных

        public static void main(String[] args) {


        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");


        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            //TODO show all tables
            getEmployers(connection); System.out.println();
            getWorker(connection); System.out.println();
            getContracts(connection); System.out.println();

            //TODO show with param
            getEmployersNamed(connection, "David", false); System.out.println();
            getEmployerNamed(connection, "Denis", false); System.out.println();
            getWorkerNamed(connection, "Anton", false); System.out.println();
            getWorkersNamed(connection, "Artem", false);    System.out.println();
            getProfessionWorkers(connection, "Administrator", false ); System.out.println();
            getVacansyEmployers(connection, "DevOps", false);  System.out.println();


            correctWorker(connection, 5, "CEO");
            correctEmployer(connection, 3, "Product Lead");
            deleteEmployer(connection, 2);



        } catch (SQLException e) {

            if (e.getSQLState().startsWith("23")){
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }



        public static void checkDriver () {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

        public static void checkDB () {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }


        private static void getEmployers(Connection connection) throws SQLException{
        // имена столбцов
        String columnName0 = "id", columnName1 = "name", columnName2 = "vacansy";
        // значения ячеек
        int param0 = -1;
        String param1 = null, param2 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM employer;");

        while (rs.next()) {
            param2 = rs.getString(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }
    }

        static void getWorker (Connection connection) throws SQLException {

        int param0 = -1, param2 = -1;
        String param1 = null;

        Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("SELECT id, name, profession FROM worker");
            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String profession = rs.getString("profession");
                System.out.println(id + " | " + name + " | " + profession);
            }
    }

        static void getContracts (Connection connection) throws SQLException {
        String param = "";

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM contract;");

        int count = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= count; i++){
            // что в этом столбце?
            System.out.println("position - " + i +
                    ", label - " + rs.getMetaData().getColumnLabel(i) +
                    ", type - " + rs.getMetaData().getColumnType(i) +
                    ", typeName - " + rs.getMetaData().getColumnTypeName(i) +
                    ", javaClass - " + rs.getMetaData().getColumnClassName(i)
            );
        }
        System.out.println();

        while (rs.next()) {
            for (int i = 1; i <= count; i++) {
                param += rs.getString(i);
                if (i != count) param += " | ";
            }
            System.out.println(param);
            param = "";
        }
    }



        private static void getEmployersNamed(Connection connection, String name, boolean fromSQL) throws SQLException {
        if (name == null || name.isBlank()) return;

        if (fromSQL) {
            getEmployersNamed(connection, name, fromSQL);
        } else {
            long time = System.currentTimeMillis();
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(
                    "SELECT id, name, vacansy " +
                            "FROM employer");
            while (rs.next()) {
                if (rs.getString(2).contains(name)) {
                    System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getString(3));
                }
            }
            System.out.println("SELECT ALL and FIND (" + (System.currentTimeMillis() - time) + " мс.)");
        }
    }

        private static void getEmployerNamed(Connection connection, String name, boolean b) throws SQLException {
        if (name == null || name.isBlank()) return;
        name = '%' + name + '%';

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT id, name, vacansy " +
                        "FROM employer " +
                        "WHERE name LIKE ?;");
        statement.setString(1, name);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getString(3));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    public static void getProfessionWorkers(Connection connection, String profession, boolean b) throws SQLException {
            if (profession == null || profession.isBlank())return;
            profession = "%" + profession + "%";

            long time = System.currentTimeMillis();
            PreparedStatement statement = connection.prepareStatement("SELECT id, name, profession " +
                    "FROM worker "+
                    " WHERE profession LIKE ?");
            statement.setString(1, profession);
            ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getString(3));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }
    public static void getVacansyEmployers(Connection connection, String vacansy, boolean b) throws SQLException {
        if (vacansy == null || vacansy.isBlank())return;
        vacansy = "%" + vacansy + "%";

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement("SELECT id, name, vacansy " +
                "FROM employer "+
                " WHERE vacansy LIKE ?");
        statement.setString(1, vacansy);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getString(3));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }
    private static void getWorkerNamed(Connection connection, String name, boolean b) throws SQLException {
        if (name == null || name.isBlank()) return;
        name = '%' + name + '%';

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT id, name, profession " +
                        "FROM worker " +
                        "WHERE name LIKE ?;");
        statement.setString(1, name);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getString(3));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }
    private static void getWorkersNamed(Connection connection, String name, boolean fromSQL) throws SQLException {
        if (name == null || name.isBlank()) return;

        if (fromSQL) {
            getWorkersNamed(connection, name, fromSQL);
        } else {
            long time = System.currentTimeMillis();
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(
                    "SELECT id, name, profession " +
                            "FROM worker");
            while (rs.next()) {
                if (rs.getString(2).contains(name)) {
                    System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getString(3));
                }
            }
            System.out.println("SELECT ALL and FIND (" + (System.currentTimeMillis() - time) + " мс.)");
        }
    }




    // add
    private static void addWorker(Connection connection, String name, String profession) throws SQLException {
        if (name == null || name.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("INSERT INTO worker( name, profession) VALUES (?, ?) returning id", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, name);
        statement.setString(2, profession);


        int count = statement.executeUpdate();
        long time = System.currentTimeMillis();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()){
            System.out.println("INSERTed "+count+" profession" + (System.currentTimeMillis() - time) + "ms");
            getWorker(connection);
        }
    }
    private static void addEmployer(Connection connection, String name, String vacansy) throws SQLException {
        if (name == null || name.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("INSERT INTO employer( name, vacansy) VALUES (?, ?) returning id", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, name);
        statement.setString(2, vacansy);


        int count = statement.executeUpdate();
        long time = System.currentTimeMillis();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()){
            System.out.println("INSERTed "+count+" vacansy" + (System.currentTimeMillis() - time) + "ms");
            getEmployers(connection);
        }
    }

    private static void correctWorker(Connection connection, int id, String profession) throws SQLException {

        if (profession == null || profession.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE worker SET profession=? WHERE id=?;");

        statement.setString(1, profession);
        statement.setInt(2, id);
        int count = statement.executeUpdate();


        System.out.println("UPDATEd " + count + " worker");

    }
    private static void deleteEmployer(Connection connection, int id) throws SQLException {

        if (id <= 0)return;
        PreparedStatement statement = connection.prepareStatement("DELETE FROM employer WHERE id =?");
        statement.setInt(1, id);
        int count = statement.executeUpdate();
        System.out.println("Deleted " + count + " employer");
        getWorker(connection);
    }
private static void deleteWorker(Connection connection, int id) throws SQLException {

    if (id <= 0)return;
    PreparedStatement statement = connection.prepareStatement("DELETE FROM worker WHERE id =?");
    statement.setInt(1, id);
    int count = statement.executeUpdate();
    System.out.println("Deleted " + count + " worker");
    getWorker(connection);
}
    private static void correctEmployer (Connection connection,  int id, String vacansy) throws SQLException {
        if (vacansy == null || vacansy.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE employer SET vacansy=? WHERE id=?;");
        statement.setString(1, vacansy);
        statement.setInt(2, id);


        int count = statement.executeUpdate();

        System.out.println("UPDATEd " + count + " employer");
    }
}

