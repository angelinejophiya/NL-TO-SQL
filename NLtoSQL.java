import java.io.IOException;
import java.util.Scanner;
import okhttp3.*;
import org.json.JSONObject;
import java.sql.*;

public class NLtoSQL {
    private static final String DB_URL = "jdbc:mysql://localhost:3306/companydb";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "Tharun@01";

    public static void main(String[] args) {
        try (java.sql.Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            String currentDB = conn.getCatalog(); 
            System.out.println("Connected to database/schema: " + currentDB);

            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tables = metaData.getTables(currentDB, null, "%", new String[]{"TABLE"});
            System.out.println("Available tables in " + currentDB + ":");
            boolean hasTables = false;
            int tableCount = 0;
            while (tables.next() && tableCount < 10) { 
                hasTables = true;
                System.out.println("   - " + tables.getString("TABLE_NAME"));
                tableCount++;
            }
            if (!hasTables) {
                System.out.println("No tables found in this schema!");
            }
        } catch (SQLException e) {
            System.err.println("Error connecting to database: " + e.getMessage());
            return;
        }

        Scanner scanner = new Scanner(System.in);
        System.out.print("\nEnter your natural language query: ");
        String userQuery = scanner.nextLine();

        String sqlQuery = getSQLFromOllama(userQuery);
        if (sqlQuery == null || sqlQuery.isEmpty()) {
            System.out.println("No valid SQL query generated.");
            return;
        }
        System.out.println("\nGenerated SQL Query:\n" + sqlQuery);
        runSQL(sqlQuery);

        scanner.close();
    }

    private static String getSQLFromOllama(String userQuery) {
        try (java.sql.Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            DatabaseMetaData metaData = conn.getMetaData();
            StringBuilder schemaBuilder = new StringBuilder("Tables and Columns (limited view):\n");

            ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});
            int tableCount = 0;
            while (tables.next() && tableCount < 10) { 
                String tableName = tables.getString("TABLE_NAME");
                schemaBuilder.append("  - ").append(tableName).append(" (");

                ResultSet columns = metaData.getColumns(null, null, tableName, "%");
                int columnCount = 0;
                boolean first = true;
                while (columns.next() && columnCount < 10) { 
                    if (!first) schemaBuilder.append(", ");
                    schemaBuilder.append(columns.getString("COLUMN_NAME"));
                    first = false;
                    columnCount++;
                }
                schemaBuilder.append(")\n");
                tableCount++;
            }

            JSONObject body = new JSONObject();
            body.put("model", "mistral");
            body.put("prompt", "You are a SQL expert. The database schema is as follows:\n"
                    + schemaBuilder.toString()
                    + "\nConvert this request into a single valid SQL query. "
                    + "Output only the SQL code, no explanations:\n" + userQuery);

            OkHttpClient client = new OkHttpClient.Builder()
                    .connectTimeout(15, java.util.concurrent.TimeUnit.SECONDS)
                    .readTimeout(15, java.util.concurrent.TimeUnit.SECONDS)
                    .writeTimeout(15, java.util.concurrent.TimeUnit.SECONDS)
                    .build();

            RequestBody requestBody = RequestBody.create(
                    MediaType.parse("application/json"),
                    body.toString()
            );

            Request request = new Request.Builder()
                    .url("http://localhost:11434/api/generate") 
                    .post(requestBody)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful())
                    throw new IOException("Unexpected code " + response);

                String responseBody = response.body().string();
                StringBuilder finalSQL = new StringBuilder();
                String[] parts = responseBody.split("\n");
                for (String part : parts) {
                    if (part.trim().isEmpty()) continue;
                    JSONObject jsonResponse = new JSONObject(part);
                    if (jsonResponse.has("response")) {
                        finalSQL.append(jsonResponse.getString("response"));
                    }
                }

                String rawSqlQuery = finalSQL.toString().trim()
                        .replaceAll("```sql", "")
                        .replaceAll("```", "")
                        .replaceAll("(?i)sql query:?", "")
                        .trim();

                if (rawSqlQuery.toLowerCase().startsWith("sql")) {
                    return rawSqlQuery.substring(3).trim();
                } else {
                    return rawSqlQuery;
                }
            }

        } catch (java.net.SocketTimeoutException e) {
            System.err.println("Timeout: Ollama server did not respond in time.");
        } catch (IOException e) {
            System.err.println("Network error while contacting Ollama: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error generating SQL: " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    private static void runSQL(String sqlQuery) {
        try (java.sql.Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement()) {

            boolean hasResultSet = stmt.execute(sqlQuery);

            if (hasResultSet) {
                try (ResultSet rs = stmt.getResultSet()) {
                    int columnCount = rs.getMetaData().getColumnCount();
                    System.out.println("\n=== Query Results ===");
                    while (rs.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            System.out.print(rs.getString(i) + "\t");
                        }
                        System.out.println();
                    }
                }
            } else {
                int updateCount = stmt.getUpdateCount();
                System.out.println("\nQuery executed successfully. Rows affected: " + updateCount);
            }

        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}