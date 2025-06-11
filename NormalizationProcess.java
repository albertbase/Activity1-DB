import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class NormalizationProcess {

    static final String URL = "jdbc:mysql://localhost:3307/school_data";
    static final String USER = "root"; 
    static final String PASSWORD = "akogwapo"; 

    public static void main(String[] args) {
        try (Scanner scanner = new Scanner(System.in);
             Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            // To store the inputted Student IDs
            List<Integer> studentIds = new ArrayList<>(); 
            // To store raw data entries
            List<RawDataEntry> rawDataEntries = new ArrayList<>(); 

            // Input raw data entries
            for(int data = 1 ; data <= 2 ; data++){
                System.out.println("Enter Data " );

                System.out.print("Enter Student ID: ");
                String idInput = scanner.nextLine();
                String[] idArray = idInput.split(","); 

                for (String id : idArray) {
                    studentIds.add(Integer.parseInt(id.trim())); 
                }

                System.out.print("Enter Student Name: ");
                String names = scanner.nextLine();

                System.out.print("Enter Courses: ");
                String courses = scanner.nextLine();

                System.out.print("Enter Instructors: ");
                String instructors = scanner.nextLine();

                // Store raw data entry for display later
                rawDataEntries.add(new RawDataEntry(idInput, names, courses, instructors));

                // Insert raw data for each Student ID
                for (int studentId : studentIds) {
                    // Insert raw data
                    String insertRaw = "INSERT INTO Raw_Students (Student_ID, Name, Courses, Instructors) VALUES (?, ?, ?, ?)";
                    try (PreparedStatement pstmt = conn.prepareStatement(insertRaw)) {
                        pstmt.setInt(1, studentId);
                        pstmt.setString(2, names);
                        pstmt.setString(3, courses);
                        pstmt.setString(4, instructors);
                        pstmt.executeUpdate();
                        System.out.println("Raw data inserted successfully for Student ID: " + studentId);
                    }
                }
                studentIds.clear(); // Clear the list for the next entry
            }
            // Display raw data entries in table format
            displayRawDataEntries(rawDataEntries);

            // Normalize the data
            normalizeData(conn, rawDataEntries);

            // Display the results
            displayNormalizationSteps(conn, rawDataEntries); // Pass rawDataEntries to filter results

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Method to display raw data entries in a table format
    public static void displayRawDataEntries(List<RawDataEntry> rawDataEntries) {
        System.out.println("\n🔹 Raw Data Entries:");
        System.out.println("+------------+---------------------+---------------------+---------------------+");
        System.out.println("| Student ID | Names               | Courses             | Instructors         |");
        System.out.println("+------------+---------------------+---------------------+---------------------+");
        
        for (RawDataEntry entry : rawDataEntries) {
            System.out.printf("| %-10s | %-19s | %-19s | %-19s |\n",
                    entry.studentIds,
                    entry.names,
                    entry.courses,
                    entry.instructors);
        }
        System.out.println("+------------+---------------------+---------------------+---------------------+");
    }

    // Class to hold raw data entries
    static class RawDataEntry {
        String studentIds;
        String names;
        String courses;
        String instructors;

        RawDataEntry(String studentIds, String names, String courses, String instructors) {
            this.studentIds = studentIds;
            this.names = names;
            this.courses = courses;
            this.instructors = instructors;
        }
    }

    public static void normalizeData(Connection conn, List<RawDataEntry> rawDataEntries) throws SQLException {
        Statement stmt = conn.createStatement();
    
        // Drop old tables if they exist
        stmt.execute("DROP TABLE IF EXISTS FirstNF, Students, Enrollment, Courses, Instructors");
    
        // 1NF: Split multi-valued attributes into separate rows
        stmt.execute("CREATE TABLE FirstNF (Student_ID INT, Name VARCHAR(100), Course VARCHAR(100), Instructor VARCHAR(100))");
    
        for (RawDataEntry entry : rawDataEntries) {
            String[] nameList = entry.names.split(","); // splitting names by comma
            String[] courseList = entry.courses.split(","); // splitting courses
            String[] instructorList = entry.instructors.split(","); // splitting instructors
            
            // Inserting data to FirstNF
            for (int i = 0; i < courseList.length; i++) {
                String course = courseList[i].trim();
                String instructor = (i < instructorList.length) ? instructorList[i].trim() : "Unknown";
    
                for (String name : nameList) {
                    try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO FirstNF (Student_ID, Name, Course, Instructor) VALUES (?, ?, ?, ?)")) {
                        pstmt.setInt(1, Integer.parseInt(entry.studentIds.split(",")[0].trim())); // Assuming single student ID for the entry
                        pstmt.setString(2, name.trim()); // Trim each name
                        pstmt.setString(3, course);
                        pstmt.setString(4, instructor);
                        pstmt.executeUpdate();
                    }
                }
            }
        }
    
        // 2NF: Separate student info and course enrollment
        stmt.execute("CREATE TABLE Students (Student_ID INT PRIMARY KEY, Name VARCHAR(100))");
        stmt.execute("CREATE TABLE Instructors (Instructor_ID INT AUTO_INCREMENT PRIMARY KEY, Instructor_Name VARCHAR(100) NOT NULL)");
        stmt.execute("CREATE TABLE Courses (Course_ID INT AUTO_INCREMENT PRIMARY KEY, Course VARCHAR(100) UNIQUE, Instructor_ID INT, FOREIGN KEY (Instructor_ID) REFERENCES Instructors(Instructor_ID))");
        stmt.execute("CREATE TABLE Enrollment (Enrollment_ID INT AUTO_INCREMENT PRIMARY KEY, Course VARCHAR(100), FOREIGN KEY (Course) REFERENCES Courses(Course))");
    
        // Insert unique students into Students table
        String insertStudentSQL = "INSERT IGNORE INTO Students (Student_ID, Name) VALUES (?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(insertStudentSQL)) {
            for (RawDataEntry entry : rawDataEntries) {
                String[] nameList = entry.names.split(","); // splitting names by comma
                for (String name : nameList) {
                    pstmt.setInt(1, Integer.parseInt(entry.studentIds.split(",")[0].trim())); // Assuming single student ID for the entry
                    pstmt.setString(2, name.trim());
                    pstmt.executeUpdate();
                }
            }
        }
    
        // Insert unique instructors into Instructors table
        String insertInstructorSQL = "INSERT IGNORE INTO Instructors (Instructor_Name) VALUES (?)";
        try (PreparedStatement pstmt = conn.prepareStatement(insertInstructorSQL)) {
            for (RawDataEntry entry : rawDataEntries) {
                String[] instructorList = entry.instructors.split(","); // splitting instructors
                for (String instructor : instructorList) {
                    pstmt.setString(1, instructor.trim());
                    pstmt.executeUpdate();
                }
            }
        }
    
        // Insert unique enrollments into Enrollment table
        String insertEnrollmentSQL = "INSERT IGNORE INTO Enrollment (Course) VALUES (?)";
        try (PreparedStatement pstmt = conn.prepareStatement(insertEnrollmentSQL)) {
            for (RawDataEntry entry : rawDataEntries) {
                String[] courseList = entry.courses.split(","); // splitting courses
                for (String course : courseList) {
                    pstmt.setString(1, course.trim());
                    pstmt.executeUpdate();
                    System.out.println("Inserted into Enrollment: " + course.trim()); // Debugging statement
                }
            }
        }
    
        // Insert unique courses into Courses table
        String insertCourseSQL = "INSERT IGNORE INTO Courses (Course) VALUES (?)";
        try (PreparedStatement pstmt = conn.prepareStatement(insertCourseSQL)) {
            for (RawDataEntry entry : rawDataEntries) {
                String[] courseList = entry.courses.split(","); // splitting courses
                for (String course : courseList) {
                    pstmt.setString(1, course.trim());
                    pstmt.executeUpdate();
                    System.out.println("Inserted into Courses: " + course.trim()); // Debugging statement
                }
            }
        }
    
        System.out.println("Data has been normalized to 1NF, 2NF, and 3NF!");
    }

    public static void displayNormalizationSteps(Connection conn, List<RawDataEntry> rawDataEntries) throws SQLException {
        Statement stmt = conn.createStatement();

        // Display 1NF
        System.out.println("\n🔹 1NF (Eliminated Multi-Valued Attributes):");
        System.out.println("+------------+--------+----------+------------+");
        System.out.println("| Student_ID | Name   | Course   | Instructor |");
        System.out.println("+------------+--------+----------+------------+");

        for (RawDataEntry entry : rawDataEntries) {
            String[] nameList = entry.names.split(","); // splitting names by comma
            String[] courseList = entry.courses.split(","); // splitting courses
            String[] instructorList = entry.instructors.split(","); // splitting instructors
            
            for (int i = 0; i < courseList.length; i++) {
                String course = courseList[i].trim();
                String instructor = (i < instructorList.length) ? instructorList[i].trim() : "Unknown";
                for (String name : nameList) {
                    System.out.printf("| %-10s | %-6s | %-8s | %-10s |\n",
                            entry.studentIds.split(",")[0].trim(), // Assuming single student ID for the entry
                            name.trim(),
                            course,
                            instructor);
                }
            }
        }
        System.out.println("+------------+--------+----------+------------+");

        // Display 2NF
        System.out.println("\n🔹 2NF (Eliminated Partial Dependency):");

        System.out.println("\nStudents Table:");
        System.out.println("+------------+-------------+");
        System.out.println("| Student_ID | Name        |");
        System.out.println("+------------+-------------+");
        
        for (RawDataEntry entry : rawDataEntries) {
            String[] nameList = entry.names.split(","); // splitting names by comma
            for (String name : nameList) {
                System.out.printf("| %-10s | %-11s |\n", entry.studentIds.split(",")[0].trim(), name.trim());
            }
        }
        System.out.println("+------------+-------------+");

        System.out.println("\nInstructors Table:");
        System.out.println("+--------------+-----------------+");
        System.out.println("| Instructor_ID| Instructor_Name |");
        System.out.println("+--------------+-----------------+");
        
        ResultSet rsInstructors = stmt.executeQuery("SELECT * FROM Instructors");
        while (rsInstructors.next()) {
            System.out.printf("| %-12d | %-15s |\n", rsInstructors.getInt("Instructor_ID"), rsInstructors.getString("Instructor_Name"));
        }
        System.out.println("+--------------+-----------------+");

        // Display Enrollment Table
        System.out.println("\nEnrollment Table:");
        System.out.println("+----------------+----------+");
        System.out.println("| Enrollment_ID  | Course   |");
        System.out.println("+----------------+----------+");
        
        ResultSet rsEnrollment = stmt.executeQuery("SELECT * FROM Enrollment");
        while (rsEnrollment.next()) {
            System.out.printf("| %-14d | %-8s |\n", rsEnrollment.getInt("Enrollment_ID"), rsEnrollment.getString("Course"));
        }
        System.out.println("+----------------+----------+");

        // Display 3NF
        System.out.println("\n🔹 3NF (Eliminated Transitive Dependency):");

        System.out.println("\nCourses Table:");
        System.out.println("+----------+-------------+--------------+");
        System.out.println("| Course_ID| Course      | Instructor_ID|");
        System.out.println("+----------+-------------+--------------+");
        
        ResultSet rsCourses = stmt.executeQuery("SELECT * FROM Courses");
        while (rsCourses.next()) {
            System.out.printf("| %-8d | %-11s | %-12d |\n", rsCourses.getInt("Course_ID"), rsCourses.getString("Course"), rsCourses.getInt("Instructor_ID"));
        }
        System.out.println("+----------+-------------+--------------+");
    }
}