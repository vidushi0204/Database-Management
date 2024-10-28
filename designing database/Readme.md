# Database Design

## Broad Objective:
The goal of this assignment is to design a robust relational database system for managing **course administration** using PostgreSQL and PL/pgSQL. You will implement complex constraints, views, triggers, and functions to ensure the integrity and correct functionality of the database.

---

## Key Components:
1. **Tables**:
   - **Student, Courses, Professors, Departments**: Core tables to store data about students, courses, professors, and departments.
   - **Student Courses, Course Offers**: Track student enrollments and course offerings across semesters.
   
2. **Constraints**:
   - **Not NULL, Unique, and Check Constraints**: Ensure data integrity (e.g., valid email formats, course capacity limits).
   - **Advanced Constraints**: Include department change restrictions, maximum course load, credit limits, and valid student ID format.

3. **Triggers & Views**:
   - **Triggers**: Automate actions such as updating student credits, validating email IDs, restricting course enrollments, and handling department changes.
   - **Views**:
     - **`course_eval`**: Real-time grade evaluation for courses.
     - **`student_semester_summary`**: Provides GPA and credit information for each student per semester.
