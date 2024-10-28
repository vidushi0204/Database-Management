
-- Create department table
CREATE TABLE department (
    dept_id CHAR(3) PRIMARY KEY,
    dept_name VARCHAR(40) UNIQUE NOT NULL
    
);

-- Create valid_entry table
CREATE TABLE valid_entry (
    dept_id CHAR(3),
    entry_year INTEGER NOT NULL,
    seq_number INTEGER NOT NULL,
    FOREIGN KEY (dept_id) REFERENCES department(dept_id) ON UPDATE CASCADE
);

-- Create professor table
CREATE TABLE professor (
    professor_id VARCHAR(10) PRIMARY KEY,
    professor_first_name VARCHAR(40) NOT NULL,
    professor_last_name VARCHAR(40) NOT NULL,
    office_number VARCHAR(20),
    contact_number CHAR(10) NOT NULL,
    start_year INTEGER CHECK(start_year<=resign_year),
    resign_year INTEGER,
    dept_id CHAR(3),
    FOREIGN KEY (dept_id) REFERENCES department(dept_id) ON UPDATE CASCADE
);

-- Create courses table
CREATE TABLE courses (
    course_id CHAR(6) PRIMARY KEY NOT NULL ,
    course_name VARCHAR(20) UNIQUE NOT NULL,
    course_desc TEXT,
    credits NUMERIC NOT NULL CHECK (credits > 0),
    dept_id CHAR(3),
    FOREIGN KEY (dept_id) REFERENCES department(dept_id) ON UPDATE CASCADE,
    CONSTRAINT check_course_id CHECK (dept_id=SUBSTRING(course_id, 1, 3) and cast(SUBSTRING(course_id FROM 4 FOR 3) as text) similar to cast('[0-9][0-9][0-9]' as text))
);

-- Create student table
CREATE TABLE student (
    first_name VARCHAR(40) NOT NULL,
    last_name VARCHAR(40),
    student_id CHAR(11) PRIMARY KEY NOT NULL,
    address VARCHAR(100),
    contact_number CHAR(10) UNIQUE NOT NULL,
    email_id VARCHAR(50) UNIQUE,
    tot_credits INTEGER NOT NULL CHECK (tot_credits >= 0),
    dept_id CHAR(3),
    FOREIGN KEY (dept_id) REFERENCES department(dept_id) ON UPDATE CASCADE
);

-- Create course_offers table
CREATE TABLE course_offers (
    course_id CHAR(6),
    session VARCHAR(9),
    semester INTEGER NOT NULL CHECK(semester IN (1,2)),
    professor_id VARCHAR(10),
    capacity INTEGER,
    enrollments INTEGER,
    FOREIGN KEY (course_id) REFERENCES courses(course_id) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (professor_id) REFERENCES professor(professor_id) ON UPDATE CASCADE,
    PRIMARY KEY(course_id,session,semester) 
);

-- Create student_courses table
CREATE TABLE student_courses (
    student_id CHAR(11),
    course_id CHAR(6),
    session VARCHAR(9),
    semester INTEGER CHECK(semester IN (1,2)),
    grade NUMERIC NOT NULL CHECK(grade>=0 AND grade<=10),
    FOREIGN KEY(student_id) REFERENCES student(student_id) ON UPDATE CASCADE,
    FOREIGN KEY(course_id,session,semester) REFERENCES course_offers(course_id,session,semester) ON UPDATE CASCADE ON DELETE CASCADE
);

create or replace function validate_student_id()
returns trigger as $$
begin
    declare
        seq_number_v int;
    begin
        seq_number_v := cast(substring(new.student_id from 8 for 3) as integer);
        if seq_number_v < 1 then
            raise exception 'invalid';
        end if;
        if length(new.student_id) <> 10 then
            raise exception 'invalid';
        end if;
        if not exists (select 1 from valid_entry 
        where entry_year = cast(substring(new.student_id from 1 for 4) as integer) 
        and dept_id = substring(new.student_id from 5 for 3) and seq_number=seq_number_v) then
            raise exception 'invalid';
        end if;
        return new;
    end;

end;
$$ language plpgsql;

create trigger validate_student_id
before insert on student
for each row
execute function validate_student_id();


create or replace function update_seq_number()
returns trigger as $$
begin
    update valid_entry
    set seq_number = seq_number + 1
    where entry_year = cast(substring(new.student_id from 1 for 4) as integer)
    and dept_id = substring(new.student_id from 5 for 3);
    
    return null;
end;
$$ language plpgsql;

create trigger update_seq_number
after insert on student
for each row
execute function update_seq_number();


create or replace function validate_email()
returns trigger as $$
begin
    declare
        dept_id_v char(3);
    begin
        dept_id_v := substring(new.student_id from 5 for 3);
        if new.email_id not like new.student_id || '@' || dept_id_v || '.iitd.ac.in' then
            raise exception 'invalid';
        end if;

        return new;
    end;
end;
$$ language plpgsql;

create trigger validate_email
before insert on student
for each row
execute function validate_email();

create table student_dept_change (
    old_student_id char(11),
    old_dept_id char(3) ,
    new_dept_id char(3) ,
    new_student_id char(11) primary key,
    foreign key (old_dept_id) references department(dept_id),
    foreign key (new_dept_id) references department(dept_id) 
);

create or replace function log_student_dept_change()
returns trigger as $$
begin
    declare 
        avg_grade numeric;
        new_seq_num integer; 
        entry_year_v integer;
        new_student_id_v CHAR(11);
        new_email_id VARCHAR(50);
    begin
        if new.dept_id != old.dept_id then 
            entry_year_v:= cast(substring(old.student_id from 1 for 4) as integer);
            if exists (select 1 from student_dept_change where new_student_id = old.student_id) then
                raise exception 'Department can be changed only once';
            end if;

            if entry_year_v < 2022 then
                raise exception 'Entry year must be >= 2022';
            end if;

            select avg(grade) into avg_grade from student_courses where student_id = old.student_id;
            if avg_grade is null or avg_grade <= 8.5 then
                raise exception 'Low Grade';
            end if;


            select seq_number into new_seq_num from valid_entry where entry_year=entry_year_v and dept_id=new.dept_id;
            new_student_id_v:=cast(entry_year_v as varchar)||new.dept_id||LPAD(new_seq_num::varchar, 3, '0');
            new_email_id:=new_student_id_v||'@'||new.dept_id||'.iitd.ac.in';

            update valid_entry
            set seq_number = new_seq_num+1
            WHERE entry_year = entry_year_v AND dept_id = new.dept_id;
            

            insert into student_dept_change (old_student_id, old_dept_id, new_dept_id, new_student_id)
            values (old.student_id, old.dept_id, new.dept_id, new_student_id_v);
            new.student_id=new_student_id_v;
            new.email_id:=new_email_id;

        end if;
        return new;
    end;  
end;
$$ language plpgsql;

create trigger student_dept_change_trigger
before update on student
for each row
execute function log_student_dept_change();



create or replace view course_eval as
select 
    student_courses.course_id,
    student_courses.session,
    student_courses.semester,
    count(student_courses.student_id) as number_of_students,
    avg(student_courses.grade) as average_grade,
    max(student_courses.grade) as max_grade,
    min(student_courses.grade) as min_grade
from 
    student_courses
group by 
    student_courses.course_id, student_courses.session, student_courses.semester;


create or replace function update_tot_credits()
returns trigger as $$
begin
    declare 
        course_credits integer;
    begin
        select credits into course_credits from courses where course_id = new.course_id;
        
        update student
        set tot_credits = tot_credits + course_credits
        where student_id = new.student_id;
        return new;
    end;
end;
$$ language plpgsql;

create trigger update_tot_credits
after insert on student_courses
for each row
execute function update_tot_credits();


CREATE OR REPLACE FUNCTION check_course_limit()
RETURNS TRIGGER AS $$
DECLARE
    total_credits INTEGER;
    current_courses INTEGER;
BEGIN
    SELECT COUNT(*) INTO current_courses
    FROM student_courses 
    WHERE student_id = NEW.student_id AND session = NEW.session AND semester = NEW.semester;
    
    IF current_courses > 5 THEN
        RAISE EXCEPTION 'invalid';
    END IF;
    
    SELECT tot_credits INTO total_credits FROM student WHERE student_id = NEW.student_id;
    IF total_credits + (SELECT credits FROM courses WHERE course_id = NEW.course_id) > 60 THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_course_limit
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION check_course_limit();


CREATE OR REPLACE FUNCTION check_course_credit()
RETURNS TRIGGER AS $$
DECLARE
    student_entry_year INT;
BEGIN
    student_entry_year := CAST(SUBSTRING(NEW.student_id FROM 1 FOR 4) AS INT);
    
    IF (SELECT credits FROM courses WHERE course_id = NEW.course_id) = 5 THEN
        IF student_entry_year != CAST(SUBSTRING(NEW.session FROM 1 FOR 4) AS INT) THEN
            RAISE EXCEPTION 'invalid';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_course_credit
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION check_course_credit();


CREATE MATERIALIZED VIEW student_semester_summary AS
SELECT 
    sc.student_id,
    sc.session,
    sc.semester,
    SUM(CASE WHEN sc.grade >= 5.0 THEN sc.grade * c.credits END) / SUM(CASE WHEN sc.grade >= 5.0 THEN c.credits END) AS sgpa,
    SUM(CASE WHEN sc.grade >= 5.0 THEN c.credits END) AS credits
FROM 
    student_courses sc
JOIN 
    courses c ON sc.course_id = c.course_id
GROUP BY 
    sc.student_id, sc.session, sc.semester;

CREATE OR REPLACE FUNCTION check_per_sem_limit() RETURNS TRIGGER AS $$
BEGIN
    DECLARE
        tot_creds INTEGER;
    BEGIN
        SELECT SUM(c.credits) INTO tot_creds FROM student_courses sc JOIN courses c ON sc.course_id = c.course_id WHERE sc.student_id = NEW.student_id AND sc.session = NEW.session AND sc.semester = NEW.semester;
        IF tot_creds+ (SELECT credits FROM courses WHERE course_id = NEW.course_id) > 26 THEN
            RAISE EXCEPTION 'credit limit exceeded';
        END IF;
        RETURN NEW;
    END;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER check_per_sem_limit
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION check_per_sem_limit();

CREATE OR REPLACE FUNCTION update_student_summary() RETURNS TRIGGER AS $$
BEGIN
    if((TG_OP='UPDATE' and old.grade!=new.grade) or TG_OP='INSERT' or TG_OP='DELETE') THEN
        REFRESH MATERIALIZED VIEW student_semester_summary;
    END IF;
    
    if(TG_OP='DELETE') THEN
        UPDATE student
        SET tot_credits = (SELECT COALESCE(SUM(c.credits), 0) FROM student_courses sc JOIN courses c ON sc.course_id = c.course_id WHERE sc.student_id = OLD.student_id)
        WHERE student_id = old.student_id;

        update course_offers
        set enrollments=enrollments-1
        where course_id=old.course_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_student_summary
AFTER INSERT OR DELETE OR UPDATE ON student_courses
FOR EACH ROW
EXECUTE FUNCTION update_student_summary();


CREATE OR REPLACE FUNCTION check_course_capacity()
RETURNS TRIGGER AS $$
BEGIN
    IF (SELECT enrollments FROM course_offers WHERE course_id = NEW.course_id) >= (SELECT capacity FROM course_offers WHERE course_id = NEW.course_id) THEN
        RAISE EXCEPTION 'course is full';
    ELSE
        UPDATE course_offers
        SET enrollments = enrollments + 1
        WHERE course_id = NEW.course_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_course_capacity_trigger
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION check_course_capacity();


CREATE OR REPLACE FUNCTION remove_course_from_students() RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM student_courses 
    WHERE course_id = OLD.course_id 
    AND session = OLD.session 
    AND semester = OLD.semester;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER remove_course_trigger
AFTER DELETE ON course_offers
FOR EACH ROW
EXECUTE FUNCTION remove_course_from_students();



CREATE OR REPLACE FUNCTION ensure_course_and_professor_existence() RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM courses WHERE course_id = NEW.course_id) THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM professor WHERE professor_id = NEW.professor_id) THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ensure_course_professor_existence_trigger
BEFORE INSERT ON course_offers
FOR EACH ROW
EXECUTE FUNCTION ensure_course_and_professor_existence();


CREATE OR REPLACE FUNCTION check_course_offers_professor() RETURNS TRIGGER AS $$
DECLARE
    course_count INTEGER;
    resign INTEGER;
BEGIN
    SELECT COUNT(course_id) INTO course_count FROM course_offers WHERE professor_id = NEW.professor_id AND session = NEW.session;

    select resign_year into resign from professor where professor_id=NEW.professor_id;
    IF course_count >= 4 THEN
        RAISE EXCEPTION 'invalid';
    END IF;
    IF CAST(SUBSTRING(NEW.session FROM 6 FOR 4) AS INTEGER) > resign THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_course_offers_professor
BEFORE INSERT ON course_offers
FOR EACH ROW
EXECUTE FUNCTION check_course_offers_professor();



CREATE OR REPLACE FUNCTION update_department() RETURNS TRIGGER AS $$
BEGIN   
    if(new.dept_id is distinct from old.dept_id) then
        UPDATE courses
        SET dept_id=NEW.dept_id, course_id = NEW.dept_id || SUBSTRING(course_id FROM 4)
        WHERE SUBSTRING(course_id FROM 1 FOR 3) like OLD.dept_id;

        UPDATE course_offers
        SET course_id = NEW.dept_id || SUBSTRING(course_id FROM 4)
        WHERE SUBSTRING(course_id FROM 1 FOR 3) like OLD.dept_id;
        
        UPDATE student_courses
        SET course_id = NEW.dept_id || SUBSTRING(course_id FROM 4)
        WHERE SUBSTRING(course_id FROM 1 FOR 3) like OLD.dept_id;

        UPDATE student
        SET student_id = SUBSTRING(student_id, 1, 4) || NEW.dept_id || SUBSTRING(student_id, 8),
            email_id = SUBSTRING(student_id, 1, 4) || NEW.dept_id || SUBSTRING(student_id, 8) || '@' || NEW.dept_id || '.iitd.ac.in'
        WHERE dept_id = OLD.dept_id;
    end if;
        
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_department() RETURNS TRIGGER AS $$
BEGIN
    
    IF EXISTS (SELECT 1 FROM student WHERE dept_id = OLD.dept_id) THEN
        RAISE EXCEPTION 'Department has students';
    END IF;
    
    -- Delete courses from course_offers and courses tables
    DELETE FROM course_offers WHERE LEFT(course_id, 3) = OLD.dept_id;
    DELETE FROM courses WHERE LEFT(course_id, 3) = OLD.dept_id;
    DELETE FROM valid_entry WHERE dept_id = OLD.dept_id;
    DELETE FROM professor WHERE dept_id = OLD.dept_id;

    
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_department_trigger
AFTER UPDATE ON department
FOR EACH ROW
EXECUTE FUNCTION update_delete_department();

CREATE TRIGGER delete_department_trigger
BEFORE DELETE ON department
FOR EACH ROW
EXECUTE FUNCTION delete_department();