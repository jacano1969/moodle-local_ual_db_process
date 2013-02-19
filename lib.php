<?php
// This file is part of Moodle - http://moodle.org/
//
// Moodle is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Moodle is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Moodle.  If not, see <http://www.gnu.org/licenses/>.

/**
 * This class abstracts the UAL external/MIS database
 *
 * @package    local_ual_db_process
 * @copyright  2012 University of London Computer Centre
 * @license    http://www.gnu.org/copyleft/gpl.html GNU GPL v3 or later
 */

// Use the connection class declared in ual_api. There's no reason to reinvent the wheel...
require_once $CFG->dirroot.'/local/ual_api/connection.class.php';

// User authentication is managed by this plugin but utilises dbsyncother...
require($CFG->dirroot.'/auth/dbsyncother/auth.php');
require_once($CFG->dirroot.'/course/lib.php');
require_once($CFG->dirroot.'/enrol/databaseextended/lib.php');

class target_mis {
    /** @var instance of connection class. Connects to external MIS database */
    private $mis;

    /**
     * @var int timestamp
     */
    public $timestart;

    /**
     * Attempts to connect to MIS as soon as an instance of this class is created.
     */
    public function __construct() {
        $this->connect();
    }

    /**
     * Attempts to connect to the mis. Returns true if connected, else returns false.
     *
     * @return bool
     */
    private function connect() {
        $dbinfo = array('type' => get_config('local_ual_db_process', 'dbconnectiontype'),
            'host' => get_config('local_ual_db_process', 'dbhost'),
            'user' => get_config('local_ual_db_process', 'dbuser'),
            'pass' => get_config('local_ual_db_process', 'dbpassword'),
            'dbname' => get_config('local_ual_db_process', 'dbname'),
            'debug' => get_config('local_ual_db_process', 'dbdebug'));

        // Include the connection class definition if we need to...
        $this->mis = new connection($dbinfo);

        $result = $this->mis->is_connected();

        return $result;
    }

    /**
     * Returns true if we are connected to the MIS, else returns false.
     * If not connected to MIS then we attempt to connect, unless $attempt_connection is set to false.
     *
     * @param bool $attempt_connection
     * @return bool
     */
    private function is_connected($attempt_connection = true) {
        $result = $this->mis->is_connected();

        if(!$result && $attempt_connection) { // Attempt to connect
            $result = $this->connect();
        }

        return $result;
    }

    /**
     * Returns the current connection status as an associative array of two elements:
     *  $result['connected'] - boolean true if connected, else false.
     *  $result['errorlist'] - list of errors returned from adodb layer. Empty if no errors.
     *
     * @return array
     */
    public function get_status() {
        $result = array();

        // Attempt to connect to the database
        $dbinfo = array('type' => get_config('local_ual_api', 'dbconnectiontype'),
            'host' => get_config('local_ual_api', 'dbhost'),
            'user' => get_config('local_ual_api', 'dbuser'),
            'pass' => get_config('local_ual_api', 'dbpassword'),
            'dbname' => get_config('local_ual_api', 'dbname') );

        // Include the connection class definition if we need to...

        $test_connection = new connection($dbinfo);
        $result['connected'] = $test_connection->is_connected();
        $result['errorlist'] = $test_connection->errorlist;

        return $result;
    }

    /**
     * Resets the DB back to a known state. Removes temporary tables, truncates tables where necessary (which
     * isn't always necessary).
     *
     * @return array
     */
    public function db_reset() {
        $result = array();
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            $sql = "DROP TABLE IF EXISTS temp_table";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $this->remove_enrolment_tables();

            // Prepare current tables if necessary. If 'id' is already there then this query will fail...
            $sql = "ALTER TABLE COURSE_STRUCTURE ADD id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE INDEX COURSEID ON COURSES(COURSEID)";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Index USERNAME column in USERS table to speed up building of My Moodle and left hand nav.
            $sql = "CREATE INDEX USERNAME ON USERS(USERNAME)";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // TODO Check that usernames are all lowercase

            $sql = "CREATE TABLE IF NOT EXISTS db_process_category
                   ( id int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),
                     CATEGORY_ID varchar(254),
                     CATEGORY_NAME varchar(255),
                     CATEGORY_PARENT varchar(254) )
                    ENGINE=InnoDB
                    CHARACTER SET utf8 COLLATE utf8_unicode_ci";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE TABLE IF NOT EXISTS db_process_courses
                   ( id int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),
                     COURSE_ID varchar(254),
                     COURSE_SHORTNAME varchar(100),
                     COURSE_NAME varchar(100),
                     COURSE_CATEGORY varchar(254) )
                   ENGINE=InnoDB
                   CHARACTER SET utf8 COLLATE utf8_unicode_ci";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE TABLE IF NOT EXISTS db_process_enrolments_students
                   ( id int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),
                     USER_ID varchar(254),
                     COURSE_ID varchar(254),
                     ROLE_NAME varchar(100),
                     GROUP_ID varchar(254),
                     GROUP_NAME varchar(254) )
                   ENGINE=InnoDB
                   CHARACTER SET utf8 COLLATE utf8_unicode_ci";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE TABLE IF NOT EXISTS db_process_enrolments_staff
                   ( id int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),
                     USER_ID varchar(254),
                     COURSE_ID varchar(254),
                     ROLE_NAME varchar(100),
                     GROUP_ID varchar(254),
                     GROUP_NAME varchar(254) )
                   ENGINE=InnoDB
                   CHARACTER SET utf8 COLLATE utf8_unicode_ci";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE TABLE IF NOT EXISTS db_process_users
                   ( id int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),
                     USERNAME varchar(254),
                     FIRSTNAME varchar(254),
                     LASTNAME varchar(254),
                     EMAIL varchar(254),
                     INSTITUTION varchar(254),
                     IDNUMBER varchar(254) )
                   ENGINE=InnoDB
                   CHARACTER SET utf8 COLLATE utf8_unicode_ci";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    public function create_new_category($category='Miscellaneous') {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            // Truncate the category table...
            $sql = "TRUNCATE TABLE db_process_category";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $category = $this->mis->real_escape_string($category);

            $sql = "INSERT INTO db_process_category(CATEGORY_ID,CATEGORY_NAME,CATEGORY_PARENT)
                        SELECT {$category},{$category},0";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    public function create_users($throttle = 0) {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            // Truncate db_process_courses
            $sql = "TRUNCATE TABLE db_process_users";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Insert users where they have a STUDENTID
            $sql = "INSERT INTO db_process_users(USERNAME,FIRSTNAME,LASTNAME,EMAIL,INSTITUTION,IDNUMBER)
                    SELECT
                      u.USERNAME AS SOURCE_USERNAME,
                      u.FIRSTNAME AS SOURCE_FIRSTNAME,
                      u.LASTNAME AS SOURCE_LASTNAME,
                      u.EMAIL AS SOURCE_EMAIL,
                      u.COLLEGE AS SOURCE_COLLEGE,
                      u.STUDENTID AS IDNUMBER
                    FROM USERS AS u
                    WHERE u.STUDENTID IS NOT NULL";

            if($throttle > 0) {
                $sql .= " LIMIT 0,".$throttle;
            }

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Insert users where they have a USERNAME but no STUDENTID
            $sql = "INSERT INTO db_process_users(USERNAME,FIRSTNAME,LASTNAME,EMAIL,INSTITUTION,IDNUMBER)
                    SELECT
                      u.USERNAME AS SOURCE_STUDENTID,
                      u.FIRSTNAME AS SOURCE_FIRSTNAME,
                      u.LASTNAME AS SOURCE_LASTNAME,
                      u.EMAIL AS SOURCE_EMAIL,
                      u.COLLEGE AS SOURCE_COLLEGE,
                      u.USERNAME AS IDNUMBER
                    FROM USERS AS u
                    WHERE u.STUDENTID IS NULL";

            if($throttle > 0) {
                $sql .= " LIMIT 0,".$throttle;
            }

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    /**
     * Perform a left join on 'courses' and 'db_process_courses' to see if there are any courses listed in 'courses'
     * that are missing from 'db_process_courses'
     *
     * @return int
     */
    public function create_courses($throttle = 0, $category='Miscellaneous') {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {

            // Truncate db_process_courses
            $sql = "TRUNCATE TABLE db_process_courses";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $category = $this->mis->real_escape_string($category);

            $sql = "INSERT INTO db_process_courses(COURSE_ID,COURSE_NAME,COURSE_SHORTNAME,COURSE_CATEGORY)
                    SELECT
                      c.COURSEID AS SOURCE_COURSEID,
                      c.AOS_DESCRIPTION AS SOURCE_DESCRIPTION,
                      c.FULL_DESCRIPTION AS SOURCE_FULL_NAME,
                      {$category} AS CATEGORY
                    FROM COURSES AS c";

            if($throttle > 0) {
                $sql .= " LIMIT 0,".$throttle;
            }

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    public function clear_enrolments() {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            $sql = "TRUNCATE TABLE db_process_enrolments_students";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "TRUNCATE TABLE db_process_enrolments_staff";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }
    }

    public function update_unit_enrolments() {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            $sql = "INSERT INTO db_process_enrolments_students(USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME)
                    SELECT USER_ID,COURSE_ID,ROLE_NAME,'','' FROM student_unit_enrolment";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    public function update_course_enrolments() {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            $sql = "INSERT INTO db_process_enrolments_students(USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME)
                    SELECT USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME FROM student_course_enrolment";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    public function update_course_all_years_enrolments() {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            $sql = "INSERT INTO db_process_enrolments_students(USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME)
                    SELECT USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME FROM student_course_all_years_enrolment";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    public function update_programme_enrolments() {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            // Now copy over the data for student programmes enrolment...
            $sql = "INSERT INTO db_process_enrolments_students(USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME)
                    SELECT USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME FROM student_programme_enrolment";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    /**
     * Temporarily handle staff enrolments.
     *
     * @return array
     */
    public function update_staff_enrolments($staffrole='editingteacher') {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {

            $staffrole = $this->mis->real_escape_string($staffrole);

            // Now copy over the data for student programmes enrolment...
            $sql = "INSERT INTO db_process_enrolments_staff(USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME)
                    SELECT
                      STAFFID,
                      COURSEID,
                      {$staffrole} AS ROLE_NAME,
                      '',
                      ''
                    FROM staff_enrolments";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    public function remove_enrolment_tables() {
        $result = array();

        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            $sql = "DROP TABLE IF EXISTS course_relationship";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "DROP TABLE IF EXISTS student_unit_enrolment";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "DROP TABLE IF EXISTS student_course_enrolment";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "DROP TABLE IF EXISTS student_course_all_years_enrolment";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "DROP TABLE IF EXISTS student_programme_enrolment";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    /**
     * 'Course (all years)' level courses are not included in the IDM data so we have to infer them. Note that they
     * might have been added by the Admin DB tool so we need to check for the presence of a course before we poke anything
     * into the 'courses' table.
     *
     * @return array
     */
    public function infer_course_all_years() {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            // Remove any entries that have COURSEIDs 12 chars long to prevent creating duplicates (note that REPLACE INTO
            // and INSERT IGNORE INTO won't work because the RECORD_IDs are unique
            $sql = "DELETE FROM COURSES WHERE LENGTH(COURSEID)=12";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            if($sqlres) {
                // The following table can take over a minute to construct because we can't use indexes with a REGEXP -
                // a WHERE clause containing a REGEXP has to scan the entire table.
                $sql = "INSERT INTO COURSES(COURSEID, AOS_CODE, AOS_PERIOD, ACAD_PERIOD, COLLEGE, AOS_DESCRIPTION, FULL_DESCRIPTION, SCHOOL)
                        (
                          SELECT DISTINCT
                            CONCAT(SUBSTR(c.COURSEID, 1, 7), SUBSTR(c.COURSEID, -5, 5)) AS COURSEID,
                            c.AOS_CODE AS AOS_CODE,
                            SUBSTR(c.AOS_PERIOD, 1, 2) AS AOS_PERIOD,
                            c.ACAD_PERIOD AS ACAD_PERIOD,
                            c.COLLEGE AS COLLEGE,
                            c.AOS_DESCRIPTION AS AOS_DESCRIPTION,
                            c.AOS_DESCRIPTION AS FULL_DESCRIPTION,
                            c.SCHOOL AS SCHOOL
                          FROM COURSES AS c
                          WHERE c.COURSEID REGEXP '^[0-9]'
                        )";

                $sqlres = $this->mis->execute($sql);
                $result[] = $sqlres;
            }

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    /**
     * Currently we are only creating student enrolment tables. Staff enrolments should come directly from the
     * Admin DB tool via a Web Services interface (i.e. not through here).
     *
     * @return array
     */
    public function create_enrolment_tables($studentrole='student') {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            // Create, and partially fill, a table demonstrating the parent/child relationship...
            $sql = "CREATE TABLE temp_table ENGINE=InnoDB CHARACTER SET utf8 COLLATE utf8_unicode_ci AS
                        SELECT
                            CONCAT(cs.AOSCD_LINK, cs.LNK_AOS_PERIOD, cs.LNK_PERIOD) AS COURSEID,
                            CONCAT(cs.AOS_CODE,cs.AOS_PERIOD,cs.ACAD_PERIOD) AS PARENTID
                        FROM COURSE_STRUCTURE AS cs
                    ";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Add an index to both the COURSEID and PARENTID columns...
            $sql = "CREATE INDEX COURSEID ON temp_table(COURSEID)";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE INDEX PARENTID ON temp_table(PARENTID)";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Populate the 'COURSE_NAME' and 'PARENT_NAME' columns...
            $sql = "CREATE TABLE course_relationship ENGINE=InnoDB CHARACTER SET utf8 COLLATE utf8_unicode_ci
                        SELECT
                            tt.COURSEID AS COURSEID,
                            c1.FULL_DESCRIPTION AS COURSE_NAME,
                            tt.PARENTID AS PARENTID,
                            c2.FULL_DESCRIPTION AS PARENT_NAME
                        FROM temp_table AS tt
                        LEFT JOIN COURSES AS c1 ON tt.COURSEID=c1.COURSEID
                        LEFT JOIN COURSES AS c2 ON tt.PARENTID=c2.COURSEID";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Add a primary key
            $sql = "ALTER TABLE course_relationship ADD id INT(11)
                    NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Add an index to both the COURSEID and PARENTID columns on new table...
            $sql = "CREATE INDEX COURSEID ON course_relationship(COURSEID)";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE INDEX PARENTID ON course_relationship(PARENTID)";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Remove the temporary table...
            $sql = "DROP TABLE temp_table";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Now replace any NULLs in the relationship table with something sensible - names are to be used for group names...
            $sql = "UPDATE course_relationship SET COURSE_NAME = COURSEID where COURSE_NAME IS NULL";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "UPDATE course_relationship SET PARENT_NAME = PARENTID where PARENT_NAME IS NULL";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Create index for STUDENTID on USERS table
            $sql = "CREATE INDEX STUDENTID ON USERS(STUDENTID)";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Create index for USERNAME on users table
            $sql = "CREATE INDEX STUDENTID ON ENROLMENTS(STUDENTID)";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Indexing the STUDENTID columns on both the ENROLMENTS and USERS table will speed up the query slightly
            // but note, again, that we can't use indexes for REGEXP calls.

            $studentrole = $this->mis->real_escape_string($studentrole);

            $sql = "CREATE TABLE student_unit_enrolment ENGINE=InnoDB CHARACTER SET utf8 COLLATE utf8_unicode_ci AS
                        SELECT
	                        u.USERNAME AS USER_ID,
	                        e.COURSEID AS COURSE_ID,
	                        {$studentrole} AS ROLE_NAME
                        FROM ENROLMENTS AS e
                        INNER JOIN USERS AS u ON e.STUDENTID = u.STUDENTID
	                    WHERE COURSEID NOT REGEXP '^[0-9]'";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Add a primary key
            $sql = "ALTER TABLE student_unit_enrolment ADD id INT(11)
                    NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE INDEX COURSE_ID ON student_unit_enrolment(COURSE_ID)";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE INDEX USER_ID ON student_unit_enrolment(USER_ID)";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

           /*

           IDW 07/01/2012 Course years enrolments come from Registry: they should not be inferred...

           $sql = "CREATE TABLE student_course_enrolment AS
                        SELECT DISTINCT
	                        unit_enrol.USER_ID AS USER_ID,
                            cr.PARENTID AS COURSE_ID,
	                        '{$studentrole}' AS ROLE_NAME,
	                        CONCAT(cr.PARENTID,'-',cr.COURSEID) AS GROUP_ID,
	                        cr.COURSE_NAME AS GROUP_NAME
                        FROM student_unit_enrolment AS unit_enrol
                        INNER JOIN course_relationship AS cr ON unit_enrol.COURSE_ID=cr.COURSEID
	                    WHERE cr.PARENTID REGEXP '^[0-9]'";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres; */

            $sql = "CREATE TABLE student_course_enrolment ENGINE=InnoDB CHARACTER SET utf8 COLLATE utf8_unicode_ci AS
                        SELECT DISTINCT
	                        unit_enrol.USER_ID AS USER_ID,
                            cr.PARENTID AS COURSE_ID,
	                        '{$studentrole}' AS ROLE_NAME,
	                        CONCAT(cr.PARENTID,'-',cr.COURSEID) AS GROUP_ID,
	                        cr.COURSE_NAME AS GROUP_NAME
                        FROM student_unit_enrolment AS unit_enrol
                        INNER JOIN course_relationship AS cr ON unit_enrol.COURSE_ID=cr.COURSEID
                        INNER JOIN ENROLMENTS AS e ON cr.PARENTID=e.COURSEID AND unit_enrol.USER_ID=e.STUDENTID
	                    WHERE cr.PARENTID REGEXP '^[0-9]' AND LENGTH(cr.PARENTID) > 12";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // We now need to include course enrolments that aren't based on what units a user is enrolled in...
            $sql = "INSERT INTO student_course_enrolment(USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME)
                    SELECT DISTINCT
                      u.USERNAME,
                      e.COURSEID,
                      '{$studentrole}' AS ROLE_NAME,
                      '',
                      ''
                    FROM ENROLMENTS AS e
                    INNER JOIN USERS AS u ON e.STUDENTID = u.STUDENTID
                    WHERE COURSEID REGEXP '^[0-9]' AND LENGTH(COURSEID) > 12";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Add a primary key
            $sql = "ALTER TABLE student_course_enrolment ADD id INT(11)
                    NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE INDEX USER_ID ON student_course_enrolment(USER_ID)";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE INDEX COURSE_ID ON student_course_enrolment(COURSE_ID)";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // I'm not sure if we are going to have 'Course (all years)' full description so I'm going to leave it out for now...
            /*$sql = "CREATE TABLE student_course_all_years_enrolment AS
                        SELECT DISTINCT
	                    course_enrol.USER_ID AS USER_ID,
                        CONCAT(SUBSTR(course_enrol.COURSE_ID, 1, 7), SUBSTR(course_enrol.COURSE_ID, -5, 5)) AS COURSE_ID,
	                    '{$studentrole}' AS ROLE_NAME,
	                    CONCAT(SUBSTR(course_enrol.COURSE_ID, 1, 7), SUBSTR(course_enrol.COURSE_ID, -5, 5),'-',course_enrol.COURSE_ID) AS GROUP_ID,
	                    course_enrol.GROUP_NAME AS GROUP_NAME
	                    FROM student_course_enrolment AS course_enrol";*/

            $sql = "CREATE TABLE student_course_all_years_enrolment ENGINE=InnoDB CHARACTER SET utf8 COLLATE utf8_unicode_ci AS
                        SELECT DISTINCT
	                    course_enrol.USER_ID AS USER_ID,
                        CONCAT(SUBSTR(course_enrol.COURSE_ID, 1, 7), SUBSTR(course_enrol.COURSE_ID, -5, 5)) AS COURSE_ID,
	                    '{$studentrole}' AS ROLE_NAME,
	                    CONCAT(SUBSTR(course_enrol.COURSE_ID, 1, 7), SUBSTR(course_enrol.COURSE_ID, -5, 5),'-',course_enrol.COURSE_ID) AS GROUP_ID,
	                    c.FULL_DESCRIPTION AS GROUP_NAME
	                    FROM student_course_enrolment AS course_enrol
	                    LEFT JOIN COURSES AS c ON course_enrol.COURSE_ID=c.COURSEID";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // We now need to include course (all years) enrolments that aren't based on what units a user is enrolled in...
            $sql = "INSERT INTO student_course_all_years_enrolment(USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME)
                    SELECT
                      u.USERNAME,
                      e.COURSEID,
                      '{$studentrole}' AS ROLE_NAME,
                      '',
                      ''
                    FROM ENROLMENTS AS e
                    INNER JOIN USERS AS u ON e.STUDENTID = u.STUDENTID
                    WHERE COURSEID REGEXP '^[0-9]' AND LENGTH(COURSEID) = 12";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Add a primary key
            $sql = "ALTER TABLE student_course_all_years_enrolment ADD id INT(11)
                    NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

           /* $sql = "CREATE TABLE student_programme_enrolment AS
                        SELECT DISTINCT
	                    e.USER_ID AS USER_ID,
	                    cr.PARENTID AS COURSE_ID,
	                    '{$studentrole}' AS ROLE_NAME,
                        CONCAT(cr.PARENTID,'-',CONCAT(SUBSTR(cr.COURSEID, 1, 7), SUBSTR(cr.COURSEID, -5, 5))) AS GROUP_ID,
                        cr.COURSEID AS CHILD_COURSE,
                        NULL AS GROUP_NAME
                        FROM course_relationship AS cr
                        INNER JOIN student_course_enrolment AS e ON cr.COURSEID=e.COURSE_ID
                        WHERE cr.PARENTID LIKE '%PROGR%'";*/

            $sql = "CREATE TABLE student_programme_enrolment ENGINE=InnoDB CHARACTER SET utf8 COLLATE utf8_unicode_ci AS
                        SELECT DISTINCT
	                    e.USER_ID AS USER_ID,
	                    cr.PARENTID AS COURSE_ID,
	                    '{$studentrole}' AS ROLE_NAME,
                        CONCAT(cr.PARENTID,'-',CONCAT(SUBSTR(cr.COURSEID, 1, 7), SUBSTR(cr.COURSEID, -5, 5))) AS GROUP_ID,
                        CONCAT(CONCAT(SUBSTR(cr.COURSEID, 1, 7), SUBSTR(cr.COURSEID, -5, 5))) AS CHILD_COURSE,
                        NULL AS GROUP_NAME
                        FROM course_relationship AS cr
                        INNER JOIN student_course_enrolment AS e ON cr.COURSEID=e.COURSE_ID
                        WHERE cr.PARENTID LIKE '%PROGR%'";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Change the COURSEID and  column size (it will start off too small)
            $sql = "ALTER TABLE student_programme_enrolment CHANGE CHILD_COURSE CHILD_COURSE varchar(40)";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Include groups for each course...
            $sql = "INSERT INTO student_programme_enrolment(USER_ID,COURSE_ID,ROLE_NAME,CHILD_COURSE,GROUP_ID,GROUP_NAME)
                        SELECT DISTINCT
	                    e.USER_ID AS USER_ID,
	                    cr.PARENTID AS COURSE_ID,
	                    '{$studentrole}' AS ROLE_NAME,
	                    cr.COURSEID AS CHILD_COURSE,
                        CONCAT(cr.PARENTID,'-',cr.COURSEID) AS GROUP_ID,
                        NULL AS GROUP_NAME
                        FROM course_relationship AS cr
                        INNER JOIN student_course_enrolment AS e ON cr.COURSEID=e.COURSE_ID
                        WHERE cr.PARENTID LIKE '%PROGR%'";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // We now need to include course (all years) enrolments that aren't based on what units a user is enrolled in...
            $sql = "INSERT INTO student_programme_enrolment(USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME)
                    SELECT
                      u.USERNAME,
                      e.COURSEID,
                      '{$studentrole}' AS ROLE_NAME,
                      '',
                      ''
                    FROM ENROLMENTS AS e
                    INNER JOIN USERS AS u ON e.STUDENTID = u.STUDENTID
                    WHERE COURSEID LIKE '%PROGR%'";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Add a primary key
            $sql = "ALTER TABLE student_programme_enrolment ADD id INT(11)
                    NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE INDEX CHILD_COURSE ON student_programme_enrolment(CHILD_COURSE)";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Change the GROUP_NAME column to accomodate the course description...
            $sql = "ALTER TABLE student_programme_enrolment MODIFY GROUP_NAME VARCHAR(254)";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Update the group names...
            $sql = "UPDATE student_programme_enrolment spe
                    INNER JOIN COURSES AS c
                    ON spe.CHILD_COURSE = c.COURSEID
                    SET spe.GROUP_NAME = c.FULL_DESCRIPTION
                    WHERE spe.GROUP_NAME IS NULL";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // For any GROUP_NAMEs that are still NULL...
            $sql = "UPDATE student_programme_enrolment SET GROUP_NAME = CHILD_COURSE where GROUP_NAME IS NULL";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    public function authenticate_users($update=true, $verbose=false) {
        $dbauth = get_auth_plugin('dbsyncother');
        return $dbauth->sync_users($update, $verbose);
    }

    public function enrol_users() {
        if (!enrol_is_enabled('databaseextended')) {
            die('enrol_databaseextended plugin is disabled, sync is disabled');
        }

        $enrol = enrol_get_plugin('databaseextended');

        $tables = array('course_categories',
            'course',
            'enrol',
            'user',
            'role',
            'user_enrolments',
            'role_assignments',
            'context',
            'groups',
            'groups_members');
        $enrol->start_timer();
        $enrol->speedy_sync($tables);
        $enrol->end_timer();
    }

    /**
     * Makes sure that we get lines breaking properly on both web and command line echo statements.
     *
     * @return string either HTML line break tag or line end character
     */
    private function get_line_end() {
        $sapi = php_sapi_name();

        if ($sapi == 'cli') {
            return "\n";
        } else {
            return '<br />';
        }
    }

    /**
     * Records when the sync process starts so that we can keep track of progress.
     */
    public function start_timer() {
        $this->timestart = time();
        $humanreadabletime = date('H:i:s', time());
        echo get_string('processstarted', 'local_ual_db_process', $humanreadabletime)
            .$this->get_line_end();

    }

    /**
     * Tells us how long the sync has been running.
     */
    private function output_time() {

        $humanreadabletime = $this->get_human_readable_time();

        echo get_string('processrunningfor', 'local_ual_db_process', $humanreadabletime).
            $this->get_line_end();
    }

    /**
     * Outputs the metrics telling us how long the sync took overall.
     */
    public function end_timer() {
        $a = new stdClass();
        $a->humanreadabletime = $this->get_human_readable_time();
        $a->timenow  = date('H:i:s', time());
        echo get_string('processfinished', 'local_ual_db_process', $a).$this->get_line_end();
    }

    /**
     * Counts the seconds since the script started and tells us in human readable format.
     *
     * @return string
     */
    private function get_human_readable_time() {

        $seconds = time() - $this->timestart;

        $units = array(
            60 * 60  => array(get_string('hour', 'local_ual_db_process'),
                get_string('hours', 'local_ual_db_process')),
            60       => array(get_string('minute', 'local_ual_db_process'),
                get_string('minutes', 'local_ual_db_process')),
            1        => array(get_string('second', 'local_ual_db_process'),
                get_string('seconds', 'local_ual_db_process')),
        );

        $result = array();
        foreach ($units as $divisor => $unitname) {
            $units = intval($seconds / $divisor);
            if ($units) {
                $seconds %= $divisor;
                $name     = $units == 1 ? $unitname[0] : $unitname[1];
                $result[] = "$units $name";
            }
        }
        if ($result) {
            $humanreadabletime = implode(', ', $result);
        } else {
            $humanreadabletime = "0 ".get_string('seconds', 'local_ual_db_process');
        }

        return $humanreadabletime;
    }

    public function perform_sync() {
        // What are plugin settings?
        $targetcategory = get_config('local_ual_db_process', 'targetcategory');
        $throttle = get_config('local_ual_db_process', 'targetcategory');

        // Perform steps to complete full syncronisation...
        $this->start_timer();

        // 1. Ensure tables we require are all present - and those that shouldn't be there have been removed...
        echo '1. Resetting database back to known state'.$this->get_line_end();
        $this->db_reset();
        $this->output_time();

        $perform_create_course_all_years = get_config('local_ual_db_process', 'create_course_all_years');

        if($perform_create_course_all_years) {
            echo '2. Create \'Course (all years)\' level courses in \'courses\' table'.$this->get_line_end();
            $this->infer_course_all_years();
            $this->output_time();
        } else {
            echo '2. Create \'Course (all years)\' level courses in \'courses\' table WAS SKIPPED (see configuration)'.$this->get_line_end();
        }

        // User authentication:
        // 3. Create new users
        echo '3. Create users'.$this->get_line_end();
        $this->create_users($throttle);
        $this->output_time();

        // Categories:
        // 4. Update categories
        echo '4. Create category'.$this->get_line_end();
        $this->create_new_category($targetcategory);
        $this->output_time();

        // Courses:
        // 5. Update current courses
        echo '5. Create courses'.$this->get_line_end();
        $this->create_courses($throttle, $targetcategory);
        $this->output_time();

        // Enrolments:
        // 6. Update internal enrolment tables
        echo '6. Remove enrolment tables'.$this->get_line_end();
        $this->remove_enrolment_tables();
        $this->output_time();

        // 7. Create the necessary tables on to the data.
        echo '7. Create enrolment tables'.$this->get_line_end();
        $this->create_enrolment_tables();
        $this->output_time();

        // 8. Truncate the enrolments table...
        echo '8. Clear current enrolments'.$this->get_line_end();
        $this->clear_enrolments();
        $this->output_time();

        // 9. Now students on to units...
        echo '9. Add student unit enrolments to student enrolment table'.$this->get_line_end();
        $this->update_unit_enrolments();
        $this->output_time();

        // 10 ... courses...
        echo '10. Add student course enrolments to student enrolment table'.$this->get_line_end();
        $this->update_course_enrolments();
        $this->output_time();

        // 11 ... course (all years)...
        echo '11. Add student course (all years) enrolments to student enrolment table'.$this->get_line_end();
        $this->update_course_all_years_enrolments();
        $this->output_time();

        // 12 ... and programmes
        echo '12. Add student programme enrolments to student enrolment table'.$this->get_line_end();
        $this->update_programme_enrolments();
        $this->output_time();

        // 13 ... Update staff enrolments
        echo '13. Add staff enrolments to staff enrolment table'.$this->get_line_end();
        $staffrole = get_config('local_ual_db_process', 'staffrole');
        $this->update_staff_enrolments($staffrole);
        $this->output_time();

        // Perform actual authentication and enrolment?
        $perform_auth = get_config('local_ual_db_process', 'userauth');
        $perform_enrol = get_config('local_ual_db_process', 'userenrol');

        if($perform_auth) {
            echo '13. Call auth plugin to authenticate users'.$this->get_line_end();
            $this->authenticate_users(false, false);
            $this->output_time();
        }
        if($perform_enrol) {
            echo '14. Call enrolment plugin to enrol users'.$this->get_line_end();
            $this->enrol_users(false, false);
            $this->output_time();
        }

        $this->end_timer();
    }
}
?>