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
require_once $CFG->dirroot . '/local/ual_api/connection.class.php';

// User authentication is managed by this plugin but utilises dbsyncother...
require($CFG->dirroot.'/auth/dbsyncother/auth.php');
require_once($CFG->dirroot.'/course/lib.php');
require_once($CFG->dirroot.'/enrol/databaseextended/lib.php');

class target_mis {
    /** @var instance of connection class. Connects to external MIS database */
    private $mis;

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
            $sql = "ALTER TABLE `COURSE_STRUCTURE` ADD `id` INT NOT NULL AUTO_INCREMENT FIRST , ADD PRIMARY KEY ( `id` ) ";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE INDEX course_idx ON courses(COURSEID)";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE TABLE IF NOT EXISTS db_process_category
                   ( id int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),
                     CATEGORY_ID varchar(254),
                     CATEGORY_NAME varchar(255),
                     CATEGORY_PARENT varchar(254) )";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE TABLE IF NOT EXISTS db_process_courses
                   ( id int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),
                     COURSE_ID varchar(254),
                     COURSE_SHORTNAME varchar(100),
                     COURSE_NAME varchar(100),
                     COURSE_CATEGORY varchar(254) )";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE TABLE IF NOT EXISTS db_process_enrolments
                   ( id int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),
                     USER_ID varchar(254),
                     COURSE_ID varchar(254),
                     ROLE_NAME varchar(100),
                     GROUP_ID varchar(254),
                     GROUP_NAME varchar(254) )";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE TABLE IF NOT EXISTS db_process_users
                   ( id int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY(id),
                     USERNAME varchar(254),
                     FIRSTNAME varchar(254),
                     LASTNAME varchar(254),
                     EMAIL varchar(254),
                     INSTITUTION varchar(254),
                     IDNUMBER varchar(254) )";
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

            $sql = "INSERT INTO db_process_category(CATEGORY_ID,CATEGORY_NAME,CATEGORY_PARENT)
                        SELECT '{$category}','{$category}',0";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    public function create_new_students($throttle = 0) {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            // Drop the temporary table if it already exists
            $sql = "DROP TABLE IF EXISTS temp_table";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $initial_select = "SELECT
                                LOWER(u.STUDENTID) AS SOURCE_STUDENTID,
                                u.FIRSTNAME AS SOURCE_FIRSTNAME,
                                u.LASTNAME AS SOURCE_LASTNAME,
                                u.EMAIL AS SOURCE_EMAIL,
                                u.COLLEGE AS SOURCE_COLLEGE,
                                db_proc.USERNAME AS TARGET_STUDENTID,
                                db_proc.FIRSTNAME AS TARGET_FIRSTNAME,
                                db_proc.LASTNAME AS TARGET_LASTNAME,
                                db_proc.EMAIL AS TARGET_EMAIL,
                                db_proc.INSTITUTION AS TARGET_COLLEGE
                                FROM
                                USERS AS u
                                LEFT JOIN db_process_users AS db_proc ON LOWER(u.STUDENTID)=db_proc.USERNAME COLLATE utf8_unicode_ci
                                WHERE db_proc.USERNAME IS NULL AND u.STUDENTID IS NOT NULL";

            if($throttle > 0) {
                $initial_select .= " LIMIT 0,".$throttle;
            }

            // Perform left join on data that should be the same. Then we can pick out rows that aren't.
            $sql = "CREATE TABLE temp_table AS
                    (
                      {$initial_select}
                    )";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            if($sqlres) {
                $sql = "INSERT INTO db_process_users(USERNAME,FIRSTNAME,LASTNAME,EMAIL,INSTITUTION,IDNUMBER)
                        SELECT SOURCE_STUDENTID,SOURCE_FIRSTNAME,SOURCE_LASTNAME,SOURCE_EMAIL,SOURCE_COLLEGE,SOURCE_STUDENTID
                        FROM temp_table
                        LEFT JOIN db_process_users AS db_proc
                        ON temp_table.SOURCE_STUDENTID = db_proc.USERNAME COLLATE utf8_unicode_ci
                        WHERE temp_table.TARGET_STUDENTID IS NULL";

                $sqlres = $this->mis->execute($sql);
                $result[] = $sqlres;

                // Drop the temp table as a matter of course...
                $sql = "DROP TABLE temp_table";
                $sqlres = $this->mis->execute($sql);
                $result[] = $sqlres;
            }

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    public function update_students($throttle = 0) {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            // Drop the temporary table if it already exists
            $sql = "DROP TABLE IF EXISTS temp_table";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Perform a SELECT DISTINCT as we are ignoring colleges.
            $initial_select = "SELECT DISTINCT
                                db_proc.USERNAME AS TARGET_STUDENTID,
                                db_proc.FIRSTNAME AS TARGET_FIRSTNAME,
                                db_proc.LASTNAME AS TARGET_LASTNAME,
                                db_proc.EMAIL AS TARGET_EMAIL,
                                db_proc.INSTITUTION AS TARGET_COLLEGE,
                                LOWER(u.STUDENTID) AS SOURCE_STUDENTID,
                                u.FIRSTNAME AS SOURCE_FIRSTNAME,
                                u.LASTNAME AS SOURCE_LASTNAME,
                                u.EMAIL AS SOURCE_EMAIL,
                                u.COLLEGE AS SOURCE_COLLEGE
                                FROM
                                db_process_users AS db_proc
                                INNER JOIN USERS AS u ON db_proc.USERNAME=LOWER(u.STUDENTID)
                                                        AND (db_proc.FIRSTNAME IS NOT u.FIRSTNAME OR
                                                             db_proc.LASTNAME IS NOT u.LASTNAME OR
                                                             db_proc.EMAIL IS NOT u.EMAIL OR
                                                             db_proc.INSTITUTION IS NOT u.COLLEGE) COLLATE utf8_unicode_ci
                                WHERE u.STUDENTID IS NOT NULL";

            if($throttle > 0) {
                $initial_select .= " LIMIT 0,".$throttle;
            }

            // Perform left join on data that should be the same.
            $sql = "CREATE TABLE temp_table AS
                    (
                      {$initial_select}
                    )";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            if($sqlres) {
                // Insert data back into db_process_courses...
                $sql = "UPDATE db_process_users INNER JOIN
                        temp_table
                        ON db_process_users.USERNAME=temp_table.TARGET_STUDENTID
                        SET
                        db_process_users.USERNAME=temp_table.TARGET_STUDENTID,
                        db_process_users.FIRSTNAME=temp_table.TARGET_FIRSTNAME,
                        db_process_users.LASTNAME=temp_table.TARGET_LASTNAME,
                        db_process_users.EMAIL=temp_table.TARGET_EMAIL,
                        db_process_users.INSTITUTION=temp_table.TARGET_COLLEGE";

                $sqlres = $this->mis->execute($sql);
                $result[] = $sqlres;
            }

            $sql = "DROP TABLE temp_table";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }
        return $result;
    }

    public function remove_redundant_students($throttle = 0) {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            // Drop the temporary table if it already exists
            $sql = "DROP TABLE IF EXISTS temp_table";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Perform a SELECT DISTINCT as we are ignoring colleges.
            $initial_select = "SELECT DISTINCT
                                db_proc.USERNAME AS TARGET_STUDENTID,
                                LOWER(u.STUDENTID) AS SOURCE_STUDENTID
                                FROM
                                db_process_users AS db_proc
                                LEFT JOIN USERS AS u ON db_proc.USERNAME=LOWER(u.STUDENTID) COLLATE utf8_unicode_ci
                                WHERE u.STUDENTID IS NULL";

            if($throttle > 0) {
                $initial_select .= " LIMIT 0,".$throttle;
            }

            // Perform left join on data that should be the same.
            $sql = "CREATE TABLE temp_table AS
                    (
                      {$initial_select}
                    )";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            if($sqlres) {
                // How many students are in the temporary table?
                $sql = "SELECT TARGET_STUDENTID FROM temp_table";
                $sqlres = $this->mis->execute($sql);
                $result[] = $sqlres;

                if($sqlres) {
                    // Delete students specified in the temp table
                    $students = $sqlres->getRows();

                    foreach($students as $student) {
                        $sql = "DELETE FROM db_process_users WHERE USERNAME='{$student->TARGET_STUDENTID}'";

                        $sqlres = $this->mis->execute($sql);
                        $result[] = $sqlres;

                        // TODO Need to output some debugging info here.
                    }
                }

                // Drop temp table as a matter of course...
                $sql = "DROP TABLE temp_table";
                $sqlres = $this->mis->execute($sql);
                $result[] = $sqlres;
            }

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
    public function create_new_courses($throttle = 0, $category='Miscellaneous') {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            // Drop temp table as a matter of course...
            $sql = "DROP TABLE IF EXISTS temp_table";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Perform a SELECT DISTINCT as we are ignoring colleges...
            $initial_select = "SELECT DISTINCT
                                c.COURSEID AS SOURCE_COURSEID,
                                c.AOS_DESCRIPTION AS SOURCE_DESCRIPTION,
                                c.FULL_DESCRIPTION AS SOURCE_FULL_NAME,
                                db_proc.COURSE_ID AS TARGET_COURSE_ID,
                                db_proc.COURSE_NAME AS TARGET_COURSE_NAME,
                                db_proc.COURSE_SHORTNAME AS TARGET_COURSE_SHORTNAME
                                FROM
                                courses AS c
                                LEFT JOIN db_process_courses AS db_proc ON c.COURSEID=db_proc.COURSE_ID
                                WHERE db_proc.COURSE_ID IS NULL";

            if($throttle > 0) {
                $initial_select .= " LIMIT 0,".$throttle;
            }

            // Perform left join on data that should be the same. Then we can pick out rows that aren't.
            $sql = "CREATE TABLE temp_table AS
                    (
                      {$initial_select}
                    )";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            if($sqlres) {
                $sql = "INSERT INTO db_process_courses(COURSE_ID,COURSE_NAME,COURSE_SHORTNAME,COURSE_CATEGORY)
                        SELECT SOURCE_COURSEID,SOURCE_FULL_NAME,SOURCE_COURSEID,'{$category}'
                        FROM temp_table
                        LEFT JOIN db_process_courses AS db_proc
                        ON temp_table.SOURCE_COURSEID = db_proc.COURSE_ID
                        WHERE temp_table.TARGET_COURSE_ID IS NULL";

                $sqlres = $this->mis->execute($sql);
                $result[] = $sqlres;

                // Drop the temp table as a matter of course...
                $sql = "DROP TABLE temp_table";
                $sqlres = $this->mis->execute($sql);
                $result[] = $sqlres;
            }

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    public function update_courses($throttle = 0, $category='Miscellaneous') {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            // Drop the temp table as a matter of course...
            $sql = "DROP TABLE IF EXISTS temp_table";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Perform a SELECT DISTINCT as we are ignoring colleges.
            $initial_select = "SELECT DISTINCT
                                db_proc.COURSE_ID AS TARGET_COURSE_ID,
                                db_proc.COURSE_NAME AS TARGET_COURSE_NAME,
                                db_proc.COURSE_SHORTNAME AS TARGET_COURSE_SHORTNAME,
                                c.COURSEID AS SOURCE_COURSEID,
                                c.AOS_DESCRIPTION AS SOURCE_DESCRIPTION,
                                c.FULL_DESCRIPTION AS SOURCE_FULL_NAME
                                FROM
                                db_process_courses AS db_proc
                                INNER JOIN courses AS c ON db_proc.COURSE_ID=c.COURSEID
                                WHERE c.COURSEID IS NOT NULL";

            if($throttle > 0) {
                $initial_select .= " LIMIT 0,".$throttle;
            }

            // Perform left join on data that should be the same.
            $sql = "CREATE TABLE temp_table AS
                    (
                      {$initial_select}
                    )";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            if($sqlres) {
                // Insert data back into db_process_courses...
                $sql = "UPDATE db_process_courses INNER JOIN
                        temp_table
                        ON db_process_courses.COURSE_ID=temp_table.TARGET_COURSE_ID
                        SET
                        db_process_courses.COURSE_ID=temp_table.TARGET_COURSE_ID,
                        db_process_courses.COURSE_NAME=temp_table.SOURCE_FULL_NAME,
                        db_process_courses.COURSE_SHORTNAME=temp_table.TARGET_COURSE_ID,
                        db_process_courses.COURSE_CATEGORY='{$category}'";

                $sqlres = $this->mis->execute($sql);
                $result[] = $sqlres;
            }

            $sql = "DROP TABLE temp_table";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }
        return $result;
    }

    public function remove_redundant_courses($throttle = 0) {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            $result[] = false;
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            // Drop the temp table as a matter of course...
            $sql = "DROP TABLE IF EXISTS temp_table";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Perform a SELECT DISTINCT as we are ignoring colleges.
            $initial_select = "SELECT DISTINCT
                                db_proc.COURSE_ID AS TARGET_COURSE_ID,
                                c.COURSEID AS SOURCE_COURSEID,
                                FROM
                                db_process_courses AS db_proc
                                LEFT JOIN courses AS c ON db_proc.COURSE_ID=c.COURSEID
                                WHERE c.COURSEID IS NOT NULL";

            if($throttle > 0) {
                $initial_select .= " LIMIT 0,".$throttle;
            }

            // Perform left join on data that should be the same.
            $sql = "CREATE TABLE temp_table AS
                    (
                      {$initial_select}
                    )";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            if($sqlres) {
                // Delete courses specified in the temp table
                $courses = $sqlres->getRows();

                foreach($courses as $course) {
                    $sql = "DELETE FROM db_process_courses WHERE COURSE_ID='{$course->TARGET_COURSE_ID}'";

                    $sqlres = $this->mis->execute($sql);
                    $result[] = $sqlres;

                    // TODO Need to output some debugging info here.
                }

                // Drop the temp table as a matter of course...
                $sql = "DROP TABLE temp_table";
                $sqlres = $this->mis->execute($sql);
                $result[] = $sqlres;
            }

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
            $sql = "TRUNCATE TABLE db_process_enrolments";

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
            $sql = "INSERT INTO db_process_enrolments(USER_ID,COURSE_ID,ROLE_NAME)
                    SELECT LOWER(USER_ID),COURSE_ID,ROLE_NAME FROM student_unit_enrolment";

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
            $sql = "INSERT INTO db_process_enrolments(USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME)
                    SELECT LOWER(USER_ID),COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME FROM student_course_enrolment";

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
            $sql = "INSERT INTO db_process_enrolments(USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME)
                    SELECT LOWER(USER_ID),COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME FROM student_course_all_years_enrolment";

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
            $sql = "INSERT INTO db_process_enrolments(USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME)
                    SELECT LOWER(USER_ID),COURSE_ID,ROLE_NAME,GROUP_ID,GROUP_NAME FROM student_programme_enrolment";

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
            // The following table can take over a minute to construct
            $sql = "REPLACE INTO courses(COURSEID, AOS_CODE, AOS_PERIOD, ACAD_PERIOD, COLLEGE, AOS_DESCRIPTION, FULL_DESCRIPTION, SCHOOL)
                    SELECT temp_table.all_years_id,
                           temp_table.aos_code,
                           temp_table.aos_period,
                           temp_table.acad_period,
                           temp_table.college,
                           temp_table.description,
                           temp_table.description,
                           temp_table.school
                    FROM
                    (
                      SELECT DISTINCT
                        CONCAT(SUBSTR(c.COURSEID, 1, 7), SUBSTR(c.COURSEID, -5, 5)) AS all_years_id,
                        c.AOS_CODE AS aos_code,
                        SUBSTR(c.AOS_PERIOD, 1, 2) AS aos_period,
                        c.ACAD_PERIOD AS acad_period,
                        c.COLLEGE AS college,
                        c.AOS_DESCRIPTION AS description,
                        c.SCHOOL AS school
                      FROM courses AS c
                      WHERE c.COURSEID REGEXP '^[0-9]' AND LENGTH(c.COURSEID)=15
                    ) AS temp_table";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }


    /**
     * Currently we are only creating student enrolment tables. Staff enrolments will come directly from the
     * Admin DB tool via a Web Services interface (i.e. not through here).
     *
     * @return array
     */
    public function create_enrolment_tables($studentrole='student', $staffrole='staff') {
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
            $sql = "CREATE TABLE temp_table AS
                        SELECT
                            CONCAT(cs.AOSCD_LINK, cs.LNK_AOS_PERIOD, cs.LNK_PERIOD) AS COURSEID,
                            CONCAT(cs.AOS_CODE,cs.AOS_PERIOD,cs.ACAD_PERIOD) AS PARENTID
                        FROM COURSE_STRUCTURE AS cs";

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
            $sql = "CREATE TABLE course_relationship
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

            $sql = "CREATE TABLE student_unit_enrolment AS
                        SELECT
	                        STUDENTID AS USER_ID,
	                        COURSEID AS COURSE_ID,
	                        '{$studentrole}' AS ROLE_NAME
                        FROM ENROLMENTS
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

            $sql = "CREATE TABLE student_course_enrolment AS
                        SELECT
	                        unit_enrol.USER_ID AS USER_ID,
                            cr.PARENTID AS COURSE_ID,
	                        '{$studentrole}' AS ROLE_NAME,
	                        CONCAT(cr.PARENTID,'-',cr.COURSEID) AS GROUP_ID,
	                        cr.COURSE_NAME AS GROUP_NAME
                        FROM student_unit_enrolment AS unit_enrol
                        INNER JOIN course_relationship AS cr ON unit_enrol.COURSE_ID=cr.COURSEID
	                    WHERE cr.PARENTID REGEXP '^[0-9]'";

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
            $sql = "CREATE TABLE student_course_all_years_enrolment AS
                        SELECT DISTINCT
	                    course_enrol.USER_ID AS USER_ID,
                        CONCAT(SUBSTR(course_enrol.COURSE_ID, 1, 7), SUBSTR(course_enrol.COURSE_ID, -5, 5)) AS COURSE_ID,
	                    '{$studentrole}' AS ROLE_NAME,
	                    CONCAT(SUBSTR(course_enrol.COURSE_ID, 1, 7), SUBSTR(course_enrol.COURSE_ID, -5, 5),'-',course_enrol.COURSE_ID) AS GROUP_ID,
	                    course_enrol.GROUP_NAME AS GROUP_NAME
	                    FROM student_course_enrolment AS course_enrol";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Add a primary key
            $sql = "ALTER TABLE student_course_all_years_enrolment ADD id INT(11)
                    NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE TABLE student_programme_enrolment AS
                        SELECT DISTINCT
	                    e.USER_ID AS USER_ID,
	                    cr.PARENTID AS COURSE_ID,
	                    '{$studentrole}' AS ROLE_NAME,
                        CONCAT(cr.PARENTID,'-',CONCAT(SUBSTR(cr.COURSEID, 1, 7), SUBSTR(cr.COURSEID, -5, 5))) AS GROUP_ID,
                        cr.COURSEID AS CHILD_COURSE,
                        NULL AS GROUP_NAME
                        FROM course_relationship AS cr
                        INNER JOIN student_course_enrolment AS e ON cr.COURSEID=e.COURSE_ID
                        WHERE cr.PARENTID LIKE '%PROGR%'";

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
            $sql = "ALTER TABLE `dev_ualmis`.`student_programme_enrolment` CHANGE
                    COLUMN `GROUP_NAME` `GROUP_NAME` VARCHAR(254) NULL DEFAULT NULL";
            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            // Update the group names...
            $sql = "UPDATE student_programme_enrolment spe
                    INNER JOIN COURSES AS c
                    ON spe.CHILD_COURSE = c.COURSEID
                    SET spe.GROUP_NAME = c.FULL_DESCRIPTION
                    WHERE spe.GROUP_NAME IS NULL;";

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

    public function perform_sync() {
        // What are plugin settings?
        $targetcategory = get_config('local_ual_db_process', 'targetcategory');
        $throttle = get_config('local_ual_db_process', 'targetcategory');

        // Perform steps to complete full syncronisation...

        // 1. Ensure tables we require are all present - and those that shouldn't be there have been removed...
        echo '1. Resetting database back to known stage'.$this->get_line_end();
        $this->db_reset();

        echo '2. Create \'Course (all years)\' level courses in \'courses\' table';
        $this->infer_course_all_years();

        // User authentication:
        // 2. Update current students
        echo '2. Update current students'.$this->get_line_end();
        $this->update_students($throttle);
        // 3. Create new students
        echo '3. Create new students'.$this->get_line_end();
        $this->create_new_students($throttle);
        // 4. Delete old students
        echo '4. Remove old students'.$this->get_line_end();
        $this->remove_redundant_students($throttle);

        // Categories:
        // 5. Update categories
        echo '5. Create category'.$this->get_line_end();
        $this->create_new_category($targetcategory);

        // Courses:
        // 6. Update current courses
        echo '6. Update current courses'.$this->get_line_end();
        $this->update_courses($throttle, $targetcategory);
        // 7. Create new courses
        echo '7. Create new courses'.$this->get_line_end();
        $this->create_new_courses($throttle, $targetcategory);
        // 8. Delete old courses
        echo '8. Create redundant courses'.$this->get_line_end();
        $this->remove_redundant_courses($throttle);

        // Enrolments:
        // 9. Update internal enrolment tables
        echo '9. Remove enrolment tables'.$this->get_line_end();
        $this->remove_enrolment_tables();
        // 10. Create the necessary tables on to the data.
        echo '10. Create enrolment tables'.$this->get_line_end();
        $this->create_enrolment_tables();
        // 11. Truncate the enrolments table...
        echo '11. Clear current enrolments'.$this->get_line_end();
        $this->clear_enrolments();
        echo '12. Add student unit enrolments to enrolment table'.$this->get_line_end();
        // 12. Now students on to units...
        $this->update_unit_enrolments();
        // 13 ... courses...
        echo '13. Add student course enrolments to enrolment table'.$this->get_line_end();
        $this->update_course_enrolments();
        // 14 ... course (all years)...
        echo '14. Add student course (all years) enrolments to enrolment table'.$this->get_line_end();
        $this->update_course_all_years_enrolments();
        // 15 ... and programmes
        echo '15. Add student programme enrolments to enrolment table'.$this->get_line_end();
        $this->update_programme_enrolments();

        // Perform actual authentication and enrolment?
        $perform_auth = get_config('local_ual_db_process', 'userauth');
        $perform_enrol = get_config('local_ual_db_process', 'userenrol');

        if($perform_auth) {
            echo '16. Call auth plugin to authenticate users'.$this->get_line_end();
            $this->authenticate_users(false, false);
        }
        if($perform_enrol) {
            echo '17. Call enrolment plugin to enrol users'.$this->get_line_end();
            $this->enrol_users(false, false);
        }
    }
}
?>