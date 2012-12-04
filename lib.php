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
require($CFG->dirroot.'/enrol/databaseextended/lib.php');

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
     * Resets the DB back to a known state. Removes views, temporary tables, truncates tables where necessary (which
     * isn't always necessary).
     *
     * @return array
     */
    public function db_reset() {
        $result = array();

        return $result;
    }

    public function create_new_category($category='Miscellaneous') {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
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
                                u.STUDENTID AS SOURCE_STUDENTID,
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
                                users AS u
                                LEFT JOIN db_process_users AS db_proc ON u.STUDENTID=db_proc.USERNAME
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
                $sql = "INSERT INTO db_process_users(USERNAME,FIRSTNAME,LASTNAME,EMAIL,INSTITUTION)
                        SELECT SOURCE_STUDENTID,SOURCE_FIRSTNAME,SOURCE_LASTNAME,SOURCE_EMAIL,SOURCE_COLLEGE
                        FROM temp_table
                        LEFT JOIN db_process_users AS db_proc
                        ON temp_table.SOURCE_STUDENTID = db_proc.USERNAME
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
                                u.STUDENTID AS SOURCE_STUDENTID,
                                u.FIRSTNAME AS SOURCE_FIRSTNAME,
                                u.LASTNAME AS SOURCE_LASTNAME,
                                u.EMAIL AS SOURCE_EMAIL,
                                u.COLLEGE AS SOURCE_COLLEGE
                                FROM
                                db_process_users AS db_proc
                                INNER JOIN users AS u ON db_proc.USERNAME=u.STUDENTID
                                                        AND (db_proc.FIRSTNAME IS NOT u.FIRSTNAME OR
                                                             db_proc.LASTNAME IS NOT u.LASTNAME OR
                                                             db_proc.EMAIL IS NOT u.EMAIL OR
                                                             db_proc.INSTITUTION IS NOT u.COLLEGE)
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
                                u.STUDENTID AS SOURCE_STUDENTID
                                FROM
                                db_process_users AS db_proc
                                LEFT JOIN users AS u ON db_proc.USERNAME=c.STUDENTID
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
                // Delete students specified in the temp table
                $students = $sqlres->getRows();

                foreach($students as $student) {
                    $sql = "DELETE FROM db_process_users WHERE USERNAME='{$student->TARGET_STUDENTID}'";

                    $sqlres = $this->mis->execute($sql);
                    $result[] = $sqlres;

                    // TODO Need to output some debugging info here.
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
                        SELECT SOURCE_COURSEID,SOURCE_DESCRIPTION,SOURCE_FULL_NAME,'{$category}'
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
                        db_process_courses.COURSE_NAME=temp_table.SOURCE_DESCRIPTION,
                        db_process_courses.COURSE_SHORTNAME=temp_table.SOURCE_FULL_NAME,
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
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            $sql = "INSERT INTO db_process_enrolments(USER_ID,COURSE_ID,ROLE_NAME)
                    SELECT USER_ID,COURSE_ID,ROLE_NAME FROM student_unit_enrolment";

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
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            $sql = "INSERT INTO db_process_enrolments(USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID)
                    SELECT USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID FROM student_course_enrolment";

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
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            $sql = "INSERT INTO db_process_enrolments(USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID)
                    SELECT USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID FROM student_course_all_years_enrolment";

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
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            // Now copy over the data for student programmes enrolment...
            $sql = "INSERT INTO db_process_enrolments(USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID)
                    SELECT USER_ID,COURSE_ID,ROLE_NAME,GROUP_ID FROM student_programme_enrolment";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    public function remove_enrolment_views() {
        $result = array();

        if(!$this->is_connected()) {
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        if($sqlres) {
            $sql = "DROP VIEW course_relationship";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "DROP VIEW student_unit_enrolment";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "DROP VIEW student_course_enrolment";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "DROP VIEW student_course_all_years_enrolment";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "DROP VIEW student_programme_enrolment";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sqlres = $this->mis->commit_transaction();
            $result[] = $sqlres;
        }

        return $result;
    }

    /**
     * Currently we are only creating student enrolment views. Staff enrolment views will come from the Admin DB tool.
     *
     * @return array
     */
    public function create_enrolment_views($studentrole='student', $staffrole='staff') {
        $result = array();

        // Are we connected?
        if(!$this->is_connected()) {
            return $result;
        }

        $sqlres = $this->mis->begin_transaction();
        $result[] = $sqlres;

        // There is going to be a lot of data to process so create views...
        if($sqlres) {
            $sql = "CREATE VIEW course_relationship AS
                        SELECT
                            CONCAT(cs.AOSCD_LINK, cs.LNK_AOS_PERIOD, cs.LNK_PERIOD) AS COURSE_ID,
                            CONCAT(cs.AOS_CODE,cs.AOS_PERIOD,cs.ACAD_PERIOD) AS PARENTID
                        FROM course_structure AS cs";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE VIEW student_unit_enrolment AS
                        SELECT
	                        STUDENTID AS USER_ID,
	                        COURSEID AS COURSE_ID,
	                        '{$studentrole}' AS ROLE_NAME
                        FROM enrolments
	                    WHERE COURSEID NOT REGEXP '^[0-9]'";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE VIEW student_course_enrolment AS
                        SELECT
	                        unit_enrol.USER_ID AS USER_ID,
                            cr.PARENTID AS COURSE_ID,
	                        '{$studentrole}' AS ROLE_NAME,
	                        CONCAT(cr.PARENTID,'-',cr.COURSE_ID) AS GROUP_ID
                        FROM student_unit_enrolment AS unit_enrol
                        INNER JOIN course_relationship AS cr ON unit_enrol.COURSE_ID=cr.COURSE_ID
	                    WHERE cr.PARENTID REGEXP '^[0-9]'";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE VIEW student_course_all_years_enrolment AS
                        SELECT DISTINCT
	                    course_enrol.USER_ID AS USER_ID,
                        CONCAT(SUBSTR(course_enrol.COURSE_ID, 1, 7), SUBSTR(course_enrol.COURSE_ID, -5, 5)) AS COURSE_ID,
	                    '{$studentrole}' AS ROLE_NAME,
	                    CONCAT(SUBSTR(course_enrol.COURSE_ID, 1, 7), SUBSTR(course_enrol.COURSE_ID, -5, 5),'-',course_enrol.COURSE_ID) AS GROUP_ID
                        FROM student_course_enrolment AS course_enrol";

            $sqlres = $this->mis->execute($sql);
            $result[] = $sqlres;

            $sql = "CREATE VIEW student_programme_enrolment AS
                        SELECT DISTINCT
	                    e.USER_ID AS USER_ID,
	                    cr.PARENTID AS COURSE_ID,
	                    '{$studentrole}' AS ROLE_NAME,
                        CONCAT(cr.PARENTID,'-',CONCAT(SUBSTR(cr.COURSE_ID, 1, 7), SUBSTR(cr.COURSE_ID, -5, 5))) AS GROUP_ID
                        FROM course_relationship AS cr
                        INNER JOIN student_course_enrolment AS e ON cr.COURSE_ID=e.COURSE_ID
                        WHERE cr.PARENTID LIKE '%PROGR%'";

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
}
?>