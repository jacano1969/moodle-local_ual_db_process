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
 * Run the code checker from the web.
 *
 * @package    local_ual_db_process
 * @copyright  2012 University of London Computer Centre
 * @license    http://www.gnu.org/copyleft/gpl.html GNU GPL v3 or later
 */

require_once(dirname(__FILE__) . '/../../config.php');
require_once($CFG->dirroot . '/local/ual_db_process/lib.php');
require_once("$CFG->libdir/formslib.php");

require_login();

class override_form extends moodleform {

    function definition() {
        $mform =& $this->_form; // Don't forget the underscore!

        $mform->addElement('header', 'resetheader', get_string('reset_header', 'local_ual_db_process'));
        $resetbuttonarray=array();
        $resetbuttonarray[] = &$mform->createElement('submit', 'reset_db', get_string('reset_db', 'local_ual_db_process'));
        $mform->addGroup($resetbuttonarray, 'resetbuttonar', '', array(' '), false);

        $mform->addElement('header', 'usersheader', get_string('users_header', 'local_ual_db_process'));
        $usersbuttonarray=array();
        $usersbuttonarray[] = &$mform->createElement('submit', 'create_users', get_string('create_users', 'local_ual_db_process'));
        $mform->addGroup($usersbuttonarray, 'usersbuttonar', '', array(' '), false);

        $mform->addElement('header', 'categoryheader', get_string('category_header', 'local_ual_db_process'));
        $categorybuttonarray=array();
        $categorybuttonarray[] = &$mform->createElement('submit', 'update_categories', get_string('update_categories', 'local_ual_db_process'));
        $mform->addGroup($categorybuttonarray, 'categorybuttonar', '', array(' '), false);

        $mform->addElement('header', 'courseheader', get_string('courses_header', 'local_ual_db_process'));
        $coursebuttonarray=array();
        $coursebuttonarray[] = &$mform->createElement('submit', 'create_allyear_courses', get_string('create_allyear_courses', 'local_ual_db_process'));
        $coursebuttonarray[] = &$mform->createElement('submit', 'create_courses', get_string('create_courses', 'local_ual_db_process'));
        $mform->addGroup($coursebuttonarray, 'coursesbuttonar', '', array(' '), false);

        $mform->addElement('header', 'authheader', get_string('auth_header', 'local_ual_db_process'));
        $authbuttonarray=array();
        $authbuttonarray[] = &$mform->createElement('submit', 'auth_users', get_string('auth_users', 'local_ual_db_process'));
        $mform->addGroup($authbuttonarray, 'authbuttonar', '', array(' '), false);

        $mform->addElement('header', 'enrolheader', get_string('enrol_header', 'local_ual_db_process'));
        $enrolbuttonarray=array();
        $enrolbuttonarray[] = &$mform->createElement('submit', 'update_enrolments', get_string('update_enrolments', 'local_ual_db_process'));
        $enrolbuttonarray[] = &$mform->createElement('submit', 'enrol_users', get_string('enrol_users', 'local_ual_db_process'));
        $mform->addGroup($enrolbuttonarray, 'enrolbuttonar', '', array(' '), false);

        $mform->addElement('header', 'syncheader', get_string('sync_header', 'local_ual_db_process'));
        $syncbuttonarray=array();
        $syncbuttonarray[] = &$mform->createElement('submit', 'perform_sync', get_string('perform_sync', 'local_ual_db_process'));
        $mform->addGroup($syncbuttonarray, 'syncbuttonar', '', array(' '), false);
    }                           // Close the function
}                               // Close the class

define('UAL_ACTION_NONE', 0);
define('UAL_ACTION_CREATE_COURSES', 1);
define('UAL_ACTION_UPDATE_ENROLMENT_TABLES', 2);
define('UAL_ACTION_ENROL_USERS', 3);
define('UAL_ACTION_UPDATE_CATEGORY', 5);
define('UAL_ACTION_CREATE_USERS', 6);
define('UAL_ACTION_AUTH_USERS', 7);
define('UAL_ACTION_SYNC', 8);
define('UAL_ACTION_ALLYEAR', 9);
define('UAL_ACTION_RESET_DB', 10);

$action = UAL_ACTION_NONE;

if (isset($_POST['reset_db'])) {
    $action = UAL_ACTION_RESET_DB;
}

if (isset($_POST['create_users'])) {
    $action = UAL_ACTION_CREATE_USERS;
}

if (isset($_POST['create_courses'])) {
    $action = UAL_ACTION_CREATE_COURSES;
}

if (isset($_POST['auth_users'])) {
    $action = UAL_ACTION_AUTH_USERS;
}

if (isset($_POST['update_enrolments'])) {
    $action = UAL_ACTION_UPDATE_ENROLMENT_TABLES;
}

if (isset($_POST['enrol_users'])) {
    $action = UAL_ACTION_ENROL_USERS;
}

if (isset($_POST['perform_sync'])) {
    $action = UAL_ACTION_SYNC;
}

if (isset($_POST['create_allyear_courses'])) {
    $action = UAL_ACTION_ALLYEAR;
}

if (isset($_POST['update_categories'])) {
    $action = UAL_ACTION_UPDATE_CATEGORY;
}

$PAGE->set_url('/local/ual_db_process/override.php');
$context = get_context_instance(CONTEXT_SYSTEM);
$PAGE->set_context($context);
$PAGE->set_title(get_string('overridepage_title', 'local_ual_db_process'));

$output = $OUTPUT->header();
$output .= $OUTPUT->heading(get_string('overridepage_title', 'local_ual_db_process'));

$hassiteconfig = has_capability('moodle/site:config', $context);

if($hassiteconfig) {
    // Link to settings page...
    $output .= html_writer::start_tag('div', array('class'=>'in-page-controls'));
    $output .= html_writer::start_tag('p', array('class='=>'settings'));
    $output .= html_writer::start_tag('a', array('href'=>$CFG->wwwroot.'/admin/settings.php?section=local_ual_db_process'));
    $output .= get_string('settings', 'local_ual_db_process');
    $output .= html_writer::end_tag('a');
    $output .= html_writer::end_tag('p');
    $output .= html_writer::end_tag('div');
}

echo $output;

if($action != UAL_ACTION_NONE) {
    if (class_exists('target_mis')) {
        // Perform the action...
        $mis = new target_mis;

        $db_result = array();

        $throttle = get_config('local_ual_db_process', 'throtte');

        switch($action) {
            case UAL_ACTION_RESET_DB:
                $db_result = $mis->db_reset();
                break;
            case UAL_ACTION_UPDATE_CATEGORY:
                $targetcategory = get_config('local_ual_db_process', 'targetcategory');
                $db_result = $mis->create_new_category($targetcategory);
                break;
            case UAL_ACTION_CREATE_USERS:
                $db_result = $mis->create_users($throttle);
                break;
            case UAL_ACTION_CREATE_COURSES:
                $targetcategory = get_config('local_ual_db_process', 'targetcategory');
                $db_result = $mis->create_courses($throttle, $targetcategory);
                break;
            case UAL_ACTION_ALLYEAR:
                $db_result = $mis->infer_course_all_years();
                break;
            // Course enrolments
            case UAL_ACTION_UPDATE_ENROLMENT_TABLES:
                // Remove enrolment views.
                $db_result = $mis->remove_enrolment_tables();
                // Create the necessary views on to the data.
                $db_result = $mis->create_enrolment_tables();
                // Truncate the enrolments table...
                $db_result = $mis->clear_enrolments();
                // Now students on to units...
                $db_result = $mis->update_unit_enrolments();
                // ... courses...
                $db_result = $mis->update_course_enrolments();
                // ... course (all years)...
                $db_result = $mis->update_course_all_years_enrolments();
                // .. and programmes
                $db_result = $mis->update_programme_enrolments();
                // Now update staff enrolments...
                $staffrole = get_config('local_ual_db_process', 'staffrole');
                $db_result = $mis->update_staff_enrolments($staffrole);
                break;
            case UAL_ACTION_AUTH_USERS:
                $db_result = $mis->authenticate_users(true, true);
                break;
            case UAL_ACTION_ENROL_USERS:
                $db_result = $mis->enrol_users();
                break;
            case UAL_ACTION_SYNC:
                $db_result = $mis->perform_sync();
                break;
            default: break;
        }

        if(!empty($db_result)) {
            // Display output in a box...
        }
    }
}

// Display buttons to prompt an action...
$form = new override_form;

$form->display();

$output = $OUTPUT->footer();

echo $output;
