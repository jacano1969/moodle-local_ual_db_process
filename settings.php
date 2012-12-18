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
 * Add page to admin menu.
 *
 * @package    local_ual_db_process
 * @copyright  2012 University of London Computer Centre
 * @license    http://www.gnu.org/copyleft/gpl.html GNU GPL v3 or later
 */

defined('MOODLE_INTERNAL') || die;

if ($hassiteconfig) { // needs this condition or there is error on login page

    $settings = new admin_settingpage('local_ual_db_process',
                                       get_string('pluginname', 'local_ual_db_process'));

    $options = array(
        ' '     => get_string('noconnection','local_ual_db_process'),
        'mssql' => 'Mssql',
        'mysql' => 'Mysql',
        'mysqli' => 'Mysqli',
        'odbc' => 'Odbc',
        'oci8' => 'Oracle',
        'postgres' => 'Postgres',
        'sybase' => 'Sybase'
    );

    $dbtype = new admin_setting_configselect('local_ual_db_process/dbconnectiontype', get_string('db_connection','local_ual_db_process'), '', '', $options);
    $settings->add($dbtype);

    $dbhost = new admin_setting_configtext('local_ual_db_process/dbhost', get_string( 'db_host', 'local_ual_db_process'), '', '', PARAM_TEXT);
    $settings->add($dbhost);

    $dbname = new admin_setting_configtext('local_ual_db_process/dbname', get_string( 'db_name', 'local_ual_db_process'), '', '', PARAM_TEXT);
    $settings->add($dbname);

    $dbuser = new admin_setting_configtext('local_ual_db_process/dbuser', get_string( 'db_user', 'local_ual_db_process'), '', '', PARAM_TEXT);
    $settings->add($dbuser);

    $dbpassword = new admin_setting_configpasswordunmask('local_ual_db_process/dbpassword', get_string( 'db_pass', 'local_ual_db_process' ), '', '');
    $settings->add($dbpassword);

    $dbdebug = new admin_setting_configcheckbox('local_ual_db_process/dbdebug', get_string( 'db_debug', 'local_ual_db_process' ), '', 0);
    $settings->add($dbdebug);

    $targetcategory = new admin_setting_configtext('local_ual_db_process/targetcategory', get_string( 'targetcategory', 'local_ual_db_process' ), '', 'UAL', PARAM_TEXT);
    $settings->add($targetcategory);

    $throttle = new admin_setting_configtext('local_ual_db_process/throttle', get_string( 'throttle', 'local_ual_db_process' ), '', 0, PARAM_INT);
    $settings->add($throttle);

    $staffrole = new admin_setting_configtext('local_ual_db_process/staffrole', get_string( 'staffrole', 'local_ual_db_process' ), '', 'editingteacher', PARAM_TEXT);
    $settings->add($staffrole);

    $studentrole = new admin_setting_configtext('local_ual_db_process/studentrole', get_string( 'studentrole', 'local_ual_db_process' ), '', 'student', PARAM_TEXT);
    $settings->add($studentrole);

    $userauth = new admin_setting_configcheckbox('local_ual_db_process/userauth', get_string( 'user_auth', 'local_ual_db_process' ), '', 1);
    $settings->add($userauth);

    $userenrol = new admin_setting_configcheckbox('local_ual_db_process/userenrol', get_string( 'user_enrol', 'local_ual_db_process' ), '', 1);
    $settings->add($userenrol);

    // Add link to configuration page.
    $ADMIN->add('localplugins', $settings);

    // Add link to test page.
    $ADMIN->add('localplugins', new admin_externalpage('local_ual_db_process_test',
            get_string('testpage_title', 'local_ual_db_process'),
            new moodle_url('/local/ual_db_process/index.php')));

    // Add link to manual override.
    $ADMIN->add('localplugins', new admin_externalpage('local_ual_db_process_override',
        get_string('overridepage_title', 'local_ual_db_process'),
        new moodle_url('/local/ual_db_process/override.php')));
}