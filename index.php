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

require_login();

$PAGE->set_url('/local/ual_db_process/index.php');
$context = get_context_instance(CONTEXT_SYSTEM);
$PAGE->set_context($context);
$PAGE->set_title(get_string('testpage_title', 'local_ual_db_process'));

$output = $OUTPUT->header();
$output .= $OUTPUT->heading(get_string('testpage_title', 'local_ual_db_process'));

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

// Test link to MIS
if (class_exists('target_mis')) {
    $mis = new target_mis();
    $status = $mis->get_status();
    $output .= html_writer::tag('h2', get_string('connection_status', 'local_ual_db_process'));
    $connected_text = ($status['connected']) ? get_string('connected', 'local_ual_db_process') : get_string('failed', 'local_ual_db_process');
    $output .= html_writer::tag('p', $connected_text);

    $output .= html_writer::tag('h2', get_string('errors', 'local_ual_db_process'));
    $error_list = $status['errorlist'];
    if(!empty($error_list) && is_array($error_list)) {
        $error_text = html_writer::start_tag('ol');
        foreach($error_list as $error) {
            $error_text .= html_writer::tag('li', $error);
        }
        $error_text .= html_writer::end_tag('ol');
        $output .= $error_text;
    } else {
        $output .= get_string('no_errors', 'local_ual_db_process');
    }

}

$output .= $OUTPUT->footer();

echo $output;
