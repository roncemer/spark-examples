<?php

$base_time = strtotime('2022-08-01 00:00:00 UTC');

$fp = fopen(dirname(__DIR__).'/flat_map_group_processing_events.csv.new', 'w');
fputs($fp, "event_time,group_no,weight\n");
for ($i = 0; $i < 1000; $i++) {
    $event_time = gmdate(DATE_ATOM, $base_time+mt_rand(0, 86399));
    $group_no = mt_rand(1, 10);
    $weight = mt_rand(1, 1000000) * 0.0001;
    fprintf($fp, "%s,%d,%.4f\n", $event_time, $group_no, $weight);
}
fclose($fp);

$fp = fopen(dirname(__DIR__).'/flat_map_group_processing_groups.csv.new', 'w');
fputs($fp, "group_no,total_value\n");
for ($group_no = 1; $group_no <= 10; $group_no++) {
    $total_value = (500 + mt_rand(0, 9500)) * 0.01;
    fprintf($fp, "%d,%.2f\n", $group_no, $total_value);
}
fclose($fp);
