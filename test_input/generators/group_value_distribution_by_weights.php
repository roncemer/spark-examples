<?php

function event_time_comparator($a, $b) {
    $atime = strtotime($a->event_time);
    $btime = strtotime($b->event_time);
    return $atime-$btime;
}

$base_time = strtotime('2022-08-01 00:00:00 UTC');
$events = [];
for ($i = 0; $i < 1000; $i++) {
    $events[] = (object)[
        'event_time' => gmdate(DATE_ATOM, $base_time+mt_rand(0, 86399)),
        'group_no' => mt_rand(1, 10),
        'weight' => mt_rand(1, 1000000) * 0.0001,
    ];
}
usort($events, 'event_time_comparator');

$fp = fopen(dirname(__DIR__).'/group_value_distribution_by_weights_events.csv.new', 'w');
fputs($fp, "id,event_time,group_no,weight\n");
$id = 100000 + mt_rand(1, 100000);
foreach ($events as $event) {
    fprintf($fp, "%d,%s,%d,%.4f\n", $id++, $event->event_time, $event->group_no, $event->weight);
}
fclose($fp);

$fp = fopen(dirname(__DIR__).'/group_value_distribution_by_weights_groups.csv.new', 'w');
fputs($fp, "group_no,total_value\n");
for ($group_no = 1; $group_no <= 10; $group_no++) {
    $total_value = (500 + mt_rand(0, 9500)) * 0.01;
    fprintf($fp, "%d,%.2f\n", $group_no, $total_value);
}
fclose($fp);
