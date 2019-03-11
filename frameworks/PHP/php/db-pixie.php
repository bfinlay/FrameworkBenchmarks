<?php

// Make sure you have Composer's autoload file included
require 'vendor/autoload.php';

// Create a connection, once only.
$config =
    [
        // Name of database driver or IConnectionAdapter class
        'driver'    => 'mysql',
        'host'      => 'tfb-database',
        'database'  => 'hello_world',
        'username'  => 'benchmarkdbuser',
        'password'  => 'benchmarkdbpass',

        // Optional
//        'charset'   => 'utf8',

        // Optional
//        'collation' => 'utf8_unicode_ci',

        // Table prefix, optional
//        'prefix'    => '',

        // PDO constructor options, optional
        'options'   => [
//            PDO::ATTR_TIMEOUT => 5,
            PDO::ATTR_EMULATE_PREPARES => false,
            PDO::ATTR_PERSISTENT => true,
        ],
    ];

$queryBuilder = (new \Pecee\Pixie\Connection('mysql', $config))->getQueryBuilder();

// Read number of queries to run from URL parameter
$query_count = 1;
$query_param = isset($_GET['queries']);
if ($query_param && $_GET['queries'] > 0) {
  $query_count = $_GET['queries'] > 500 ? 500 : $_GET['queries'];
}

// Create an array with the response string.
$arr = array();

// Define query
$sql = $queryBuilder->table('World')->where('id',0)->getQuery()->getSql();
$statement = $queryBuilder->pdo()->prepare($sql);

// For each query, store the result set values in the response array
$query_counter = $query_count;
while (0 < $query_counter--) {
  $id = mt_rand(1, 10000);
  $statement->execute(array($id));
  
  // Store result in array.
  $arr[] = array('id' => $id, 'randomNumber' => $statement->fetchColumn());
}

// Use the PHP standard JSON encoder.
// http://www.php.net/manual/en/function.json-encode.php
if ($query_count === 1 && !$query_param) {
      $arr = $arr[0];
}

header('Content-type: application/json');
echo json_encode($arr);
