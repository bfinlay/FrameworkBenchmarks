<?php
require_once __DIR__ . '/vendor/autoload.php';

use ClanCats\Hydrahon\Builder;

header('Content-type: application/json');

// Database connection
// http://www.php.net/manual/en/ref.pdo-mysql.php
$pdo = new PDO('mysql:host=tfb-database;dbname=hello_world', 'benchmarkdbuser', 'benchmarkdbpass', array(
    PDO::ATTR_PERSISTENT => true
));

// create a new mysql query builder
$h = new Builder('mysql', function($query, $queryString, $queryParameters) use($pdo)
{
    $statement = $pdo->prepare($queryString);
    $statement->execute($queryParameters);

    // when the query is fetchable return all results and let hydrahon do the rest
    if ($query instanceof \ClanCats\Hydrahon\Query\Sql\FetchableInterface)
    {
        return $statement->fetchAll(\PDO::FETCH_ASSOC);
    }
});


// Read number of queries to run from URL parameter
$query_count = 1;
$query_param = isset($_GET['queries']);
if ($query_param && $_GET['queries'] > 0) {
  $query_count = $_GET['queries'] > 500 ? 500 : $_GET['queries'];
}

// Create an array with the response string.
$arr = array();

// Define query
$world = $h->table('World');
//$statement = $pdo->prepare('SELECT randomNumber FROM World WHERE id = ?');

// For each query, store the result set values in the response array
$query_counter = $query_count;
while (0 < $query_counter--) {
  $id = mt_rand(1, 10000);

  // Store result in array.
  $arr[] = array('id' => $id, 'randomNumber' => $world->select()->where('id', $id)->column('randomNumber'));
}

// Use the PHP standard JSON encoder.
// http://www.php.net/manual/en/function.json-encode.php
if ($query_count === 1 && !$query_param) {
      $arr = $arr[0];
}

echo json_encode($arr);
