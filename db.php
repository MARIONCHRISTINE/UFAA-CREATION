<?php
// Database Configuration - TRINO/PRESTO MODE
require_once 'trino_client.php';

// Config from your Python snippet
$host = '172.23.56.100'; 
$port = '8445';
$user = '23203159';    
$pass = 'Ordinary@1234'; 
$catalog = 'hive';  // From python: catalog='hive'
$schema = 'canvas'; // From python: schema='canvas'

try {
    // Instantiate our custom wrapper instead of PDO
    // We assign it to $pdo variable name so existing code (index.php) works with minimal changes
    $pdo = new TrinoClient($host, $port, $user, $pass, $catalog, $schema);
} catch (Exception $e) {
    die("Trino Connection Setup Failed: " . $e->getMessage());
}
?>
