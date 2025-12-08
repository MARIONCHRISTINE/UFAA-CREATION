<?php
// Database Configuration
// Updated to use port 8445 (from your Python sample)
// CRITICAL: Standard MySQL is port 3306. The sample usage of 8445 usually implies Trino/Presto over HTTPs.
// If this fails, please check the 'Port' field in your DBeaver connection settings.

$host = '172.23.56.100'; 
$port = '8445';        // Changed from 3306 to 8445 based on your sample
$db   = 'adhoc';       // Schema name
$user = '23203159';    
$pass = 'Ordinary@1234'; 
$charset = 'utf8mb4';

$dsn = "mysql:host=$host;port=$port;dbname=$db;charset=$charset";
$options = [
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    PDO::ATTR_EMULATE_PREPARES   => false,
    PDO::ATTR_TIMEOUT            => 5,
];

try {
    $pdo = new PDO($dsn, $user, $pass, $options);
} catch (\PDOException $e) {
    // We catch this in the files designed to include this, 
    // but for debugging purposes, we leave it to the calling script to handle or die.
    if (basename($_SERVER['PHP_SELF']) == 'db.php') {
        die("Connection Failed: " . $e->getMessage());
    }
    throw $e;
}
?>
