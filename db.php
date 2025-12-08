<?php
// Database Configuration
// Updated based on user feedback to prioritize User: 23203159
// NOTE: If your database is on a remote server (like 172.23.56.100), change 'localhost' to that IP.

$host = 'localhost'; // <--- CHANGE THIS to the IP address if your DB is not on your laptop
$port = '3306';      // Default MySQL port
$db   = 'adhoc';     // Assumption based on "iceberg.adhoc..."
$user = '23203159';  // Confirmed User
$pass = 'Ordinary@1234'; // Confirmed Pass
$charset = 'utf8mb4';

$dsn = "mysql:host=$host;port=$port;dbname=$db;charset=$charset";
$options = [
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    PDO::ATTR_EMULATE_PREPARES   => false,
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
