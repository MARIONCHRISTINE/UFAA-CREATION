<?php
// Lightweight Connection Tester
error_reporting(E_ALL);
ini_set('display_errors', 1);

echo "<h1>Fast Connection Check</h1>";
echo "<p>Testing connection to <strong>Host: 172.23.56.100</strong> on <strong>Port: 8445</strong>...</p>";

$start = microtime(true);

try {
    require_once 'db.php';
    
    // 1. Simple Ping
    $pdo->query("SELECT 1");
    echo "<div style='color: green; font-weight: bold;'>✅ TCP Connection & Auth Successful!</div>";
    
    $duration = round(microtime(true) - $start, 4);
    echo "<p>Time taken: {$duration} seconds</p>";

    // 2. Simple Table Access (No Metadata/Show Tables)
    // We try to select 0 rows just to check if table exists and we have permission
    $tableName = 'iceberg.adhoc.ufaa_23203159';
    echo "<p>Checking existence of table: <code>$tableName</code>...</p>";
    
    try {
        $pdo->query("SELECT * FROM $tableName LIMIT 0");
        echo "<div style='color: green;'>✅ Table found and accessible!</div>";
    } catch (Exception $e) {
        echo "<div style='color: orange;'>⚠️ Connection OK, but Table check failed.</div>";
        echo "<small>" . $e->getMessage() . "</small>";
    }

} catch (Exception $e) {
    echo "<div style='color: red; font-weight: bold;'>❌ Connection Failed</div>";
    echo "<p>Error: " . $e->getMessage() . "</p>";
}
?>
