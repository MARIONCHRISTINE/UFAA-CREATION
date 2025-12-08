<?php
// Run this file in your browser: http://localhost/UFAA-CREATION/test_connection.php
error_reporting(E_ALL);
ini_set('display_errors', 1);

echo "<h1>Database Connection Diagnostic</h1>";

try {
    require_once 'db.php';
    echo "<div style='color: green; font-weight: bold;'>✅ Success! Connection to MySQL established.</div>";
    
    echo "<h3>Connection Details:</h3>";
    echo "<ul>";
    echo "<li>Host: $host</li>";
    echo "<li>Database: $db</li>";
    echo "<li>User: $user</li>";
    echo "</ul>";

    echo "<h3>Table Check:</h3>";
    $tableName = 'ufaa_23203159'; // Your table
    $stmt = $pdo->query("SHOW TABLES LIKE '$tableName'");
    if ($stmt->rowCount() > 0) {
        echo "<div style='color: green;'>✅ Table <code>$tableName</code> exists.</div>";
        
        // Show columns to verify structure
        $cols = $pdo->query("DESCRIBE $tableName")->fetchAll();
        echo "<br><strong>Columns Found:</strong><br><pre>";
        foreach($cols as $col) {
            echo $col['Field'] . " (" . $col['Type'] . ")\n";
        }
        echo "</pre>";
        
    } else {
        echo "<div style='color: red;'>❌ Table <code>$tableName</code> NOT found in database <code>$db</code>.</div>";
        echo "<p>Please check if the table name is correct or if it's in a different database.</p>";
    }

} catch (Exception $e) {
    echo "<div style='color: red; font-weight: bold;'>❌ Connection Failed</div>";
    echo "<p>Error Message: " . $e->getMessage() . "</p>";
    
    echo "<h3>Troubleshooting Tips:</h3>";
    echo "<ul>";
    echo "<li><strong>Unknown Database?</strong>: Check if '$db' is the correct name.</li>";
    echo "<li><strong>Access Denied?</strong>: Check password for user '$user'.</li>";
    echo "<li><strong>Connection Refused?</strong>: If DB is on a company server, change <code>\$host = 'localhost'</code> in <code>db.php</code> to the server IP (e.g., 172.23.56.100).</li>";
    echo "</ul>";
}
?>
