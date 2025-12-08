<?php
// Lightweight Connection Tester
error_reporting(E_ALL);
ini_set('display_errors', 1);

echo "<h1>Trino HTTP Connection Check</h1>";
echo "<p>Test Type: <strong>CURL / HTTPS</strong> (Trino Protocol)</p>";

try {
    require_once 'db.php';
    
    // 1. Simple Select
    echo "<p>Sending <code>SELECT 1</code> to $host:$port...</p>";
    $stmt = $pdo->query("SELECT 1 as test_col");
    $row = $stmt->fetch();
    
    if ($row) {
         echo "<div style='color: green; font-weight: bold;'>✅ Success! Connected via HTTPS.</div>";
         print_r($row);
    } else {
         echo "<div style='color: orange;'>⚠️ Connected but no data returned?</div>";
    }

    // 2. Table Check
    $tableName = 'iceberg.adhoc.ufaa_23203159';
    echo "<p>Checking Table: <code>$tableName</code>...</p>";
    $stmt = $pdo->query("SELECT * FROM $tableName LIMIT 1");
    $data = $stmt->fetchAll();
    
    if (count($data) > 0) {
        echo "<div style='color: green;'>✅ Table Accessible! Found " . count($data) . " row(s).</div>";
        echo "<pre>" . print_r($data[0], true) . "</pre>";
    } else {
        echo "<div style='color: orange;'>⚠️ Table found but empty?</div>";
    }

} catch (Exception $e) {
    echo "<div style='color: red; font-weight: bold;'>❌ Failure</div>";
    echo "<p>Error: " . $e->getMessage() . "</p>";
}
?>
