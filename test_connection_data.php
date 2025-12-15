<?php
// Lightweight Connection Tester for NEW TABLES
error_reporting(E_ALL);
ini_set('display_errors', 1);

echo "<h1>Trino HTTP Connection Check (NEW TABLES)</h1>";
echo "<p>Test Type: <strong>CURL / HTTPS</strong> (Trino Protocol)</p>";
echo "<p>Testing: <strong>hive.sre.UFAA_data</strong> → <strong>iceberg.adhoc.ufaa_data</strong></p>";

try {
    require_once 'db_data.php';
    
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

    // 2. Hive Table Check
    $hiveTable = 'hive.sre.UFAA_data';
    echo "<p>Checking Hive Table: <code>$hiveTable</code>...</p>";
    $stmt = $pdo->query("SELECT * FROM $hiveTable LIMIT 1");
    $data = $stmt->fetchAll();
    
    if (count($data) > 0) {
        echo "<div style='color: green;'>✅ Hive Table Accessible! Found " . count($data) . " row(s).</div>";
        echo "<pre>" . print_r($data[0], true) . "</pre>";
    } else {
        echo "<div style='color: orange;'>⚠️ Hive Table exists but is empty (expected for new table).</div>";
    }

    // 3. Iceberg Table Check
    $icebergTable = 'iceberg.adhoc.ufaa_data';
    echo "<p>Checking Iceberg Table: <code>$icebergTable</code>...</p>";
    $stmt = $pdo->query("SELECT * FROM $icebergTable LIMIT 1");
    $data = $stmt->fetchAll();
    
    if (count($data) > 0) {
        echo "<div style='color: green;'>✅ Iceberg Table Accessible! Found " . count($data) . " row(s).</div>";
        echo "<pre>" . print_r($data[0], true) . "</pre>";
    } else {
        echo "<div style='color: orange;'>⚠️ Iceberg Table exists but is empty (expected for new table).</div>";
    }

} catch (Exception $e) {
    echo "<div style='color: red; font-weight: bold;'>❌ Failure</div>";
    echo "<p>Error: " . $e->getMessage() . "</p>";
}
?>
