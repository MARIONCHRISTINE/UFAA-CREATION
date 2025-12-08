<?php
require_once 'db.php';

$tableName = 'iceberg.adhoc.ufaa_23203159'; // Updated to full qualified name

try {
    // 1. Fetch filtered data (streaming)
    $sql = "SELECT * FROM $tableName WHERE 1=1";
    $params = [];

    // Capture Filter Inputs
    $filterName = $_GET['filter_name'] ?? '';
    $filterDate = $_GET['filter_date'] ?? '';

    // Apply Name Filter
    if (!empty($filterName)) {
        $sql .= " AND LOWER(\"﻿owner_name\") LIKE :name";
        $params[':name'] = '%' . strtolower($filterName) . '%';
    }

    // Apply Date Filter
    if (!empty($filterDate)) {
        $sql .= " AND \"transaction_date\" = :date";
        $params[':date'] = $filterDate;
    }

    // Note for Bulk Download: No LIMIT here. We want ALL matching rows.
    
    $stmt = $pdo->prepare($sql);
    
    // Bind
    foreach ($params as $key => $val) {
        $stmt->bindValue($key, $val);
    }
    
    $stmt->execute();
    
    // 2. Set Headers for Download
    header('Content-Type: text/csv');
    header('Content-Disposition: attachment; filename="full_dataset_export.csv"');
    header('Pragma: no-cache');
    header('Expires: 0');

    $fp = fopen('php://output', 'w');

    // 3. Write CSV Headers
    // Fetch first row to get column names
    // Note: If table is empty, this might fetch nothing.
    // We'll peek at column meta if possible, or just fetch first row
    $firstRow = $stmt->fetch(PDO::FETCH_ASSOC);
    
    if ($firstRow) {
        // Write headers
        fputcsv($fp, array_keys($firstRow));
        
        // Write first row
        fputcsv($fp, $firstRow);

        // 4. Loop remaining
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            fputcsv($fp, $row);
        }
    } else {
        // Empty CSV if no data
        fputcsv($fp, ['No Data Found']);
    }

    fclose($fp);
    exit;

} catch (Exception $e) {
    die("Error exporting data: " . $e->getMessage());
}
?>
