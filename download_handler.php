<?php
require_once 'db.php';

$tableName = 'iceberg.adhoc.ufaa_23203159'; // Updated to full qualified name

try {
    // 1. Fetch filtered data (streaming)
    $sql = "SELECT * FROM $tableName WHERE 1=1";
    $params = [];

    // Capture Filter Inputs
    $f_name = $_GET['f_name'] ?? '';
    $f_id   = $_GET['f_id'] ?? '';
    $f_dob  = $_GET['f_dob'] ?? '';
    $f_amount = $_GET['f_amount'] ?? '';
    $f_date_start = $_GET['f_date_start'] ?? '';
    $f_date_end   = $_GET['f_date_end'] ?? '';

    // 1. Owner Name
    if (!empty($f_name)) {
        $sql .= " AND LOWER(\"﻿owner_name\") LIKE :name";
        $params[':name'] = '%' . strtolower($f_name) . '%';
    }
    
    // 2. Owner ID
    if (!empty($f_id)) {
            $sql .= " AND \"owner_id\" LIKE :id";
            $params[':id'] = '%' . $f_id . '%';
    }

    // 3. DOB
    if (!empty($f_dob)) {
        $sql .= " AND \"owner_dob\" = :dob";
        $params[':dob'] = $f_dob;
    }

    // 4. Amount
    if (!empty($f_amount)) {
        $sql .= " AND \"owner_due_amount\" = :amount";
        $params[':amount'] = $f_amount;
    }

    // 5. Transaction Date Range
    if (!empty($f_date_start)) {
        $sql .= " AND \"transaction_date\" >= DATE(:start_date)"; 
        $params[':start_date'] = $f_date_start;
    }
    if (!empty($f_date_end)) {
        $sql .= " AND \"transaction_date\" <= DATE(:end_date)";
        $params[':end_date'] = $f_date_end;
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
