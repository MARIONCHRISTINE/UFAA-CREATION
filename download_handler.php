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

    // 1. Owner Name (Bulk Regex - Exact Phrase)
    if (!empty($f_name)) {
        // Split by Newline or Comma only (keep spaces)
        $names = preg_split('/[\n\r,]+/', $f_name, -1, PREG_SPLIT_NO_EMPTY);
        if (count($names) > 0) {
            $cleanedNames = array_map(function($n) { return preg_quote(strtolower(trim($n))); }, $names);
            $regex = implode('|', $cleanedNames);
            $sql .= " AND REGEXP_LIKE(LOWER(\"﻿owner_name\"), :name_regex)";
            $params[':name_regex'] = $regex;
        }
    }
    
    // 2. Owner ID (Bulk Search)
    if (!empty($f_id)) {
         $ids = preg_split('/[\s,\n\r]+/', $f_id, -1, PREG_SPLIT_NO_EMPTY);
         if (count($ids) > 0) {
             $cleanedIds = array_map(function($i) { return preg_quote(trim($i)); }, $ids);
             $regexId = implode('|', $cleanedIds);
             
             $sql .= " AND REGEXP_LIKE(\"owner_id\", :id_regex)";
             $params[':id_regex'] = $regexId;
         }
    }

    // 3. DOB
    if (!empty($f_dob)) {
        $sql .= " AND \"owner_dob\" = :dob";
        $params[':dob'] = $f_dob;
    }

    // 4. Amount (Flexible Numeric)
    if (strlen($f_amount) > 0) {
        $sql .= " AND CAST(\"owner_due_amount\" AS DOUBLE) = :amount";
        $params[':amount'] = (float)$f_amount;
    }

    // 5. Transaction Date Range
    if (!empty($f_date_start)) {
        $sql .= " AND CAST(\"transaction_date\" AS DATE) >= DATE(:start_date)"; 
        $params[':start_date'] = $f_date_start;
    }
    if (!empty($f_date_end)) {
        $sql .= " AND CAST(\"transaction_date\" AS DATE) <= DATE(:end_date)";
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
