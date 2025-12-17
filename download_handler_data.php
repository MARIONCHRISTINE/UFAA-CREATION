<?php
require_once 'db_data.php';
session_start();

if (!isset($_SESSION['user_id'])) {
    die("Unauthorized");
}

$dataTable = 'iceberg.adhoc.ufaadata2025';
$lettersTable = 'iceberg.adhoc.ufaaletters';

// Filters
$f_name = $_GET['f_name'] ?? '';
$f_id = $_GET['f_id'] ?? '';
$f_code = $_GET['f_code'] ?? '';

try {
    // Query data
    $sql = "SELECT * FROM $dataTable WHERE 1=1";
    $params = [];

    if (!empty($f_name)) {
        $sql .= " AND LOWER(\"owner_name\") LIKE LOWER(:name)";
        $params[':name'] = "%$f_name%";
    }

    if (!empty($f_id)) {
        $sql .= " AND \"owner_id\" = :id";
        $params[':id'] = $f_id;
    }

    if (!empty($f_code)) {
        $sql .= " AND \"owner_code\" = :code";
        $params[':code'] = $f_code;
    }

    $stmt = $pdo->prepare($sql);
    foreach ($params as $key => $val) {
        $stmt->bindValue($key, $val);
    }
    $stmt->execute();
    $data = $stmt->fetchAll();

    // Query letters
    $lettersStmt = $pdo->query("SELECT * FROM $lettersTable");
    $letters = $lettersStmt->fetchAll();
    $lettersMap = [];
    foreach ($letters as $letter) {
        $lettersMap[$letter['owner_code']] = $letter;
    }

} catch (Exception $e) {
    die("Error: " . $e->getMessage());
}

// Set headers for CSV download
header('Content-Type: text/csv; charset=utf-8');
header('Content-Disposition: attachment; filename=ufaa_data_' . date('Y-m-d_H-i-s') . '.csv');

$output = fopen('php://output', 'w');

// Write headers
fputcsv($output, [
    'Owner Code',
    'Owner Name',
    'Owner DOB',
    'Owner ID',
    'Owner MSISDN',
    'Transaction Date',
    'Transaction Time',
    'Owner Due Amount',
    'Letter Sent',
    'Letter Date',
    'Letter Ref No'
]);

// Write data
foreach ($data as $row) {
    $code = $row['owner_code'];
    $hasLetter = isset($lettersMap[$code]);
    
    fputcsv($output, [
        $row['owner_code'] ?? '',
        $row['owner_name'] ?? '',
        $row['owner_dob'] ?? '',
        $row['owner_id'] ?? '',
        $row['owner_msisdn'] ?? '',
        $row['transaction_date'] ?? '',
        $row['transaction_time'] ?? '',
        $row['owner_due_amount'] ?? '',
        $hasLetter ? 'Yes' : 'No',
        $hasLetter ? $lettersMap[$code]['letter_date'] : '',
        $hasLetter ? $lettersMap[$code]['letter_ref_no'] : ''
    ]);
}

fclose($output);
exit;
?>
