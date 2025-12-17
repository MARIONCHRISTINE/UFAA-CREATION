<?php
require_once 'db_data.php';
session_start();

// Check authentication
if (!isset($_SESSION['user_id'])) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Unauthorized']);
    exit;
}

$response = ['success' => false, 'message' => ''];

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $owner_code = trim($_POST['owner_code'] ?? '');
    $letter_date = trim($_POST['letter_date'] ?? '');
    $letter_ref = trim($_POST['letter_ref_no'] ?? '');
    
    // Validation
    if (empty($owner_code) || empty($letter_date) || empty($letter_ref)) {
        $response['message'] = 'All fields are required';
        echo json_encode($response);
        exit;
    }
    
    try {
        $stagingTable = 'hive.sre.ufaaletters';
        $prodTable = 'iceberg.adhoc.ufaaletters';
        
        // Check if owner_code exists in data table
        $checkData = $pdo->query("SELECT owner_code FROM iceberg.adhoc.ufaadata2025 WHERE owner_code = '$owner_code' LIMIT 1");
        if ($checkData->rowCount() === 0) {
            $response['message'] = 'Invalid owner code';
            echo json_encode($response);
            exit;
        }
        
        // Check if letter already exists
        $checkLetter = $pdo->query("SELECT owner_code FROM $prodTable WHERE owner_code = '$owner_code' LIMIT 1");
        if ($checkLetter->rowCount() > 0) {
            $response['message'] = 'Letter already recorded for this code';
            echo json_encode($response);
            exit;
        }
        
        // Clear staging
        try {
            $pdo->query("DELETE FROM $stagingTable WHERE owner_code = '$owner_code'");
        } catch (Exception $e) {
            // Ignore
        }
        
        // Insert into Hive
        $insertSQL = "INSERT INTO $stagingTable (owner_code, letter_sent, letter_date, letter_ref_no) 
                      VALUES ('$owner_code', 'Yes', DATE '$letter_date', '$letter_ref')";
        $pdo->query($insertSQL);
        
        // Promote to Iceberg
        $promoteSQL = "INSERT INTO $prodTable SELECT * FROM $stagingTable WHERE owner_code = '$owner_code'";
        $pdo->query($promoteSQL);
        
        $response['success'] = true;
        $response['message'] = 'Letter recorded successfully';
        
    } catch (Exception $e) {
        $response['message'] = 'Error: ' . $e->getMessage();
    }
}

echo json_encode($response);
?>
