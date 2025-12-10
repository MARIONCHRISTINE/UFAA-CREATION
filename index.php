<?php
require_once 'db.php';

// Simple Router / State Management
$view = $_GET['view'] ?? 'upload'; // Default to upload view
$message = '';
$error = '';

?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>UFAA Data Manager</title>
    <link rel="stylesheet" href="style.css">
    
    <!-- Optional: Tailwind for utility classes if user prefers, but we are using custom CSS as per prompt -->
</head>
<body>

<div class="container fade-in">
    <header>
        <h1>UFAA Data Manager</h1>
        <p>Upload, View, and Manage Massive Datasets</p>
    </header>

    <div class="glass-card">
        <!-- Navigation Tabs -->
        <div class="nav-tabs">
            <a href="?view=upload" class="nav-link <?php echo $view === 'upload' ? 'active' : ''; ?>">Upload Data</a>
            <a href="?view=view_data" class="nav-link <?php echo $view === 'view_data' ? 'active' : ''; ?>">View Data</a>
            <!-- <a href="?view=download" class="nav-link">Download</a> -->
        </div>

        <!-- Content Area -->
        <div class="content-area">
            <?php if ($view === 'upload'): ?>
                
                <h2 class="mb-1">Upload CSV File</h2>
                <p style="color: var(--text-muted); margin-bottom: 2rem;">Select a CSV file to upload into the database. Supports large files (200k+ rows).</p>
                
                <form action="upload_handler.php" method="POST" enctype="multipart/form-data" class="fade-in">
                    <div class="form-group">
                        <label for="csv_file" class="form-label">Choose CSV File</label>
                        <input type="file" name="csv_file" id="csv_file" class="form-control" accept=".csv" required>
                    </div>
                    
                    <button type="submit" class="btn btn-primary">
                        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right: 0.5rem;">
                            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                            <polyline points="17 8 12 3 7 8"></polyline>
                            <line x1="12" y1="3" x2="12" y2="15"></line>
                        </svg>
                        Upload Dataset
                    </button>
                </form>

            <?php elseif ($view === 'view_data'): ?>
                
                <?php
                    // Fetch Data Logic with EXTENDED FILTERS
                    $page = $_GET['page'] ?? 1;
                    $fetchLimit = 100;
                    $tableName = 'iceberg.adhoc.ufaa_23203159';

                    // Capture Filter Inputs
                    $f_name = $_GET['f_name'] ?? '';
                    $f_id   = $_GET['f_id'] ?? '';
                    $f_dob  = $_GET['f_dob'] ?? '';
                    $f_amount = $_GET['f_amount'] ?? '';
                    $f_date_start = $_GET['f_date_start'] ?? '';
                    $f_date_end   = $_GET['f_date_end'] ?? '';

                    try {
                        // Build Query Dynamically
                        $sql = "SELECT * FROM $tableName WHERE 1=1";
                        $params = [];

                        // 1. Owner Name (Bulk Search Support - Exact Phrases)
                        // User wants to paste "multiple names" but also "specific full name".
                        // Strategy: Split by NEWLINE or COMMA only. Do NOT split by SPACE.
                        // This allows pasting "Elias Juma" and finding exactly "Elias Juma".
                        if (!empty($f_name)) {
                            // Split by newline or comma. NOT by space.
                            $names = preg_split('/[\n\r,]+/', $f_name, -1, PREG_SPLIT_NO_EMPTY);
                            
                            if (count($names) > 0) {
                                // Trim spaces around each name (e.g. " Elias Juma " -> "Elias Juma")
                                $cleanedNames = array_map(function($n) { return preg_quote(strtolower(trim($n))); }, $names);
                                $regex = implode('|', $cleanedNames);
                                
                                // Handling potential BOM hidden char in column name if necessary, though we try to trust standard col names now.
                                // We use "owner_name" usually, but user saw "﻿owner_name" (BOM) earlier. 
                                // Ideally we use the clean name. Let's try standard first.
                                $sql .= " AND REGEXP_LIKE(LOWER(\"owner_name\"), :name_regex)";
                                $params[':name_regex'] = $regex;
                            }
                        }
                        
                        // 2. Owner ID (Bulk Search Support)
                        if (!empty($f_id)) {
                             $ids = preg_split('/[\s,\n\r]+/', $f_id, -1, PREG_SPLIT_NO_EMPTY);
                             if (count($ids) > 0) {
                                 $cleanedIds = array_map(function($i) { return preg_quote(trim($i)); }, $ids);
                                 $regexId = implode('|', $cleanedIds);
                                 
                                 $sql .= " AND REGEXP_LIKE(\"owner_id\", :id_regex)";
                                 $params[':id_regex'] = $regexId;
                             }
                        }

                        // 3. DOB (Exact)
                        if (!empty($f_dob)) {
                            $sql .= " AND \"owner_dob\" = :dob";
                            $params[':dob'] = $f_dob;
                        }

                        // 4. Amount (Flexible Numeric Match)
                        // Cast DB value to DOUBLE to match integers against stored decimals (100 == 100.00)
                        if (strlen($f_amount) > 0) { 
                            $sql .= " AND CAST(\"owner_due_amount\" AS DOUBLE) = :amount";
                            $params[':amount'] = (float)$f_amount; // PHP float handling
                        }

                        // 5. Transaction Date Range (Robust Casting)
                        if (!empty($f_date_start)) {
                            // Ensure we compare against a DATE type. 
                            $sql .= " AND CAST(\"transaction_date\" AS DATE) >= DATE(:start_date)"; 
                            $params[':start_date'] = $f_date_start;
                        }
                        if (!empty($f_date_end)) {
                            $sql .= " AND CAST(\"transaction_date\" AS DATE) <= DATE(:end_date)";
                            $params[':end_date'] = $f_date_end;
                        }

                        // Add Limit
                        $sql .= " LIMIT :limit";
                        $params[':limit'] = $fetchLimit;

                        $dataStmt = $pdo->prepare($sql);
                        
                        // Bind all params
                        foreach ($params as $key => $val) {
                            $dataStmt->bindValue($key, $val);
                        }
                        
                        $dataStmt->execute();
                        $rows = $dataStmt->fetchAll(PDO::FETCH_ASSOC);

                        $totalRows = count($rows) . (count($rows) >= $fetchLimit ? "+" : "");

                    } catch (Exception $e) {
                         // Fallback for column name error (BOM issue)
                         if (strpos($e->getMessage(), 'Column') !== false && strpos($e->getMessage(), 'owner_name') !== false) {
                            // Try again with BOM
                            // Ideally we fix the DB column, but for now we patch the query
                            try {
                                $sql = str_replace('"owner_name"', '"﻿owner_name"', $sql);
                                $dataStmt = $pdo->prepare($sql);
                                foreach ($params as $key => $val) $dataStmt->bindValue($key, $val);
                                $dataStmt->execute();
                                $rows = $dataStmt->fetchAll(PDO::FETCH_ASSOC);
                                $totalRows = count($rows) . (count($rows) >= $fetchLimit ? "+" : "");
                                $error = ""; // Clear error if fallback worked
                            } catch (Exception $e2) {
                                $error = "Database Error: " . $e2->getMessage();
                                $rows = [];
                            }
                         } else {
                            $error = "Database Error: " . $e->getMessage();
                            $rows = [];
                         }
                    }
                ?>

                <h2 class="mb-1">Data Viewer</h2>
                
                <!-- Expanded Search Bar -->
                <div class="glass-card" style="padding: 1.5rem; margin-bottom: 2rem;">
                    <form method="GET" action="index.php">
                        <input type="hidden" name="view" value="view_data">
                        
                        <div class="filter-grid">
                            
                            <!-- Row 1 -->
                            <div class="form-group mb-0 span-2">
                                <label class="form-label">Owner Name(s) <small class="text-muted">(Paste multiple separated by space or comma)</small></label>
                                <textarea name="f_name" class="form-control" rows="1" placeholder="Search by name(s)..."><?php echo htmlspecialchars($f_name); ?></textarea>
                            </div>
                            
                            <div class="form-group mb-0">
                                <label class="form-label">Owner ID(s) <small class="text-muted">(Paste multiple)</small></label>
                                <textarea name="f_id" class="form-control" rows="1" placeholder="Search IDs..."><?php echo htmlspecialchars($f_id); ?></textarea>
                            </div>

                            <div class="form-group mb-0">
                                <label class="form-label">Owner DOB</label>
                                <input type="text" name="f_dob" class="form-control" value="<?php echo htmlspecialchars($f_dob); ?>" placeholder="e.g. 13/04/1990">
                            </div>

                            <div class="form-group mb-0">
                                <label class="form-label">Due Amount</label>
                                <input type="number" step="0.01" name="f_amount" class="form-control" placeholder="100.00" value="<?php echo htmlspecialchars($f_amount); ?>">
                            </div>

                            <!-- Row 2: Date Range -->
                            <div class="form-group mb-0">
                                <label class="form-label">Trans. Date From</label>
                                <input type="date" name="f_date_start" class="form-control" value="<?php echo htmlspecialchars($f_date_start); ?>">
                            </div>

                            <div class="form-group mb-0">
                                <label class="form-label">Trans. Date To</label>
                                <input type="date" name="f_date_end" class="form-control" value="<?php echo htmlspecialchars($f_date_end); ?>">
                            </div>
                        </div>

                        <div class="actions-row">
                            <a href="index.php?view=view_data" class="btn btn-secondary">Reset Filters</a>
                            <button type="submit" class="btn btn-primary">
                                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:0.5rem"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>
                                Apply Filters
                            </button>
                        </div>
                    </form>
                </div>

                <div class="results-header">
                    <p style="color: var(--text-muted);">
                        Showing: <strong><?php echo $totalRows; ?></strong> result(s)
                    </p>
                    
                    <!-- Build Download Link with all params -->
                    <?php
                        $dlParams = http_build_query([
                            'f_name' => $f_name,
                            'f_id' => $f_id,
                            'f_dob' => $f_dob,
                            'f_amount' => $f_amount,
                            'f_date_start' => $f_date_start,
                            'f_date_end' => $f_date_end
                        ]);
                    ?>
                    <a href="download_handler.php?<?php echo $dlParams; ?>" class="btn btn-secondary">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:0.5rem"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="7 10 12 15 17 10"></polyline><line x1="12" y1="15" x2="12" y2="3"></line></svg>
                        Download Results
                    </a>
                </div>
                
                <?php if (!empty($message)): ?>
                    <div class="alert alert-success fade-in"><?php echo htmlspecialchars($message); ?></div>
                <?php endif; ?>

                <?php if (!empty($error) || (!empty($_GET['error']))): ?>
                    <div class="alert alert-error fade-in">
                        <?php echo htmlspecialchars($error ?: $_GET['error']); ?>
                    </div>
                <?php endif; ?>

                <!-- Data Table -->
                <div class="table-wrapper fade-in">
                    <?php if (!empty($rows)): ?>
                        <table>
                            <thead>
                                <tr>
                                    <?php 
                                        // Use keys from the first row of THIS page (or cached firstRow) for headers
                                        $headers = array_keys($rows[0]); 
                                        foreach ($headers as $th): 
                                    ?>
                                        <th><?php echo htmlspecialchars($th); ?></th>
                                    <?php endforeach; ?>
                                </tr>
                            </thead>
                            <tbody>
                                <?php foreach ($rows as $row): ?>
                                    <tr>
                                        <?php foreach ($row as $cell): ?>
                                            <td><?php echo htmlspecialchars(substr($cell, 0, 50)); ?><?php echo strlen($cell) > 50 ? '...' : ''; ?></td>
                                        <?php endforeach; ?>
                                    </tr>
                                <?php endforeach; ?>
                            </tbody>
                        </table>
                    <?php else: ?>
                        <div style="padding: 2rem; text-align: center; color: var(--text-muted);">
                            <?php if (isset($firstRow) && $firstRow === false): ?>
                                <em>Could not fetch data. Check if table '<?php echo $tableName; ?>' exists.</em>
                            <?php else: ?>
                                <em>No data found. Upload a CSV file to get started.</em>
                            <?php endif; ?>
                        </div>
                    <?php endif; ?>
                </div>

                <!-- Simple Pagination -->
                <div class="pagination-row">
                    <?php if ($page > 1): ?>
                        <a href="?view=view_data&page=<?php echo $page - 1; ?>" class="btn btn-secondary" style="padding: 0.5rem 1rem;">&laquo; Previous</a>
                    <?php endif; ?>
                    
                    <span style="display:flex; align-items:center; color: var(--text-muted);">
                        Page <?php echo $page; ?>
                    </span>

                    <?php if (isset($hasNextPage) && $hasNextPage): ?>
                        <a href="?view=view_data&page=<?php echo $page + 1; ?>" class="btn btn-secondary" style="padding: 0.5rem 1rem;">Next &raquo;</a>
                    <?php endif; ?>
                </div>
            
            <?php endif; ?> <!-- End of view_data elseif -->
        </div>
    </div>
</div>

</body>
</html>
