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
                
                <h2 style="margin-bottom: 1rem;">Upload CSV File</h2>
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
                    // Fetch Data Logic with FILTERS
                    $page = $_GET['page'] ?? 1;
                    $fetchLimit = 100;
                    $tableName = 'iceberg.adhoc.ufaa_23203159';

                    // Capture Filter Inputs
                    $filterName = $_GET['filter_name'] ?? '';
                    $filterDate = $_GET['filter_date'] ?? '';

                    try {
                        // Build Query Dynamically
                        $sql = "SELECT * FROM $tableName WHERE 1=1";
                        $params = [];

                        // 1. Name Filter (Case Insensitive Partial Match)
                        if (!empty($filterName)) {
                            // Note: Trino usually requires LOWER() for case insensitivity
                            // Using standard SQL standard for pattern
                            $sql .= " AND LOWER(\"﻿owner_name\") LIKE :name";
                            $params[':name'] = '%' . strtolower($filterName) . '%';
                        }

                        // 2. Date Filter (Exact Match)
                        if (!empty($filterDate)) {
                            $sql .= " AND \"transaction_date\" = :date";
                            $params[':date'] = $filterDate; // Assuming YYYY-MM-DD input matches DB format
                        }

                        // Add Ordering (Optional, but good for UX)
                        // $sql .= " ORDER BY some_date DESC"; // Only if we are sure of column

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

                        // If rows exist, we get header metadata from keys
                        $totalRows = count($rows) . (count($rows) >= $fetchLimit ? "+" : "");

                    } catch (Exception $e) {
                        $error = "Database Error: " . $e->getMessage();
                        $rows = [];
                    }
                ?>

                <h2 style="margin-bottom: 1rem;">Data Viewer</h2>
                
                <!-- Search & Actions Bar -->
                <div class="glass-card" style="padding: 1.5rem; margin-bottom: 2rem;">
                    <form method="GET" action="index.php" style="display: flex; gap: 1rem; align-items: flex-end; flex-wrap: wrap;">
                        <input type="hidden" name="view" value="view_data">
                        
                        <div class="form-group" style="margin-bottom: 0; flex: 1; min-width: 200px;">
                            <label class="form-label">Owner Name</label>
                            <input type="text" name="filter_name" class="form-control" placeholder="Search by name..." value="<?php echo htmlspecialchars($filterName); ?>">
                        </div>
                        
                        <div class="form-group" style="margin-bottom: 0; flex: 1; min-width: 150px;">
                            <label class="form-label">Transaction Date</label>
                            <input type="date" name="filter_date" class="form-control" value="<?php echo htmlspecialchars($filterDate); ?>">
                        </div>

                        <div style="display: flex; gap: 0.5rem;">
                            <button type="submit" class="btn btn-primary">
                                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:0.5rem"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>
                                Filter
                            </button>
                            <a href="index.php?view=view_data" class="btn btn-secondary">Reset</a>
                        </div>
                    </form>
                </div>

                <div class="flex-row" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                    <p style="color: var(--text-muted);">
                        Showing: <strong><?php echo $totalRows; ?></strong> result(s)
                    </p>
                    
                    <!-- Download Button now passes current filters -->
                    <a href="download_handler.php?filter_name=<?php echo urlencode($filterName); ?>&filter_date=<?php echo urlencode($filterDate); ?>" class="btn btn-secondary">
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
                <div style="margin-top: 1.5rem; display: flex; gap: 0.5rem; justify-content: center;">
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

</body>
</html>
