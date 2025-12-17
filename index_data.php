<?php
require_once 'db_data.php';
session_start();
if (!isset($_SESSION['user_id'])) {
    header("Location: login_data.php");
    exit;
}

$view = $_GET['view'] ?? 'upload';
$message = $_GET['message'] ?? '';
$error = $_GET['error'] ?? '';
?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>UFAA Data Manager 2025</title>
    <link rel="stylesheet" href="style_data.css">
    <style>
        .badge { padding: 0.25rem 0.5rem; border-radius: 0.375rem; font-size: 0.75rem; font-weight: 600; }
        .badge-success { background-color: #10b981; color: white; }
        .badge-warning { background-color: #f59e0b; color: white; }
        .modal {display:none; position:fixed; z-index:1000; left:0; top:0; width:100%; height:100%; background-color:rgba(0,0,0,0.5);}
        .modal-content {background-color:#fff; margin:10% auto; padding:2rem; width:90%; max-width:500px; border-radius:1rem;}
        .close {color:#aaa; float:right; font-size:28px; font-weight:bold; cursor:pointer;}
    </style>
</head>
<body>

<div class="container fade-in">
    <header style="position:relative;">
        <h1>UFAA Data Manager 2025</h1>
        <p>Upload, View, and Manage Datasets with Letter Tracking</p>
        <a href="logout_data.php" class="btn btn-secondary" style="position:absolute; top:0; right:0; padding:0.5rem 1rem; font-size:0.8rem;">Logout</a>
    </header>

    <div class="glass-card">
        <div class="nav-tabs">
            <a href="?view=upload" class="nav-link <?php echo $view === 'upload' ? 'active' : ''; ?>">Upload Data</a>
            <a href="?view=view_data" class="nav-link <?php echo $view === 'view_data' ? 'active' : ''; ?>">View Data</a>
        </div>

        <div class="content-area">
            <?php if ($message): ?>
                <div class="alert alert-success"><?php echo htmlspecialchars($message); ?></div>
            <?php endif; ?>
            <?php if ($error): ?>
                <div class="alert alert-error"><?php echo htmlspecialchars($error); ?></div>
            <?php endif; ?>

            <?php if ($view === 'upload'): ?>
                
                <h2 class="mb-1">Upload CSV File</h2>
                <p style="color: var(--text-muted); margin-bottom: 2rem;">Upload records. Each row gets auto-assigned owner_code (00000001, 00000002...)</p>
                
                <form action="upload_handler_data.php" method="POST" enctype="multipart/form-data" class="fade-in">
                    <div class="form-group">
                        <label for="csv_file" class="form-label">Choose CSV File (7 columns expected)</label>
                        <input type="file" name="csv_file" id="csv_file" class="form-control" accept=".csv" required>
                        <small style="color:var(--text-muted);">Expected columns: Owner Name, Owner DOB, Owner ID, MSISDN, Transaction Date, Transaction Time, Amount</small>
                    </div>
                    
                    <button type="submit" class="btn btn-primary">Upload Dataset</button>
                </form>

            <?php elseif ($view === 'view_data'): ?>
                
                <?php
                    $page = $_GET['page'] ?? 1;
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

                        $sql .= " LIMIT 100";
                        
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
                        $error = "Query Error: " . $e->getMessage();
                        $data = [];
                        $lettersMap = [];
                    }
                ?>

                <h2>View Data (<?php echo count($data); ?> records)</h2>

                <form method="GET" action="" class="mb-2">
                    <input type="hidden" name="view" value="view_data">
                    <div style="display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:1rem; margin-bottom:1rem;">
                        <div class="form-group mb-0">
                            <label class="form-label">Owner Name</label>
                            <input type="text" name="f_name" class="form-control" value="<?php echo htmlspecialchars($f_name); ?>">
                        </div>
                        <div class="form-group mb-0">
                            <label class="form-label">Owner ID</label>
                            <input type="text" name="f_id" class="form-control" value="<?php echo htmlspecialchars($f_id); ?>">
                        </div>
                        <div class="form-group mb-0">
                            <label class="form-label">Owner Code</label>
                            <input type="text" name="f_code" class="form-control" value="<?php echo htmlspecialchars($f_code); ?>">
                        </div>
                    </div>
                    <button type="submit" class="btn btn-primary">Filter</button>
                    <a href="?view=view_data" class="btn btn-secondary">Clear</a>
                    <a href="download_handler_data.php?<?php echo http_build_query($_GET); ?>" class="btn btn-secondary">Download CSV</a>
                </form>

                <div style="overflow-x:auto;">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>Owner Name</th>
                                <th>DOB</th>
                                <th>Owner ID</th>
                                <th>MSISDN</th>
                                <th>Trans. Date</th>
                                <th>Trans. Time</th>
                                <th>Amount</th>
                                <th>Letter Status</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            <?php foreach ($data as $row): ?>
                                <?php
                                    $code = $row['owner_code'];
                                    $hasLetter = isset($lettersMap[$code]);
                                ?>
                                <tr>
                                    <td><?php echo htmlspecialchars($row['owner_name'] ?? '-'); ?></td>
                                    <td><?php echo htmlspecialchars($row['owner_dob'] ?? '-'); ?></td>
                                    <td><?php echo htmlspecialchars($row['owner_id'] ?? '-'); ?></td>
                                    <td><?php echo htmlspecialchars($row['owner_msisdn'] ?? '-'); ?></td>
                                    <td><?php echo htmlspecialchars($row['transaction_date'] ?? '-'); ?></td>
                                    <td><?php echo htmlspecialchars($row['transaction_time'] ?? '-'); ?></td>
                                    <td><?php echo htmlspecialchars($row['owner_due_amount'] ?? '-'); ?></td>
                                    <td>
                                        <?php if ($hasLetter): ?>
                                            <span class="badge badge-success">✅ Sent</span><br>
                                            <small>Date: <?php echo $lettersMap[$code]['letter_date']; ?></small><br>
                                            <small>Ref: <?php echo $lettersMap[$code]['letter_ref_no']; ?></small>
                                        <?php else: ?>
                                            <span class="badge badge-warning">❌ Not Sent</span>
                                        <?php endif; ?>
                                    </td>
                                    <td>
                                        <?php if (!$hasLetter): ?>
                                            <button onclick="openLetterModal('<?php echo $code; ?>', '<?php echo addslashes($row['owner_name']); ?>')" class="btn btn-primary" style="padding:0.25rem 0.5rem; font-size:0.8rem;">Mark Sent</button>
                                        <?php endif; ?>
                                    </td>
                                </tr>
                            <?php endforeach; ?>
                        </tbody>
                    </table>
                </div>

            <?php endif; ?>
        </div>
    </div>
</div>

<!-- Letter Modal -->
<div id="letterModal" class="modal">
    <div class="modal-content">
        <span class="close" onclick="closeLetterModal()">&times;</span>
        <h2>Mark Letter as Sent</h2>
        <p id="modalOwnerName" style="color:var(--text-muted);"></p>
        <form id="letterForm">
            <input type="hidden" id="modal_owner_code" name="owner_code">
            <div class="form-group">
                <label class="form-label">Letter Date</label>
                <input type="date" id="letter_date" name="letter_date" class="form-control" required>
            </div>
            <div class="form-group">
                <label class="form-label">Letter Reference No</label>
                <input type="text" id="letter_ref_no" name="letter_ref_no" class="form-control" placeholder="e.g., LTR-2025-001" required>
            </div>
            <button type="submit" class="btn btn-primary">Submit</button>
            <button type="button" onclick="closeLetterModal()" class="btn btn-secondary">Cancel</button>
        </form>
        <div id="modalMessage" style="margin-top:1rem;"></div>
    </div>
</div>

<script>
function openLetterModal(code, name) {
    document.getElementById('letterModal').style.display = 'block';
    document.getElementById('modal_owner_code').value = code;
    document.getElementById('modalOwnerName').textContent = 'Owner: ' + name + ' (Code: ' + code + ')';
}

function closeLetterModal() {
    document.getElementById('letterModal').style.display = 'none';
    document.getElementById('letterForm').reset();
    document.getElementById('modalMessage').innerHTML = '';
}

document.getElementById('letterForm').addEventListener('submit', function(e) {
    e.preventDefault();
    const formData = new FormData(this);
    
    fetch('letter_handler_data.php', {
        method: 'POST',
        body: formData
    })
    .then(res => res.json())
    .then(data => {
        if (data.success) {
            document.getElementById('modalMessage').innerHTML = '<div class="alert alert-success">' + data.message + '</div>';
            setTimeout(() => {location.reload();}, 1500);
        } else {
            document.getElementById('modalMessage').innerHTML = '<div class="alert alert-error">' + data.message + '</div>';
        }
    })
    .catch(err => {
        document.getElementById('modalMessage').innerHTML = '<div class="alert alert-error">Request failed</div>';
    });
});
</script>

</body>
</html>
