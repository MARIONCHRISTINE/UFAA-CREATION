<?php
require_once 'auth_db_data.php';
session_start();

$error = '';
$success = '';

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $username = trim($_POST['username'] ?? '');
    $newPass  = $_POST['new_password'] ?? '';

    if (empty($username) || empty($newPass)) {
        $error = "Please fill in all fields.";
    } else {
        try {
            $stmt = $authDb->prepare("SELECT id FROM users WHERE username = :u");
            $stmt->execute([':u' => $username]);
            if ($stmt->fetch()) {
                $hash = password_hash($newPass, PASSWORD_DEFAULT);
                $update = $authDb->prepare("UPDATE users SET password = :p WHERE username = :u");
                $update->execute([':p' => $hash, ':u' => $username]);
                $success = "Password updated successfully. <a href='login_data.php'>Login now</a>";
            } else {
                $error = "User not found.";
            }
        } catch (Exception $e) {
            $error = "Error: " . $e->getMessage();
        }
    }
}
?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reset Password - UFAA Data Manager</title>
    <link rel="stylesheet" href="style_data.css">
    <style>
        body { display: flex; align-items: center; justify-content: center; min-height: 100vh; padding: 0; }
        .auth-card { width: 100%; max-width: 400px; padding: 2.5rem; }
    </style>
</head>
<body>
    <div class="glass-card auth-card fade-in">
        <h2 style="text-align: center; margin-bottom: 2rem;">Reset Password (Data)</h2>
        
        <?php if ($error): ?>
            <div class="alert alert-error"><?php echo htmlspecialchars($error); ?></div>
        <?php endif; ?>
        <?php if ($success): ?>
            <div class="alert alert-success"><?php echo $success; ?></div>
        <?php endif; ?>

        <form method="POST" action="">
            <div class="form-group">
                <label class="form-label">Username</label>
                <input type="text" name="username" class="form-control" placeholder="Enter your username" required>
            </div>
            <div class="form-group">
                <label class="form-label">New Password</label>
                <input type="password" name="new_password" class="form-control" placeholder="Enter new password" required>
            </div>
            <button type="submit" class="btn btn-primary" style="width: 100%;">Update Password</button>
        </form>
        
        <p style="text-align: center; margin-top: 1.5rem; color: var(--text-muted);">
            <a href="login_data.php" style="color: var(--text-muted);">Back to Login</a>
        </p>
    </div>
</body>
</html>
