<?php
require_once 'auth_db_data.php';
session_start();

$error = '';
$success = '';

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $username = trim($_POST['username'] ?? '');
    $password = $_POST['password'] ?? '';
    $confirm  = $_POST['confirm_password'] ?? '';

    if (empty($username) || empty($password)) {
        $error = "Please fill in all fields.";
    } elseif ($password !== $confirm) {
        $error = "Passwords do not match.";
    } else {
        try {
            // Check if user exists
            $stmt = $authDb->prepare("SELECT COUNT(*) FROM users WHERE username = :u");
            $stmt->execute([':u' => $username]);
            if ($stmt->fetchColumn() > 0) {
                $error = "Username already taken.";
            } else {
                // Insert User
                $hash = password_hash($password, PASSWORD_DEFAULT);
                $stmt = $authDb->prepare("INSERT INTO users (username, password) VALUES (:u, :p)");
                $stmt->execute([':u' => $username, ':p' => $hash]);
                
                $success = "Registration successful! <a href='login_data.php'>Login here</a>";
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
    <title>Register - UFAA Data Manager</title>
    <link rel="stylesheet" href="style_data.css">
    <style>
        body { display: flex; align-items: center; justify-content: center; min-height: 100vh; padding: 0; }
        .auth-card { width: 100%; max-width: 400px; padding: 2.5rem; }
    </style>
</head>
<body>
    <div class="glass-card auth-card fade-in">
        <h2 style="text-align: center; margin-bottom: 2rem;">Create Account (Data)</h2>
        
        <?php if ($error): ?>
            <div class="alert alert-error"><?php echo htmlspecialchars($error); ?></div>
        <?php endif; ?>
        <?php if ($success): ?>
            <div class="alert alert-success"><?php echo $success; // Allow HTML link ?></div>
        <?php endif; ?>

        <form method="POST" action="">
            <div class="form-group">
                <label class="form-label">Username</label>
                <input type="text" name="username" class="form-control" required autofocus>
            </div>
            <div class="form-group">
                <label class="form-label">Password</label>
                <input type="password" name="password" class="form-control" required>
            </div>
            <div class="form-group">
                <label class="form-label">Confirm Password</label>
                <input type="password" name="confirm_password" class="form-control" required>
            </div>
            <button type="submit" class="btn btn-primary" style="width: 100%;">Register</button>
        </form>
        
        <p style="text-align: center; margin-top: 1.5rem; color: var(--text-muted);">
            Already have an account? <a href="login_data.php" style="color: var(--primary-color);">Login</a>
        </p>
    </div>
</body>
</html>
