<?php
// Gemeenschappelijke database helperfuncties

date_default_timezone_set('Europe/Amsterdam');

function default_db_config() {
    return [
        'host' => getenv('DB_HOST') ?: '127.0.0.1',
        'port' => getenv('DB_PORT') ?: '3306',
        'name' => getenv('DB_NAME') ?: 'brugmelding',
        'user' => getenv('DB_USER') ?: 'brugmelding_user',
        'pass' => getenv('DB_PASS') ?: '',
        'table' => 'bridge_status_history'
    ];
}

function load_db_config() {
    $config = default_db_config();
    $credentialsFile = __DIR__ . '/.db_credentials.php';

    if (file_exists($credentialsFile)) {
        $fileConfig = include $credentialsFile;
        if (is_array($fileConfig)) {
            $config = array_merge($config, $fileConfig);
        }
    }

    return $config;
}

function sanitize_table_name($name) {
    if (!preg_match('/^[A-Za-z0-9_]+$/', $name)) {
        die("Ongeldige tabelnaam voor statusgeschiedenis.\n");
    }

    return $name;
}

/**
 * Initialiseer MySQL database voor statusgeschiedenis.
 */
function init_db($config, $table)
{
    $dsn = sprintf(
        'mysql:host=%s;port=%s;dbname=%s;charset=utf8mb4',
        $config['host'],
        $config['port'],
        $config['name']
    );

    $pdo = new PDO($dsn, $config['user'], $config['pass']);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    try {
        $pdo->exec("SET time_zone = 'Europe/Amsterdam'");
    } catch (PDOException $e) {
        // Als de time zone niet beschikbaar is, gaan we verder met de default.
    }
    $pdo->exec(sprintf(
        'CREATE TABLE IF NOT EXISTS `%s` (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT,
            bridge_id VARCHAR(255) NOT NULL,
            status VARCHAR(32) NOT NULL,
            recorded_at VARCHAR(64) NOT NULL,
            opened_at DATETIME NULL,
            closed_at DATETIME NULL,
            opened_at_raw VARCHAR(128) NULL,
            closed_at_raw VARCHAR(128) NULL,
            seconds_since_previous_open INT UNSIGNED NULL,
            PRIMARY KEY (id),
            INDEX bridge_id_idx (bridge_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;',
        $table
    ));

    ensure_column($pdo, $table, 'opened_at', 'DATETIME NULL');
    ensure_column($pdo, $table, 'closed_at', 'DATETIME NULL');
    ensure_column($pdo, $table, 'opened_at_raw', 'VARCHAR(128) NULL');
    ensure_column($pdo, $table, 'closed_at_raw', 'VARCHAR(128) NULL');
    ensure_column($pdo, $table, 'seconds_since_previous_open', 'INT UNSIGNED NULL');

    return $pdo;
}

function ensure_column(PDO $pdo, $table, $column, $definition)
{
    $stmt = $pdo->prepare("SHOW COLUMNS FROM `{$table}` LIKE ?");
    $stmt->execute([$column]);

    if ($stmt->fetch(PDO::FETCH_ASSOC) === false) {
        $pdo->exec(sprintf('ALTER TABLE `%s` ADD COLUMN `%s` %s', $table, $column, $definition));
    }
}

function format_datetime_amsterdam(?string $value)
{
    if (!$value) {
        return null;
    }

    try {
        $dt = new DateTime($value, new DateTimeZone('Europe/Amsterdam'));
        return $dt->format(DateTime::ATOM);
    } catch (Exception $e) {
        return $value;
    }
}

/**
 * Sla een status op als deze gewijzigd is t.o.v. de laatst bekende.
 */
function log_status(?PDO $pdo, $bridgeId, $status, $timestamp, $table, $rawTimestamp = null)
{
    // Als de database niet beschikbaar is, slaan we de logging over zodat de hoofdscript kan doorgaan
    if ($pdo === null) {
        return;
    }

    $amsTz = new DateTimeZone('Europe/Amsterdam');

    try {
        $timestampObj = new DateTime($timestamp);
        $timestampObj->setTimezone($amsTz);
    } catch (Exception $e) {
        $timestampObj = new DateTime('now', $amsTz);
    }

    $recordedAt = $timestampObj->format('Y-m-d H:i:s');
    $isOpen = ($status === 'open');
    $rawTimestamp = $timestamp;

    $stmt = $pdo->prepare("SELECT id, status, opened_at, closed_at FROM `{$table}` WHERE bridge_id = ? ORDER BY id DESC LIMIT 1");
    $stmt->execute([$bridgeId]);
    $last = $stmt->fetch(PDO::FETCH_ASSOC);

    if ($isOpen) {
        if ($last && $last['status'] === 'open') {
            return; // geen wijziging ten opzichte van vorige status
        }

        $insert = $pdo->prepare(
            "INSERT INTO `{$table}` (bridge_id, status, recorded_at, opened_at, opened_at_raw, seconds_since_previous_open) VALUES (?, ?, ?, ?, ?, ?)"
        );
        $insert->execute([$bridgeId, $status, $recordedAt, $recordedAt, $rawTimestamp, 0]);
    } else {
        $openStmt = $pdo->prepare(
            "SELECT id, opened_at FROM `{$table}` WHERE bridge_id = ? AND opened_at IS NOT NULL AND closed_at IS NULL ORDER BY id DESC LIMIT 1"
        );
        $openStmt->execute([$bridgeId]);
        $openRow = $openStmt->fetch(PDO::FETCH_ASSOC);

        if ($openRow) {
            $openedAt = $openRow['opened_at'] ? new DateTime($openRow['opened_at']) : null;
            $secondsOpen = ($openedAt instanceof DateTime)
                ? max(0, $timestampObj->getTimestamp() - $openedAt->getTimestamp())
                : null;

            $update = $pdo->prepare(
                "UPDATE `{$table}` SET status = ?, recorded_at = ?, closed_at = ?, closed_at_raw = ?, seconds_since_previous_open = ? WHERE id = ?"
            );
            $update->execute([$status, $recordedAt, $recordedAt, $rawTimestamp, $secondsOpen, $openRow['id']]);
        } elseif (!$last || $last['status'] !== $status) {
            $insert = $pdo->prepare(
                "INSERT INTO `{$table}` (bridge_id, status, recorded_at) VALUES (?, ?, ?)"
            );
            $insert->execute([$bridgeId, $status, $recordedAt]);
        }
    }
}

/**
 * Haal de laatste statusmutaties op (nieuwste eerst).
 */
function fetch_history(PDO $pdo, $bridgeId, $table, $limit = 5, $hours = 24)
{
    $stmt = $pdo->prepare(
        "SELECT status, recorded_at, opened_at, closed_at, TIMESTAMPDIFF(SECOND, opened_at, COALESCE(closed_at, NOW())) AS seconds_since_previous_open FROM `{$table}` WHERE bridge_id = ? AND STR_TO_DATE(recorded_at, '%Y-%m-%d %H:%i:%s') >= DATE_SUB(NOW(), INTERVAL ? HOUR) ORDER BY id DESC LIMIT ?"
    );
    $stmt->bindValue(1, $bridgeId);
    $stmt->bindValue(2, (int)$hours, PDO::PARAM_INT);
    $stmt->bindValue(3, (int)$limit, PDO::PARAM_INT);
    $stmt->execute();

    $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
    $amsTz = new DateTimeZone('Europe/Amsterdam');

    foreach ($rows as &$row) {
        $openedRaw = $row['opened_at'];
        $closedRaw = $row['closed_at'];

        try {
            $openedDt = $openedRaw ? new DateTime($openedRaw) : null;
            if ($openedDt) {
                $openedDt->setTimezone($amsTz);
            }
        } catch (Exception $e) {
            $openedDt = null;
        }

        try {
            $closedDt = $closedRaw ? new DateTime($closedRaw) : null;
            if ($closedDt) {
                $closedDt->setTimezone($amsTz);
            }
        } catch (Exception $e) {
            $closedDt = null;
        }

        $row['opened_at'] = $openedDt ? $openedDt->format(DateTime::ATOM) : format_datetime_amsterdam($openedRaw);
        $row['closed_at'] = $closedDt ? $closedDt->format(DateTime::ATOM) : format_datetime_amsterdam($closedRaw);

        if ($openedDt) {
            $end = $closedDt ?: new DateTime('now', $amsTz);
            $row['seconds_since_previous_open'] = max(0, $end->getTimestamp() - $openedDt->getTimestamp());
        }

        try {
            $recordedDt = new DateTime($row['recorded_at']);
            $recordedDt->setTimezone($amsTz);
            $row['recorded_at'] = $recordedDt->format(DateTime::ATOM);
        } catch (Exception $e) {
            // Laat de originele recorded_at staan als deze niet parsebaar is.
        }
    }

    return $rows;
}
