<?php

header('Content-Type: text/html; charset=utf-8');           // Força UTF-8
mb_internal_encoding('UTF-8');                               // Strings em UTF-8
date_default_timezone_set('America/Sao_Paulo');              // Fuso horário

ini_set('display_errors', '1');                              // Mostrar erros
ini_set('display_startup_errors', '1');                      // Mostrar erros de startup
error_reporting(E_ALL);                                      // Mostrar tudo

$DB_HOST = 'localhost';                                      // Host MySQL
$DB_USER = 'root';                                           // Usuário MySQL
$DB_PASS = '';                                   // Senha MySQL
$DB_NAME = 'cloudwalk';                                      // Banco

$CSV_CHECKOUT_1 = 'https://raw.githubusercontent.com/thais-menezes/monitoring/main/checkout_1.csv'; // CSV checkout_1
$CSV_CHECKOUT_2 = 'https://raw.githubusercontent.com/thais-menezes/monitoring/main/checkout_2.csv'; // CSV checkout_2
$CSV_AUTH_CODES = 'https://raw.githubusercontent.com/everton-cw/monitoring_test/main/transactions_auth_codes.csv'; // CSV auth_codes
$CSV_TX         = 'https://raw.githubusercontent.com/everton-cw/monitoring_test/main/transactions.csv';            // CSV transactions

$mysqli = mysqli_connect($DB_HOST, $DB_USER, $DB_PASS, $DB_NAME); // Conecta MySQL
if (!$mysqli) { die('Erro MySQL: ' . mysqli_connect_error()); }   // Para se falhar
mysqli_set_charset($mysqli, 'utf8mb4');                           // Charset conexão

// ----------------------- Utilitários ----------------------- //
function http_get($url, $timeout = 30) {                          // Baixa um URL
    $ctx = stream_context_create(['http' => ['timeout' => $timeout]]); // Contexto
    $data = @file_get_contents($url, false, $ctx);                 // Tenta baixar
    if ($data !== false) { return $data; }                         // OK
    if (function_exists('curl_init')) {                            // Fallback cURL
        $ch = curl_init($url);                                     // Init
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);            // Retorna string
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, $timeout);        // Timeout connect
        curl_setopt($ch, CURLOPT_TIMEOUT, $timeout);               // Timeout total
        $out = curl_exec($ch);                                     // Exec
        curl_close($ch);                                           // Close
        if ($out !== false) { return $out; }                       // OK
    }
    return false;                                                  // Falhou
}

function is_status_token($v) {                                     // Valida status
    $s = strtolower(trim($v));                                     // Normaliza
    return in_array($s, ['approved','failed','denied','reversed'], true); // Lista
}

function parse_datetime($v) {                                      // Normaliza data/hora p/ minuto
    $v = trim((string)$v);                                         // Trim
    if ($v === '') { return null; }                                // Vazio
    $v = preg_replace('/[\"\\\']/', '', $v);                       // Remove aspas
    $t = strtotime($v);                                            // Converte
    if ($t !== false && $t > 0) { return date('Y-m-d H:i:00', $t); } // Formata
    return null;                                                   // Sem parse
}

function normalize_header($h) {                                    // Normaliza cabeçalho
    $h = strtolower(trim($h));                                     // Lower/trim
    $h = preg_replace('/^\xEF\xBB\xBF/', '', $h);                  // Remove BOM
    $h = str_replace(['-', ' '], ['_', '_'], $h);                  // Troca separadores
    return $h;                                                     // Retorna
}

function strip_bom($s){                                            // Remove BOM do início
    if (substr($s,0,3) === "\xEF\xBB\xBF") { return substr($s,3); } // Corta
    return $s;                                                     // Retorna original
}

function detect_delimiter($line){                                  // Detecta delimitador provável
    $candidates = [',',';','\t','|'];                              // Opções
    $best = ','; $bestCount = 0;                                   // Padrão
    foreach ($candidates as $d){                                   // Loop
        $cnt = substr_count($line, $d);                            // Conta
        if ($cnt > $bestCount){ $best = $d; $bestCount = $cnt; }   // Atualiza
    }
    return $best;                                                  // Melhor
}

function csv_to_lines($csv) {                                      // Converte CSV em linhas
    $csv = strip_bom(trim($csv));                                  // Limpa + BOM
    if ($csv === '') { return []; }                                // Vazio
    return preg_split('/\r\n|\r|\n/', $csv);                       // Explode
}

function parse_row($line, $delim){                                 // Faz parse de linha
    return str_getcsv($line, $delim);                              // Usa str_getcsv
}

// -------------------- Schema & Settings -------------------- //
function ensure_schema($mysqli) {                                   // Recria schema
    mysqli_query($mysqli, "DROP TABLE IF EXISTS alerts_log");      // Drop
    mysqli_query($mysqli, "DROP TABLE IF EXISTS transactions_minute_agg");
    mysqli_query($mysqli, "DROP TABLE IF EXISTS transactions_raw");
    mysqli_query($mysqli, "DROP TABLE IF EXISTS auth_codes");
    mysqli_query($mysqli, "DROP TABLE IF EXISTS checkout_hours");
    mysqli_query($mysqli, "DROP TABLE IF EXISTS settings");

    mysqli_query($mysqli, "CREATE TABLE settings (
        k VARCHAR(64) PRIMARY KEY,
        v VARCHAR(255) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");                      // settings

    mysqli_query($mysqli, "CREATE TABLE auth_codes (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        ts DATETIME NULL,
        code VARCHAR(64) NOT NULL,
        status ENUM('approved','failed','denied','reversed') NOT NULL,
        KEY idx_code (code),
        KEY idx_ts (ts),
        KEY idx_code_ts (code, ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");                      // auth_codes

    mysqli_query($mysqli, "CREATE TABLE transactions_raw (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        ts DATETIME NOT NULL,
        amount DECIMAL(12,2) DEFAULT NULL,
        qty INT NOT NULL DEFAULT 1,
        status ENUM('approved','failed','denied','reversed') NULL,
        raw_status VARCHAR(32) DEFAULT NULL,
        auth_code VARCHAR(64) DEFAULT NULL,
        source_file VARCHAR(64) DEFAULT NULL,
        KEY idx_ts (ts),
        KEY idx_status (status),
        KEY idx_auth (auth_code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");                      // transactions_raw

    mysqli_query($mysqli, "CREATE TABLE transactions_minute_agg (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        minute DATETIME NOT NULL,
        approved INT NOT NULL DEFAULT 0,
        failed INT NOT NULL DEFAULT 0,
        denied INT NOT NULL DEFAULT 0,
        reversed INT NOT NULL DEFAULT 0,
        total INT NOT NULL DEFAULT 0,
        UNIQUE KEY uniq_minute (minute)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");                      // agregados

    mysqli_query($mysqli, "CREATE TABLE checkout_hours (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        src VARCHAR(16) NOT NULL,
        hour TINYINT NOT NULL,
        today INT NOT NULL,
        yesterday INT NOT NULL,
        avg_other INT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_src_hour (src, hour)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");                      // checkout

    mysqli_query($mysqli, "CREATE TABLE alerts_log (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        minute DATETIME NOT NULL,
        type ENUM('failed','denied','reversed') NOT NULL,
        message VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_minute_type (minute, type),
        KEY idx_minute (minute),
        KEY idx_type (type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");                      // alerts_log

    seed_default_settings($mysqli);                                 // Defaults
}

function seed_default_settings($mysqli){                            // Preenche defaults
    $defaults = [
        'ALERT_BASE_WINDOW_MINUTES' => '120',
        'ALERT_SIGMA_MULTIPLIER' => '4.5',
        'ALERT_RATE_MULTIPLIER' => '2.5',
        'ALERT_MIN_ABS_COUNT' => '20',
        'ALERT_BASE_MEAN_MIN' => '1.0',
        'ALERT_MIN_TOTAL_IN_MINUTE' => '20',
        'ALERT_REQUIRE_CONSECUTIVE' => '2',
        'ALERT_COOLDOWN_MINUTES' => '15',
    ];
    foreach ($defaults as $k=>$v){
        $ks = mysqli_real_escape_string($mysqli, $k);               // Escapa k
        $vs = mysqli_real_escape_string($mysqli, $v);               // Escapa v
        mysqli_query($mysqli, "INSERT INTO settings (k,v) VALUES ('$ks','$vs')
                               ON DUPLICATE KEY UPDATE v=VALUES(v)"); // Upsert
    }
}

function get_setting($mysqli, $key, $fallback){                     // Lê setting
    $ks = mysqli_real_escape_string($mysqli, $key);                 // Escapa
    $res = mysqli_query($mysqli, "SELECT v FROM settings WHERE k='$ks' LIMIT 1"); // Query
    if ($res && ($r = mysqli_fetch_assoc($res))){                   // Se achou
        mysqli_free_result($res);                                   // Libera
        return $r['v'];                                             // Retorna
    }
    return $fallback;                                               // Padrão
}

function get_alert_config($mysqli){                                 // Config alerta
    return [
        'win' => (int)get_setting($mysqli,'ALERT_BASE_WINDOW_MINUTES', '120'),
        'sigma' => (float)get_setting($mysqli,'ALERT_SIGMA_MULTIPLIER', '4.5'),
        'rate' => (float)get_setting($mysqli,'ALERT_RATE_MULTIPLIER', '2.5'),
        'min_abs' => (int)get_setting($mysqli,'ALERT_MIN_ABS_COUNT', '20'),
        'base_min' => (float)get_setting($mysqli,'ALERT_BASE_MEAN_MIN', '1.0'),
        'min_total' => (int)get_setting($mysqli,'ALERT_MIN_TOTAL_IN_MINUTE', '20'),
        'consecutive' => (int)get_setting($mysqli,'ALERT_REQUIRE_CONSECUTIVE', '2'),
        'cooldown' => (int)get_setting($mysqli,'ALERT_COOLDOWN_MINUTES', '15'),
    ];
}

// -------------------- Importação -------------------- //
function import_checkout($mysqli, $csv, $src) {                     // Importa checkout
    $lines = csv_to_lines($csv);                                    // Linhas
    if (empty($lines)) { return 'Arquivo vazio: ' . $src; }         // Vazio
    $delim = detect_delimiter($lines[0]);                           // Delimitador
    $start = 0;                                                     // Início
    $hdr = parse_row($lines[0], $delim);                            // Cabeçalho
    if (!empty($hdr) && preg_grep('/hour/i', $hdr)) { $start = 1; } // Tem header
    mysqli_query($mysqli, "DELETE FROM checkout_hours WHERE src='" . mysqli_real_escape_string($mysqli, $src) . "'"); // Limpa
    $ins = mysqli_prepare($mysqli, "INSERT INTO checkout_hours (src, hour, today, yesterday, avg_other) VALUES (?,?,?,?,?)"); // Prepare
    if (!$ins) { return 'Erro prepare checkout: ' . mysqli_error($mysqli); } // Erro
    for ($i = $start; $i < count($lines); $i++) {                   // Loop
        $row = parse_row($lines[$i], $delim);                       // Parse
        if (count($row) < 4) { continue; }                          // Linha curta
        $hour  = (int)trim($row[0]);                                // Hora
        $today = (int)trim($row[1]);                                // Hoje
        $yday  = (int)trim($row[2]);                                // Ontem
        $avg   = (int)trim($row[3]);                                // Média
        mysqli_stmt_bind_param($ins, 'siiii', $src, $hour, $today, $yday, $avg); // Bind
        mysqli_stmt_execute($ins);                                  // Exec
    }
    mysqli_stmt_close($ins);                                        // Fecha
    return 'OK';                                                    // Sucesso
}

function import_auth_codes($mysqli, $csv) {                         // Importa auth_codes
    mysqli_query($mysqli, "TRUNCATE TABLE auth_codes");             // Limpa
    $lines = csv_to_lines($csv);                                    // Linhas
    if (empty($lines)) { return 'Arquivo vazio: auth_codes'; }      // Vazio
    $delim = detect_delimiter($lines[0]);                           // Delim
    $hdr = parse_row($lines[0], $delim);                            // Header
    $start = 0;                                                     // Início
    $map = ['ts' => -1, 'code' => -1, 'status' => -1];              // Mapeamento

    if (!empty($hdr)) {                                             // Se tem header
        $norm = array_map('normalize_header', $hdr);                // Normaliza
        foreach ($norm as $i => $h) {                               // Mapeia
            if (in_array($h, ['timestamp','ts','time','date','datetime'], true)) { $map['ts'] = $i; }
            if (in_array($h, ['auth_code','code','acquirer_code','authorization_code'], true)) { $map['code'] = $i; }
            if (in_array($h, ['status','result','auth_status'], true)) { $map['status'] = $i; }
        }
        if ($map['status'] !== -1 || $map['code'] !== -1 || $map['ts'] !== -1) { $start = 1; } // Header útil
    }

    $ins = mysqli_prepare($mysqli, "INSERT INTO auth_codes (ts, code, status) VALUES (?,?,?)"); // Prepare
    if (!$ins) { return 'Erro prepare auth_codes: ' . mysqli_error($mysqli); } // Erro
    $inserted = 0;                                                  // Contador

    for ($i = $start; $i < count($lines); $i++) {                   // Loop
        $cols = parse_row($lines[$i], $delim);                      // Parse linha
        if (count($cols) === 0) { continue; }                       // Vazia
        $ts = null; $code = null; $status = null;                   // Vars
        if ($map['status'] >= 0 || $map['code'] >= 0 || $map['ts'] >= 0) { // Se mapeado
            if ($map['status'] >= 0 && isset($cols[$map['status']]) && is_status_token($cols[$map['status']])) { $status = strtolower(trim($cols[$map['status']])); }
            if ($map['code'] >= 0 && isset($cols[$map['code']])) { $code = trim($cols[$map['code']]); }
            if ($map['ts'] >= 0 && isset($cols[$map['ts']])) { $ts = parse_datetime($cols[$map['ts']]); }
        } else {                                                    // Sem header confiável
            $c = array_map('trim', $cols);                          // Normaliza
            $status_found = -1;                                     // Marcador
            for ($k = 0; $k < count($c); $k++) { if (is_status_token($c[$k])) { $status = strtolower($c[$k]); $status_found = $k; break; } } // Acha status
            $ts_found = -1;                                         // Marcador
            for ($k = 0; $k < count($c); $k++) { if ($k === $status_found) { continue; } $try = parse_datetime($c[$k]); if ($try) { $ts = $try; $ts_found = $k; break; } } // Acha ts
            for ($k = 0; $k < count($c); $k++) { if ($k === $status_found || $k === $ts_found) { continue; } if ($c[$k] !== '') { $code = $c[$k]; break; } } // Acha code
        }
        if (!$status) { continue; }                                 // Ignora sem status
        mysqli_stmt_bind_param($ins, 'sss', $ts, $code, $status);   // Bind
        if (mysqli_stmt_execute($ins)) { $inserted++; }             // Exec + soma
    }

    mysqli_stmt_close($ins);                                        // Fecha
    return 'OK (' . $inserted . ' linhas)';                         // Retorna
}

function import_transactions($mysqli, $csv, $source_file) {         // Importa transações
    mysqli_query($mysqli, "DELETE FROM transactions_raw WHERE source_file='" . mysqli_real_escape_string($mysqli, $source_file) . "'"); // Limpa fonte
    $lines = csv_to_lines($csv);                                    // Linhas
    if (empty($lines)) { return 'Arquivo vazio: transactions'; }    // Vazio
    $delim = detect_delimiter($lines[0]);                           // Delim
    $hdr = parse_row($lines[0], $delim);                            // Header
    $start = 0;                                                     // Início
    $map = ['ts' => -1, 'auth_code' => -1, 'amount' => -1, 'status' => -1, 'qty' => -1]; // Map

    if (!empty($hdr)) {                                             // Se tem header
        $norm = array_map('normalize_header', $hdr);                // Normaliza
        foreach ($norm as $i => $h) {                               // Mapeia
            if (in_array($h, ['timestamp','ts','time','date','datetime'], true)) { $map['ts'] = $i; }
            if (in_array($h, ['auth_code','code','acquirer_code','authorization_code'], true)) { $map['auth_code'] = $i; }
            if (in_array($h, ['amount','value','price'], true)) { $map['amount'] = $i; }
            if (in_array($h, ['count','qty','quantity','total','n','num'], true)) { $map['qty'] = $i; }
            if (in_array($h, ['status','result'], true)) { $map['status'] = $i; }
        }
        if ($map['ts'] !== -1 || $map['auth_code'] !== -1 || $map['amount'] !== -1 || $map['status'] !== -1 || $map['qty'] !== -1) { $start = 1; } // Header útil
    }

    $ins = mysqli_prepare($mysqli, "INSERT INTO transactions_raw (ts, amount, qty, status, raw_status, auth_code, source_file) VALUES (?,?,?,?,?,?,?)"); // Prepare
    if (!$ins) { return 'Erro prepare transactions_raw: ' . mysqli_error($mysqli); } // Erro
    $sel = mysqli_prepare($mysqli, "SELECT status FROM auth_codes WHERE code=? ORDER BY ts DESC LIMIT 1"); // Resolve status por auth_code
    $inserted = 0;                                                  // Contador

    for ($i = $start; $i < count($lines); $i++) {                   // Loop
        $cols = parse_row($lines[$i], $delim);                      // Parse
        if (count($cols) === 0) { continue; }                       // Vazia
        $ts = null; $code = null; $amount = null; $qty = 1; $status = null; $raw_status = null; // Vars

        if ($map['ts'] >= 0 || $map['auth_code'] >= 0 || $map['amount'] >= 0 || $map['status'] >= 0 || $map['qty'] >= 0) { // Se mapeado
            if ($map['ts'] >= 0 && isset($cols[$map['ts']])) { $ts = parse_datetime($cols[$map['ts']]); }
            if ($map['auth_code'] >= 0 && isset($cols[$map['auth_code']])) { $code = trim($cols[$map['auth_code']]); }
            if ($map['amount'] >= 0 && isset($cols[$map['amount']])) { $val = str_replace([','], ['.'], $cols[$map['amount']]); $amount = is_numeric($val) ? (float)$val : null; }
            if ($map['qty'] >= 0 && isset($cols[$map['qty']])) { $qv = preg_replace('/[^0-9\-\.]/', '', $cols[$map['qty']]); if ($qv !== '' && is_numeric($qv)) { $qty = max(1, (int)$qv); } }
            if ($map['status'] >= 0 && isset($cols[$map['status']])) { $raw_status = strtolower(trim($cols[$map['status']])); if (is_status_token($raw_status)) { $status = $raw_status; } }
        } else {                                                    // Sem header confiável (fallback heurístico)
            $c = array_map('trim', $cols);                          // Normaliza
            $ts_found = -1;                                         // Índice ts
            for ($k = 0; $k < count($c); $k++) { $try = parse_datetime($c[$k]); if ($try) { $ts = $try; $ts_found = $k; break; } } // Tenta achar ts
            $status_found = -1;                                     // Índice status
            for ($k = 0; $k < count($c); $k++) { if ($k === $ts_found) { continue; } if (is_status_token($c[$k])) { $raw_status = strtolower($c[$k]); $status = $raw_status; $status_found = $k; break; } }
            $qty_found = -1;                                        // Índice qty
            for ($k = 0; $k < count($c); $k++) { if ($k === $ts_found || $k === $status_found) { continue; } $num = preg_replace('/[^0-9\-\.]/', '', $c[$k]); if ($num !== '' && ctype_digit($num)) { $qty = max(1, (int)$num); $qty_found = $k; break; } }
            $amount_found = -1;                                     // Índice amount
            for ($k = 0; $k < count($c); $k++) { if (in_array($k, [$ts_found,$status_found,$qty_found], true)) { continue; } $num = str_replace(',', '.', $c[$k]); if (is_numeric($num)) { $amount = (float)$num; $amount_found = $k; break; } }
            for ($k = 0; $k < count($c); $k++) { if (in_array($k, [$ts_found,$status_found,$qty_found,$amount_found], true)) { continue; } if ($c[$k] !== '') { $code = $c[$k]; break; } } // O restante vira auth_code
        }

        if (!$ts) { continue; }                                     // Sem ts válido, ignora

        if (!$status && $code) {                                    // Se sem status, tenta pelos auth_codes
            mysqli_stmt_bind_param($sel, 's', $code);               // Bind code
            mysqli_stmt_execute($sel);                              // Exec
            mysqli_stmt_bind_result($sel, $st);                     // Result
            mysqli_stmt_fetch($sel);                                // Fetch
            $status = $st ?? null;                                  // Status deduzido
            mysqli_stmt_reset($sel);                                // Reset
        }

        $status_valid = ($status && is_status_token($status)) ? $status : null; // Valida status
        mysqli_stmt_bind_param($ins, 'sdissss', $ts, $amount, $qty, $status_valid, $raw_status, $code, $source_file); // Bind insert
        if (mysqli_stmt_execute($ins)) { $inserted++; }             // Executa + soma
    }

    mysqli_stmt_close($sel);                                        // Fecha select
    mysqli_stmt_close($ins);                                        // Fecha insert
    return 'OK (' . $inserted . ' linhas)';                         // Retorna
}

// -------------------- Agregação -------------------- //
function rebuild_minute_agg($mysqli) {                              // Recria agregados
    mysqli_query($mysqli, "DELETE FROM transactions_minute_agg");   // Limpa
    $sql = "INSERT INTO transactions_minute_agg (minute, approved, failed, denied, reversed, total)
            SELECT DATE_FORMAT(ts, '%Y-%m-%d %H:%i:00') AS m,
                   SUM(CASE WHEN status='approved' THEN qty ELSE 0 END) AS approved,
                   SUM(CASE WHEN status='failed'   THEN qty ELSE 0 END) AS failed,
                   SUM(CASE WHEN status='denied'   THEN qty ELSE 0 END) AS denied,
                   SUM(CASE WHEN status='reversed' THEN qty ELSE 0 END) AS reversed,
                   SUM(CASE WHEN status IN ('approved','failed','denied','reversed') THEN qty ELSE 0 END) AS total
            FROM transactions_raw
            GROUP BY m
            ORDER BY m";
    mysqli_query($mysqli, $sql);                                    // Executa
}

// -------------------- Backfill -------------------- //
function backfill_auth_codes_from_transactions($mysqli) {           // Preenche auth_codes
    $cnt = 0;                                                       // Contador
    $res = mysqli_query($mysqli, "SELECT COUNT(*) AS c FROM auth_codes"); // Conta
    if ($res) { $row = mysqli_fetch_assoc($res); $cnt = (int)$row['c']; mysqli_free_result($res); } // Lê
    if ($cnt > 0) { return 'auth_codes já possui dados'; }          // Já tem
    $sql = "INSERT INTO auth_codes (ts, code, status)
            SELECT MAX(ts) AS ts, auth_code AS code,
                   SUBSTRING_INDEX(GROUP_CONCAT(status ORDER BY ts DESC SEPARATOR ','), ',', 1) AS status
            FROM transactions_raw
            WHERE auth_code IS NOT NULL AND auth_code <> ''
              AND status IN ('approved','failed','denied','reversed')
            GROUP BY auth_code";
    mysqli_query($mysqli, $sql);                                    // Executa
    $res2 = mysqli_query($mysqli, "SELECT COUNT(*) AS c FROM auth_codes"); // Conta
    $ins = 0; if ($res2) { $r2 = mysqli_fetch_assoc($res2); $ins = (int)$r2['c']; mysqli_free_result($res2); } // Lê
    return 'auth_codes preenchida: ' . $ins;                         // Retorna
}

// -------------------- Alertas -------------------- //
function decide_alerts_for_minute($mysqli, $minute) {               // Decide alertas
    $cfg = get_alert_config($mysqli);                               // Lê config
    $minute_s = mysqli_real_escape_string($mysqli, $minute);        // Escapa
    $from = date('Y-m-d H:i:00', strtotime($minute . ' -' . $cfg['win'] . ' minutes')); // Inicio janela
    $from_s = mysqli_real_escape_string($mysqli, $from);            // Escapa

    $q = "SELECT
            AVG(approved) AS ma_approved, STDDEV(approved) AS sd_approved,
            AVG(failed)   AS ma_failed,   STDDEV(failed)   AS sd_failed,
            AVG(denied)   AS ma_denied,   STDDEV(denied)   AS sd_denied,
            AVG(reversed) AS ma_reversed, STDDEV(reversed) AS sd_reversed,
            AVG(total)    AS ma_total,    STDDEV(total)    AS sd_total
          FROM transactions_minute_agg
          WHERE minute >= '$from_s' AND minute < '$minute_s'";
    $res = mysqli_query($mysqli, $q);                               // Executa
    $bl = $res ? mysqli_fetch_assoc($res) : null;                   // Baseline
    if ($res) { mysqli_free_result($res); }                         // Libera

    $cur = mysqli_fetch_assoc(mysqli_query($mysqli, "SELECT * FROM transactions_minute_agg WHERE minute='$minute_s' LIMIT 1")); // Linha atual
    $alerts = [];                                                   // Vetor alertas

    $total = isset($cur['total']) ? (int)$cur['total'] : 0;         // Total do minuto
    if ($total < $cfg['min_total']) { return [$cur, $bl, $alerts]; } // Ignora baixo volume

    foreach (['failed','denied','reversed'] as $k) {                // Para cada tipo
        $curv = isset($cur[$k]) ? (int)$cur[$k] : 0;                // Valor atual
        $ma = isset($bl['ma_'.$k]) ? (float)$bl['ma_'.$k] : 0.0;    // Média base
        $sd = isset($bl['sd_'.$k]) ? (float)$bl['sd_'.$k] : 0.0;    // Desvio base
        $ma_total = isset($bl['ma_total']) ? max(1.0, (float)$bl['ma_total']) : 1.0; // Média total
        $sigmaThr = $ma + $cfg['sigma'] * $sd;                      // Limite sigma
        $cur_rate = ($total > 0) ? ($curv / $total) : 0.0;          // Taxa atual
        $ma_rate  = ($ma_total > 0) ? ($ma / $ma_total) : 0.0;      // Taxa base

        $reasons = [];                                              // Motivos
        if ($sd > 0 && $ma >= $cfg['base_min'] && $curv >= $cfg['min_abs'] && $curv > $sigmaThr) {
            $reasons[] = "$k acima de ".$cfg['sigma']."σ (thr=".round($sigmaThr,2).")";
        }
        if ($curv >= $cfg['min_abs'] && $ma_rate > 0 && $cur_rate > $cfg['rate'] * $ma_rate) {
            $reasons[] = "$k taxa ".round($cur_rate*100,1)."&#37; > ".$cfg['rate']."x baseline ".round($ma_rate*100,1)."&#37;";
        }

        if (!empty($reasons) && $cfg['consecutive'] > 1) {          // Exige consecutivos?
            $need = $cfg['consecutive'] - 1;                        // Quantos anteriores
            $okRun = true;                                          // Flag
            $prevMin = $minute;                                     // Cursor
            for ($i = 0; $i < $need; $i++) {                        // Loop retroativo
                $prevMin = date('Y-m-d H:i:00', strtotime($prevMin . ' -1 minute')); // Minuto anterior
                $prev = mysqli_fetch_assoc(mysqli_query($mysqli, "SELECT * FROM transactions_minute_agg WHERE minute='".mysqli_real_escape_string($mysqli,$prevMin)."' LIMIT 1")); // Busca
                if (!$prev) { $okRun = false; break; }              // Falhou
                $prevTotal = (int)($prev['total'] ?? 0);            // Total
                $prevVal   = (int)($prev[$k] ?? 0);                 // Valor
                $prevRate = ($prevTotal>0)?($prevVal/$prevTotal):0.0; // Taxa
                $hitSigma = ($sd>0 && $ma>=$cfg['base_min'] && $prevVal>=$cfg['min_abs'] && $prevVal > $sigmaThr); // Sigma
                $hitRate  = ($prevVal>=$cfg['min_abs'] && $ma_rate>0 && $prevRate > $cfg['rate'] * $ma_rate);       // Taxa
                if (!($hitSigma || $hitRate)) { $okRun = false; break; } // Se não manter, falha
            }
            if (!$okRun) { $reasons = []; }                          // Sem cadeia, zera
        }

        if (!empty($reasons) && $cfg['cooldown'] > 0) {             // Aplica cooldown
            $coolFrom = date('Y-m-d H:i:00', strtotime($minute . ' -' . $cfg['cooldown'] . ' minutes')); // Janela
            $coolFrom_s = mysqli_real_escape_string($mysqli, $coolFrom); // Escapa
            $k_s = mysqli_real_escape_string($mysqli, $k);          // Escapa tipo
            $hasRecent = mysqli_fetch_assoc(mysqli_query($mysqli, "SELECT id FROM alerts_log WHERE type='$k_s' AND minute >= '$coolFrom_s' AND minute < '$minute_s' LIMIT 1")); // Já houve?
            if ($hasRecent) { $reasons = []; }                      // Se sim, suprime
        }

        if (!empty($reasons)) {                                     // Persistir no vetor
            $alerts[] = ['type'=>$k, 'reasons'=>$reasons, 'current'=>$curv, 'baseline_mean'=>$ma, 'baseline_sd'=>$sd];
        }
    }
    return [$cur, $bl, $alerts];                                    // Retorna trio
}

function log_alerts_for_minute($mysqli, $minute) {                  // Grava alertas
    list($cur, $bl, $alerts) = decide_alerts_for_minute($mysqli, $minute); // Calcula
    foreach ($alerts as $a) {                                       // Loop
        $type = $a['type'];                                         // Tipo
        $msg = $type . ' ' . implode(' | ', $a['reasons']);         // Mensagem
        $m = mysqli_real_escape_string($mysqli, $minute);           // Escapa minuto
        $t = mysqli_real_escape_string($mysqli, $type);             // Escapa tipo
        $mm = mysqli_real_escape_string($mysqli, $msg);             // Escapa msg
        mysqli_query($mysqli, "INSERT INTO alerts_log (minute, type, message) VALUES ('$m', '$t', '$mm')
                               ON DUPLICATE KEY UPDATE message=VALUES(message)"); // Upsert
    }
    return count($alerts);                                          // Qtde
}

// -------------------- Endpoints -------------------- //
$action = $_GET['action'] ?? 'dashboard';                           // Ação

if ($action === 'series') {                                         // Série transações
    header('Content-Type: application/json; charset=utf-8');        // JSON
    $statuses = isset($_GET['statuses']) ? explode(',', strtolower($_GET['statuses'])) : ['failed','denied','reversed']; // Status
    $statuses = array_values(array_intersect($statuses, ['approved','failed','denied','reversed'])); // Valida
    if (empty($statuses)) { $statuses = ['failed','denied','reversed']; } // Padrão
    $from = isset($_GET['from']) ? parse_datetime($_GET['from']) : null; // Periodo de
    $to   = isset($_GET['to'])   ? parse_datetime($_GET['to'])   : null; // Periodo até
    $last = isset($_GET['last']) ? max(1, min(2000, (int)$_GET['last'])) : 240; // Últimos
    $where = "1=1";                                                 // WHERE base
    if ($from) { $from_s = mysqli_real_escape_string($mysqli, date('Y-m-d H:i:00', strtotime($from))); $where .= " AND minute >= '$from_s'"; } // De
    if ($to)   { $to_s   = mysqli_real_escape_string($mysqli, date('Y-m-d H:i:00', strtotime($to)));   $where .= " AND minute <= '$to_s'"; }   // Até
    $sql = "SELECT minute, approved, failed, denied, reversed, total FROM transactions_minute_agg WHERE $where ORDER BY minute DESC"; // SQL
    if (!$from && !$to) { $sql .= " LIMIT $last"; }                 // Limita quando não há período
    $res = mysqli_query($mysqli, $sql);                             // Exec
    $rows_desc = [];                                                // Buffer reverso
    while ($res && ($r = mysqli_fetch_assoc($res))) { $rows_desc[] = $r; } // Coleta
    if ($res) { mysqli_free_result($res); }                         // Libera
    $rows = array_reverse($rows_desc);                              // Inverte ordem cronológica

    $roll = function($arr, $key, $win){                             // Função MA
        $out=[]; $sum=0.0; $q=[];
        for($i=0;$i<count($arr);$i++){
            $v=(int)$arr[$i][$key]; $q[]=$v; $sum+=$v;
            if(count($q)>$win){ $sum-=array_shift($q); }
            $out[]=(count($q)>0)?$sum/count($q):0.0;
        }
        return $out;
    };
    $win = 60;                                                      // Janela MA60
    $ma = [];                                                       // Resultado MA
    foreach (['approved','failed','denied','reversed'] as $k) { $ma[$k] = $roll($rows, $k, $win); } // Calcula
    echo json_encode(['rows'=>$rows, 'ma'=>$ma, 'statuses'=>$statuses], JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES); // Retorna
    exit;                                                           // Sai
}

if ($action === 'alerts') {                                         // Lista alertas
    header('Content-Type: application/json; charset=utf-8');        // JSON
    $types = isset($_GET['types']) ? explode(',', strtolower($_GET['types'])) : ['failed','denied','reversed']; // Tipos
    $types = array_values(array_intersect($types, ['failed','denied','reversed'])); // Valida
    if (empty($types)) { $types = ['failed','denied','reversed']; } // Padrão
    $from = isset($_GET['from']) ? parse_datetime($_GET['from']) : null; // De
    $to   = isset($_GET['to'])   ? parse_datetime($_GET['to'])   : null; // Até
    $last = isset($_GET['last']) ? max(1, min(2000, (int)$_GET['last'])) : null; // Últimos N
    $page = isset($_GET['page']) ? max(1, (int)$_GET['page']) : 1;  // Página
    $size = isset($_GET['size']) ? max(1, min(200, (int)$_GET['size'])) : 10; // Tamanho
    $where = " type IN ('" . implode("','", array_map(fn($x)=>mysqli_real_escape_string($mysqli,$x), $types)) . "') "; // WHERE
    if ($from) { $from_s = mysqli_real_escape_string($mysqli, date('Y-m-d H:i:00', strtotime($from))); $where .= " AND minute >= '$from_s'"; } // Filtro de
    if ($to)   { $to_s   = mysqli_real_escape_string($mysqli, date('Y-m-d H:i:00', strtotime($to)));   $where .= " AND minute <= '$to_s'"; } // Filtro até
    $rc = mysqli_query($mysqli, "SELECT COUNT(*) AS c FROM alerts_log WHERE $where"); // Contagem
    $total = 0; if ($rc) { $row = mysqli_fetch_assoc($rc); $total = (int)$row['c']; mysqli_free_result($rc); } // Lê
    if ($last && !$from && !$to) { $page = 1; $size = $last; }      // Ajuste
    $offset = ($page - 1) * $size;                                  // Offset
    $sql = "SELECT id, minute, type, message FROM alerts_log WHERE $where ORDER BY minute DESC, id DESC LIMIT $offset, $size"; // Consulta
    $res = mysqli_query($mysqli, $sql);                             // Exec
    $rows = [];                                                     // Buffer
    while ($res && ($r = mysqli_fetch_assoc($res))) { $rows[] = $r; } // Coleta
    if ($res) { mysqli_free_result($res); }                         // Libera
    echo json_encode(['rows'=>$rows, 'total'=>$total, 'page'=>$page, 'size'=>$size], JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES); // Retorna
    exit;                                                           // Sai
}

if ($action === 'alerts_markers') {                                 // Marcadores p/ anotar no gráfico
    header('Content-Type: application/json; charset=utf-8');        // JSON
    $from = isset($_GET['from']) ? parse_datetime($_GET['from']) : null; // De
    $to   = isset($_GET['to'])   ? parse_datetime($_GET['to'])   : null; // Até
    $last = isset($_GET['last']) ? max(1, min(2000, (int)$_GET['last'])) : 240; // Últimos N
    $where = "1=1";                                                 // WHERE
    if ($from) { $from_s = mysqli_real_escape_string($mysqli, date('Y-m-d H:i:00', strtotime($from))); $where .= " AND minute >= '$from_s'"; } // De
    if ($to)   { $to_s   = mysqli_real_escape_string($mysqli, date('Y-m-d H:i:00', strtotime($to)));   $where .= " AND minute <= '$to_s'"; } // Até
    if (!$from && !$to) { $where .= " AND minute >= DATE_SUB(NOW(), INTERVAL $last MINUTE)"; } // Janela padrão
    $res = mysqli_query($mysqli, "SELECT DISTINCT minute FROM alerts_log WHERE $where ORDER BY minute ASC"); // Distintos
    $rows = [];                                                     // Lista
    while ($res && ($r = mysqli_fetch_assoc($res))) { $rows[] = $r['minute']; } // Coleta
    if ($res) { mysqli_free_result($res); }                         // Libera
    echo json_encode(['minutes'=>$rows], JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES); // Retorna
    exit;                                                           // Sai
}

if ($action === 'series_checkout') {                                // Série checkout
    header('Content-Type: application/json; charset=utf-8');        // JSON
    $src = $_GET['src'] ?? null;                                    // Fonte
    $where = $src ? "WHERE src='" . mysqli_real_escape_string($mysqli, $src) . "'" : ""; // WHERE
    $res = mysqli_query($mysqli, "SELECT src, hour, today, yesterday, avg_other FROM checkout_hours $where ORDER BY src, hour ASC"); // Query
    $rows = [];                                                     // Lista
    while ($res && ($r = mysqli_fetch_assoc($res))) { $rows[] = $r; } // Coleta
    if ($res) { mysqli_free_result($res); }                         // Libera
    echo json_encode(['rows'=>$rows], JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES); // Retorna
    exit;                                                           // Sai
}

if ($action === 'ingest') {                                         // Ingestão realtime
    header('Content-Type: application/json; charset=utf-8');        // JSON
    $data = json_decode(file_get_contents('php://input'), true);    // Lê JSON
    if (!is_array($data)) { echo json_encode(['ok'=>false,'error'=>'JSON inválido']); exit; } // Valida
    $ts = isset($data['ts']) ? parse_datetime($data['ts']) : date('Y-m-d H:i:00'); // ts
    $amount = isset($data['amount']) ? (float)$data['amount'] : null; // amount
    $qty = isset($data['qty']) ? max(1,(int)$data['qty']) : 1;      // qty
    $status_in = isset($data['status']) ? strtolower(trim($data['status'])) : null; // status informado
    $auth_code = isset($data['auth_code']) ? trim($data['auth_code']) : null;       // auth_code
    $status = is_status_token($status_in) ? $status_in : null;      // status válido?
    if (!$status && $auth_code) {                                   // Deduz por código
        $stmt = mysqli_prepare($mysqli, "SELECT status FROM auth_codes WHERE code=? ORDER BY ts DESC LIMIT 1"); // Prepare
        mysqli_stmt_bind_param($stmt, 's', $auth_code);             // Bind
        mysqli_stmt_execute($stmt);                                 // Exec
        mysqli_stmt_bind_result($stmt, $st);                        // Bind result
        mysqli_stmt_fetch($stmt);                                   // Fetch
        $status = $st ?? null;                                      // Deduzido
        mysqli_stmt_close($stmt);                                   // Fecha
    }
    $ins = mysqli_prepare($mysqli, "INSERT INTO transactions_raw (ts, amount, qty, status, raw_status, auth_code, source_file) VALUES (?,?,?,?,?,?,?)"); // Prepare
    $src = 'api';                                                   // Fonte
    mysqli_stmt_bind_param($ins, 'sdissss', $ts, $amount, $qty, $status, $status_in, $auth_code, $src); // Bind
    mysqli_stmt_execute($ins);                                      // Exec
    mysqli_stmt_close($ins);                                        // Fecha
    $minute = date('Y-m-d H:i:00', strtotime($ts));                 // Minuto
    $minute_s = mysqli_real_escape_string($mysqli, $minute);        // Escapa
    mysqli_query($mysqli, "INSERT INTO transactions_minute_agg (minute, approved, failed, denied, reversed, total)
                           VALUES ('$minute_s',0,0,0,0,0)
                           ON DUPLICATE KEY UPDATE minute=VALUES(minute)");       // Garante linha
    if ($status) { $field = $status; mysqli_query($mysqli, "UPDATE transactions_minute_agg SET `$field` = `$field`+$qty, total = total+$qty WHERE minute='$minute_s'"); } // Atualiza
    $n = log_alerts_for_minute($mysqli, $minute);                   // Grava alertas
    list($cur, $bl, $alerts) = decide_alerts_for_minute($mysqli, $minute); // Decide
    echo json_encode(['ok'=>true,'minute'=>$minute,'current'=>$cur,'baseline'=>$bl,'alert'=>count($alerts)>0,'alerts'=>$alerts], JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES); // Retorna
    exit;                                                           // Sai
}

if ($action === 'run_alerts') {                                     // Varrer alertas
    $res = mysqli_query($mysqli, "SELECT minute FROM transactions_minute_agg ORDER BY minute ASC"); // Minutos
    $n=0;                                                           // Contador
    while ($res && ($r = mysqli_fetch_assoc($res))) { $n += log_alerts_for_minute($mysqli, $r['minute']); } // Gera alertas
    if ($res) { mysqli_free_result($res); }                         // Libera
    echo '<!doctype html><meta charset="utf-8"><body style="font-family:system-ui,Segoe UI,Arial"><h3>Varredura concluída.</h3><p>Alertas registrados: '.(int)$n.'</p><p><a href="cloudwalk.php">Voltar</a></p></body>'; // Saída simples
    exit;                                                           // Sai
}

if ($action === 'settings_get') {                                   // Busca settings
    header('Content-Type: application/json; charset=utf-8');        // JSON
    $cfg = get_alert_config($mysqli);                               // Lê
    echo json_encode(['ok'=>true,'cfg'=>$cfg], JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES); // Retorna
    exit;                                                           // Sai
}

if ($action === 'settings_save' && $_SERVER['REQUEST_METHOD'] === 'POST') { // Salva settings
    header('Content-Type: application/json; charset=utf-8');        // JSON
    $body = json_decode(file_get_contents('php://input'), true);    // Lê
    if (!is_array($body)) { echo json_encode(['ok'=>false,'error'=>'JSON inválido']); exit; } // Valida
    $map = [                                                        // Mapa campos
        'win'=>'ALERT_BASE_WINDOW_MINUTES',
        'sigma'=>'ALERT_SIGMA_MULTIPLIER',
        'rate'=>'ALERT_RATE_MULTIPLIER',
        'min_abs'=>'ALERT_MIN_ABS_COUNT',
        'base_min'=>'ALERT_BASE_MEAN_MIN',
        'min_total'=>'ALERT_MIN_TOTAL_IN_MINUTE',
        'consecutive'=>'ALERT_REQUIRE_CONSECUTIVE',
        'cooldown'=>'ALERT_COOLDOWN_MINUTES',
    ];
    foreach ($map as $in=>$key){                                    // Salva
        if (isset($body[$in])){
            $val = (string)$body[$in];                              // Valor
            $ks = mysqli_real_escape_string($mysqli, $key);         // Escapa k
            $vs = mysqli_real_escape_string($mysqli, $val);         // Escapa v
            mysqli_query($mysqli, "INSERT INTO settings (k,v) VALUES ('$ks','$vs')
                                   ON DUPLICATE KEY UPDATE v=VALUES(v)"); // Upsert
        }
    }
    echo json_encode(['ok'=>true]);                                 // OK
    exit;                                                           // Sai
}

if ($action === 'load') {                                           // Importar CSVs
    ensure_schema($mysqli);                                         // Recria schema
    $c1 = http_get($GLOBALS['CSV_CHECKOUT_1']);                     // Baixa checkout_1
    $c2 = http_get($GLOBALS['CSV_CHECKOUT_2']);                     // Baixa checkout_2
    $ac = http_get($GLOBALS['CSV_AUTH_CODES']);                     // Baixa auth_codes
    $tx = http_get($GLOBALS['CSV_TX']);                             // Baixa transactions
    if ($c1 === false || $c2 === false || $ac === false || $tx === false) { die('Falha ao baixar um ou mais CSVs.'); } // Valida
    $r1 = import_checkout($mysqli, $c1, 'checkout_1');              // Importa 1
    $r2 = import_checkout($mysqli, $c2, 'checkout_2');              // Importa 2
    $r3 = import_auth_codes($mysqli, $ac);                          // Importa auth_codes
    $r4 = import_transactions($mysqli, $tx, 'transactions.csv');    // Importa transações
    $bf = backfill_auth_codes_from_transactions($mysqli);           // Backfill auth_codes se vazio
    rebuild_minute_agg($mysqli);                                    // Recria agregados
    echo '<!doctype html><meta charset="utf-8"><body style="font-family:system-ui,Segoe UI,Arial"><h3>Importação concluída:</h3><ul>'; // Saída
    echo '<li>checkout_1: ' . htmlspecialchars($r1) . '</li>';      // Log 1
    echo '<li>checkout_2: ' . htmlspecialchars($r2) . '</li>';      // Log 2
    echo '<li>auth_codes: ' . htmlspecialchars($r3) . '</li>';      // Log 3
    echo '<li>transactions: ' . htmlspecialchars($r4) . '</li>';    // Log 4
    echo '<li>backfill: ' . htmlspecialchars($bf) . '</li>';        // Log BF
    echo '</ul><p><a href="cloudwalk.php">Voltar ao Dashboard</a> | <a href="?action=run_alerts">Executar varredura de alertas</a></p></body>'; // Links
    exit;                                                           // Sai
}

// -------------------- Dashboard (HTML + JS) -------------------- //
if ($action === 'dashboard' || $action === '') {                    // Render UI
    ?>
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8">
  <title>CloudWalk Monitoring</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom@2.0.1/dist/chartjs-plugin-zoom.umd.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3.0.1/dist/chartjs-plugin-annotation.min.js"></script>
</head>
<body class="bg-body-tertiary">
  <div class="container py-4">
    <header class="d-flex align-items-center justify-content-between mb-4">
      <h1 class="h4 mb-0">CloudWalk Monitoring</h1>
      <div class="btn-group">
        <a class="btn btn-sm btn-primary" href="?action=load">Carregar CSVs</a>
        <button class="btn btn-sm btn-warning" id="btnThresholds">Thresholds</button>
        <a class="btn btn-sm btn-outline-secondary" href="?action=run_alerts">Varredura de Alertas</a>
        <a class="btn btn-sm btn-outline-danger" href="?action=reset">Resetar</a>
      </div>
    </header>

    <div class="row g-4">
      <div class="col-12">
        <div class="card shadow-sm">
          <div class="card-header d-flex align-items-center justify-content-between">
            <span>Transações por minuto — Últimos N / Período</span>
            <div class="d-flex gap-2">
              <select id="chartStyle" class="form-select form-select-sm">
                <option value="line">Linha</option>
                <option value="area" selected>Área</option>
                <option value="bar">Barras</option>
                <option value="stacked-area">Área Empilhada</option>
                <option value="stacked-bar">Barras Empilhadas</option>
              </select>
              <div class="form-check form-check-inline">
                <input class="form-check-input" type="checkbox" id="optSmooth" checked>
                <label class="form-check-label" for="optSmooth">Suavizar</label>
              </div>
              <div class="form-check form-check-inline">
                <input class="form-check-input" type="checkbox" id="optPoints">
                <label class="form-check-label" for="optPoints">Mostrar pontos</label>
              </div>
              <div class="form-check form-check-inline">
                <input class="form-check-input" type="checkbox" id="optMA">
                <label class="form-check-label" for="optMA">MA60</label>
              </div>
            </div>
          </div>
          <div class="card-body">
            <form id="txFilters" class="row gy-3 gx-3 align-items-end">
              <div class="col-md-2">
                <label class="form-label">Statuses</label>
                <select multiple class="form-select" id="statuses">
                  <option value="approved">approved</option>
                  <option value="failed" selected>failed</option>
                  <option value="denied" selected>denied</option>
                  <option value="reversed" selected>reversed</option>
                </select>
              </div>
              <div class="col-md-2">
                <label class="form-label">Últimos N min</label>
                <input type="number" class="form-control" id="last" value="2000" min="10" max="2000">
              </div>
              <div class="col-md-3">
                <label class="form-label">De</label>
                <input type="datetime-local" class="form-control" id="from">
              </div>
              <div class="col-md-3">
                <label class="form-label">Até</label>
                <input type="datetime-local" class="form-control" id="to">
              </div>
              <div class="col-md-2 d-flex gap-2">
                <button type="button" class="btn btn-primary w-100" id="applyTx">Aplicar</button>
                <button type="button" class="btn btn-outline-secondary" id="resetZoom">Zoom reset</button>
              </div>
            </form>
            <canvas id="txChart" class="mt-3"></canvas>

            <div class="mt-4 border-top pt-3">
              <div class="d-flex align-items-center justify-content-between mb-2">
                <div class="d-flex gap-2 align-items-center">
                  <span class="fw-semibold">Alertas no período</span>
                  <div class="form-check form-check-inline">
                    <input class="form-check-input" type="checkbox" id="fltFailed" checked>
                    <label class="form-check-label" for="fltFailed">failed</label>
                  </div>
                  <div class="form-check form-check-inline">
                    <input class="form-check-input" type="checkbox" id="fltDenied" checked>
                    <label class="form-check-label" for="fltDenied">denied</label>
                  </div>
                  <div class="form-check form-check-inline">
                    <input class="form-check-input" type="checkbox" id="fltReversed" checked>
                    <label class="form-check-label" for="fltReversed">reversed</label>
                  </div>
                </div>
                <div class="d-flex align-items-center gap-2">
                  <label class="form-label m-0">Tamanho da página</label>
                  <select id="pageSize" class="form-select form-select-sm" style="width:auto">
                    <option>5</option><option selected>10</option><option>20</option><option>50</option>
                  </select>
                </div>
              </div>
              <div class="table-responsive">
                <table class="table table-sm align-middle" id="alertsTable">
                  <thead>
                    <tr><th style="width:180px">Minuto</th><th style="width:120px">Tipo</th><th>Mensagem</th></tr>
                  </thead>
                  <tbody></tbody>
                </table>
              </div>
              <div class="d-flex justify-content-between align-items-center">
                <div id="alertsInfo" class="text-muted small"></div>
                <div class="btn-group">
                  <button class="btn btn-sm btn-outline-secondary" id="prevPage">Anterior</button>
                  <button class="btn btn-sm btn-outline-secondary" id="nextPage">Próxima</button>
                </div>
              </div>
            </div>

          </div>
        </div>
      </div>

      <div class="col-12">
        <div class="card shadow-sm">
          <div class="card-header d-flex align-items-center justify-content-between">
            <span>Checkout por hora — Comparativo</span>
            <div class="d-flex gap-2">
              <select id="ckChartStyle" class="form-select form-select-sm">
                <option value="line">Linha</option>
                <option value="area" selected>Área</option>
                <option value="bar">Barras</option>
                <option value="stacked-area">Área Empilhada</option>
                <option value="stacked-bar">Barras Empilhadas</option>
              </select>
              <div class="form-check form-check-inline">
                <input class="form-check-input" type="checkbox" id="ckSmooth" checked>
                <label class="form-check-label" for="ckSmooth">Suavizar</label>
              </div>
              <div class="form-check form-check-inline">
                <input class="form-check-input" type="checkbox" id="ckPoints">
                <label class="form-check-label" for="ckPoints">Mostrar pontos</label>
              </div>
            </div>
          </div>
          <div class="card-body">
            <form id="ckFilters" class="row gy-3 gx-3 align-items-end">
              <div class="col-md-3">
                <label class="form-label">Fonte</label>
                <select class="form-select" id="ckSrc">
                  <option value="">Ambas</option>
                  <option value="checkout_1" selected>checkout_1</option>
                  <option value="checkout_2">checkout_2</option>
                </select>
              </div>
              <div class="col-md-2">
                <button type="button" class="btn btn-primary w-100" id="applyCk">Aplicar</button>
              </div>
            </form>
            <canvas id="ckChart" class="mt-3"></canvas>
          </div>
        </div>
      </div>
    </div>

    <!-- Modal Thresholds -->
    <div class="modal fade" id="thresholdsModal" tabindex="-1">
      <div class="modal-dialog modal-dialog-centered">
        <div class="modal-content">
          <div class="modal-header">
            <h5 class="modal-title">Thresholds de Alerta</h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Fechar"></button>
          </div>
          <div class="modal-body">
            <form id="formThresholds" class="row g-3">
              <div class="col-6">
                <label class="form-label">Janela baseline (min)</label>
                <input type="number" class="form-control" id="th_win" min="30" max="720">
              </div>
              <div class="col-6">
                <label class="form-label">Sigma (Z-score)</label>
                <input type="number" step="0.1" class="form-control" id="th_sigma" min="1" max="10">
              </div>
              <div class="col-6">
                <label class="form-label">Multiplicador taxa</label>
                <input type="number" step="0.1" class="form-control" id="th_rate" min="1" max="10">
              </div>
              <div class="col-6">
                <label class="form-label">Qtd mínima no status</label>

                <input type="number" class="form-control" id="th_min_abs" min="0" max="10000">
              </div>
              <div class="col-6">
                <label class="form-label">Mínimo da média base</label>
                <input type="number" step="0.1" class="form-control" id="th_base_min" min="0" max="1000">
              </div>
              <div class="col-6">
                <label class="form-label">Qtd mínima total/minuto</label>
                <input type="number" class="form-control" id="th_min_total" min="0" max="10000">
              </div>
              <div class="col-6">
                <label class="form-label">Minutos consecutivos</label>
                <input type="number" class="form-control" id="th_consecutive" min="1" max="5">
              </div>
              <div class="col-6">
                <label class="form-label">Cooldown (min)</label>
                <input type="number" class="form-control" id="th_cooldown" min="0" max="240">
              </div>
            </form>
            <div class="form-text">As mudanças valem para ingestão em tempo real. Para reprocessar alertas passados, clique em “Varredura de Alertas”.</div>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn btn-outline-secondary" data-bs-dismiss="modal">Fechar</button>
            <button type="button" class="btn btn-primary" id="btnSaveThresholds">Salvar</button>
          </div>
        </div>
      </div>
    </div>

    <footer class="text-center text-muted small mt-4">
      Criado por Luís Guilherme Brunck. PHP 8.3, Chart.js, MariaDB.
    </footer>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
  <script>
  const txCtx = document.getElementById('txChart');
  const ckCtx = document.getElementById('ckChart');
  let txChart = null;
  let ckChart = null;

  let alertsPage = 1;
  let alertsSize = 10;

  // Cache de dados do Checkout para reconstruir sem refetch
  let ckDataCache = null;

  function applyCommonStyle(d, smooth, points){
    d.tension = smooth ? 0.25 : 0;
    d.pointRadius = points ? 2 : 0;
    d.pointHoverRadius = points ? 4 : 0;
    d.borderWidth = 2;
  }

  function applyFillAndType(datasets, style){
    const isArea = style === 'area' || style === 'stacked-area';
    const isBar = style === 'bar' || style === 'stacked-bar';
    datasets.forEach(d => { d.type = isBar ? 'bar' : 'line'; d.fill = isArea; });
    return { stacked: style.startsWith('stacked-'), isBar };
  }

  function getSelectedStatuses(){
    return Array.from(document.getElementById('statuses').selectedOptions).map(o=>o.value);
  }
  function getRangeParams(){
    const last = document.getElementById('last').value;
    const from = document.getElementById('from').value;
    const to = document.getElementById('to').value;
    const params = new URLSearchParams();
    if (from) params.set('from', from.replace('T',' '));
    if (to) params.set('to', to.replace('T',' '));
    if (!from && !to) params.set('last', last);
    return params;
  }

  async function fetchSeries(){
    const statusesSel = getSelectedStatuses().join(',');
    const params = getRangeParams();
    if (statusesSel) params.set('statuses', statusesSel);
    const r = await fetch('?action=series&'+params.toString());
    return await r.json();
  }

  async function fetchAlerts(page=1){
    const params = getRangeParams();
    const types = [
      document.getElementById('fltFailed').checked ? 'failed' : null,
      document.getElementById('fltDenied').checked ? 'denied' : null,
      document.getElementById('fltReversed').checked ? 'reversed' : null
    ].filter(Boolean).join(',');
    if (types) params.set('types', types);
    params.set('page', page);
    params.set('size', alertsSize);
    const r = await fetch('?action=alerts&'+params.toString());
    return await r.json();
  }

  async function fetchAlertMarkers(){
    const params = getRangeParams();
    const r = await fetch('?action=alerts_markers&'+params.toString());
    return await r.json();
  }

  function applyAnnotations(minutes){
    const labels = txChart.data.labels || [];
    const anns = {};
    (minutes||[]).forEach((m,i)=>{
      if (labels.includes(m)){
        anns['alert'+i] = {
          type: 'line',
          xValue: m,
          borderColor: 'red',
          borderWidth: 1,
          borderDash: [4,3],
          label: { enabled: false }
        };
      }
    });
    txChart.options.plugins.annotation = { annotations: anns };
    txChart.update();
  }

  function buildTxChart(data){
    const st = data.statuses;
    const labels = data.rows.map(r => r.minute);
    const ds = [];
    if (st.includes('approved')) ds.push({label:'approved', data:data.rows.map(r=>+r.approved)});
    if (st.includes('failed'))   ds.push({label:'failed',   data:data.rows.map(r=>+r.failed)});
    if (st.includes('denied'))   ds.push({label:'denied',   data:data.rows.map(r=>+r.denied)});
    if (st.includes('reversed')) ds.push({label:'reversed', data:data.rows.map(r=>+r.reversed)});

    const style = document.getElementById('chartStyle').value;
    const smooth = document.getElementById('optSmooth').checked;
    const points = document.getElementById('optPoints').checked;
    ds.forEach(d => applyCommonStyle(d, smooth, points));
    const cfg = applyFillAndType(ds, style);

    if (document.getElementById('optMA').checked && st.length === 1) {
      const k = st[0];
      ds.push({label:'MA60 '+k, data:data.ma[k].map(v=>+v), borderDash:[5,5], pointRadius:0, pointHoverRadius:0, borderWidth:2, fill:false, type:'line'});
    }

    if (txChart) txChart.destroy();
    txChart = new Chart(txCtx, {
      type: cfg.isBar ? 'bar' : 'line',
      data:{ labels, datasets: ds },
      options:{
        responsive:true, animation:false, interaction:{mode:'index',intersect:false},
        plugins:{
          legend:{position:'bottom'},
          zoom:{ zoom:{ wheel:{enabled:true}, pinch:{enabled:true}, mode:'x' }, pan:{ enabled:true, mode:'x' } },
          annotation: { annotations: {} }
        },
        scales:{ x:{ title:{display:true, text:'Minuto'}, stacked: cfg.stacked }, y:{ title:{display:true, text:'Qtd'}, stacked: cfg.stacked } }
      }
    });
  }

  function renderAlertsTable(j){
    const tbody = document.querySelector('#alertsTable tbody');
    tbody.innerHTML = '';
    j.rows.forEach(r=>{
      const tr = document.createElement('tr');
      const td1 = document.createElement('td'); td1.textContent = r.minute;
      const td2 = document.createElement('td'); td2.textContent = r.type;
      const td3 = document.createElement('td'); td3.innerHTML = r.message;
      tr.appendChild(td1); tr.appendChild(td2); tr.appendChild(td3);
      tbody.appendChild(tr);
    });
    const info = document.getElementById('alertsInfo');
    const start = (j.total === 0) ? 0 : ((j.page-1)*j.size + 1);
    const end = Math.min(j.page*j.size, j.total);
    info.textContent = `${start}-${end} de ${j.total}`;
    alertsPage = j.page;
    document.getElementById('prevPage').disabled = (alertsPage <= 1);
    document.getElementById('nextPage').disabled = (end >= j.total);
  }

  async function refreshAll(){
    const data = await fetchSeries();
    buildTxChart(data);
    const markers = await fetchAlertMarkers();
    applyAnnotations(markers.minutes || []);
    const j = await fetchAlerts(alertsPage);
    renderAlertsTable(j);
  }

  document.getElementById('applyTx').addEventListener('click', ()=>{ alertsPage = 1; refreshAll(); });
  document.getElementById('resetZoom').addEventListener('click', ()=>{ if(txChart){ txChart.resetZoom(); } });
  document.getElementById('pageSize').addEventListener('change', (e)=>{ alertsSize = parseInt(e.target.value,10); alertsPage = 1; refreshAll(); });
  document.getElementById('prevPage').addEventListener('click', ()=>{ if(alertsPage>1){ alertsPage--; refreshAll(); }});
  document.getElementById('nextPage').addEventListener('click', ()=>{ alertsPage++; refreshAll(); });
  document.getElementById('fltFailed').addEventListener('change', ()=>{ alertsPage=1; refreshAll(); });
  document.getElementById('fltDenied').addEventListener('change', ()=>{ alertsPage=1; refreshAll(); });
  document.getElementById('fltReversed').addEventListener('change', ()=>{ alertsPage=1; refreshAll(); });

  // --------- Checkout ---------
  async function fetchCheckout(){
    const src = document.getElementById('ckSrc').value;
    const params = new URLSearchParams(); if (src) params.set('src', src);
    const r = await fetch('?action=series_checkout&'+params.toString());
    return await r.json();
  }

  function buildCkChart(data){
    const rows = data.rows || [];
    const bySrc = {};
    rows.forEach(r => { if(!bySrc[r.src]) bySrc[r.src] = []; bySrc[r.src].push(r); });
    Object.keys(bySrc).forEach(s => bySrc[s].sort((a,b)=>a.hour-b.hour));

    const labels = Array.from({length:24}, (_,h)=> h+'h');
    const datasets = [];

    Object.keys(bySrc).forEach(s => {
      const arr = bySrc[s];
      const bufToday = new Array(24).fill(0);
      const bufYesterday = new Array(24).fill(0);
      const bufAvg = new Array(24).fill(0);
      arr.forEach(r => { const h = +r.hour; if (h>=0 && h<24){ bufToday[h]=+r.today; bufYesterday[h]=+r.yesterday; bufAvg[h]=+r.avg_other; } });
      datasets.push({label:s+' hoje', data:bufToday});
      datasets.push({label:s+' ontem', data:bufYesterday});
      datasets.push({label:s+' média', data:bufAvg, borderDash:[5,5]});
    });

    const style = document.getElementById('ckChartStyle').value;
    const smooth = document.getElementById('ckSmooth').checked;
    const points = document.getElementById('ckPoints').checked;
    datasets.forEach(d => applyCommonStyle(d, smooth, points));
    const cfg = applyFillAndType(datasets, style);

    if (ckChart) ckChart.destroy();
    ckChart = new Chart(ckCtx, {
      type: cfg.isBar ? 'bar' : 'line',
      data:{ labels, datasets },
      options:{
        responsive:true, interaction:{mode:'index',intersect:false},
        plugins:{ legend:{position:'bottom'} },
        scales:{ x:{stacked:cfg.stacked}, y:{stacked:cfg.stacked, title:{display:true, text:'Qtd por hora'}} }
      }
    });
  }

  async function refreshCk(){
    ckDataCache = await fetchCheckout();
    buildCkChart(ckDataCache);
  }

  document.getElementById('applyCk').addEventListener('click', refreshCk);
  document.getElementById('ckSrc').addEventListener('change', refreshCk);
  document.getElementById('ckChartStyle').addEventListener('change', ()=>{ if (ckDataCache) buildCkChart(ckDataCache); });
  document.getElementById('ckSmooth').addEventListener('change', ()=>{ if (ckDataCache) buildCkChart(ckDataCache); });
  document.getElementById('ckPoints').addEventListener('change', ()=>{ if (ckDataCache) buildCkChart(ckDataCache); });

  // --------- Thresholds UI ---------
  document.getElementById('btnThresholds').addEventListener('click', async ()=>{
    const r = await fetch('?action=settings_get');
    const j = await r.json();
    if (j && j.ok){
      document.getElementById('th_win').value = j.cfg.win;
      document.getElementById('th_sigma').value = j.cfg.sigma;
      document.getElementById('th_rate').value = j.cfg.rate;
      document.getElementById('th_min_abs').value = j.cfg.min_abs;
      document.getElementById('th_base_min').value = j.cfg.base_min;
      document.getElementById('th_min_total').value = j.cfg.min_total;
      document.getElementById('th_consecutive').value = j.cfg.consecutive;
      document.getElementById('th_cooldown').value = j.cfg.cooldown;
    }
    const modalEl = document.getElementById('thresholdsModal');
    const modal = new bootstrap.Modal(modalEl);
    modal.show();
    window._thModal = modal;
  });

  document.getElementById('btnSaveThresholds').addEventListener('click', async ()=>{
    const payload = {
      win: +document.getElementById('th_win').value,
      sigma: +document.getElementById('th_sigma').value,
      rate: +document.getElementById('th_rate').value,
      min_abs: +document.getElementById('th_min_abs').value,
      base_min: +document.getElementById('th_base_min').value,
      min_total: +document.getElementById('th_min_total').value,
      consecutive: +document.getElementById('th_consecutive').value,
      cooldown: +document.getElementById('th_cooldown').value,
    };
    const r = await fetch('?action=settings_save', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
    const j = await r.json();
    if (j.ok){
      window._thModal.hide();
      await fetch('?action=run_alerts');
      await refreshAll();
    }
  });

  setTimeout(()=>{ refreshAll(); }, 0);
  setTimeout(refreshCk, 0);
  </script>
</body>
</html>
<?php
    exit;                                                           // Sai
}

if ($action === 'reset') {                                          // Reset schema
    ensure_schema($mysqli);                                         // Recria
    echo '<!doctype html><meta charset="utf-8"><body style="font-family:system-ui,Segoe UI,Arial"><h3>OK: schema recriado.</h3><p><a href="cloudwalk.php">Voltar ao Dashboard</a></p></body>'; // Mensagem
    exit;                                                           // Sai
}

http_response_code(404);                                            // 404
echo 'Ação desconhecida.';                                          // Mensagem
?>
