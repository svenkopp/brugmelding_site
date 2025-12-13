<?php
// Get_XML.php
// Snelle, robuuste versie met automatische JSON-check voor foute bruggen.
// Zorg dat map 'get' schrijfbaar is door de webserver voor log & output.

require_once __DIR__ . '/db_helpers.php';

date_default_timezone_set("UTC");

// ---------- Config ----------
$jsonInputFile  = __DIR__ . '/get/bruggen.json';
$jsonOutputFile = __DIR__ . '/get/bruggen_open.json';
$logBadFile     = __DIR__ . '/get/foute_bruggen.log';
$ndwUrl         = "http://opendata.ndw.nu/brugopeningen.xml.gz";

$dbConfig = load_db_config();

// ---------- Helpers ----------
function safe_get_string($var) {
    return isset($var) ? (string)$var : '';
}

// ---------- Inlezen bruggen.json ----------
if (!file_exists($jsonInputFile)) {
    die("Input bestand niet gevonden: $jsonInputFile\n");
}

$json_data = file_get_contents($jsonInputFile);
$bruggen_raw = json_decode($json_data, true);

if (!is_array($bruggen_raw)) {
    // Log en stop
    file_put_contents($logBadFile, date('c') . " - Ongeldige JSON in bruggen.json\n", FILE_APPEND);
    die("bruggen.json kon niet worden geparsed als JSON.\n");
}

// ---------- Valideer en normaliseer JSON (en log foute items) ----------
$bruggen = [];
foreach ($bruggen_raw as $index => $item) {

    // Normalize keys: sommige bestanden hebben Lat/Lon/Naam/ISRS
    // Maak een tolk zodat de rest van de code met id/latitude/longitude/naam kan werken.
    if (isset($item['ISRS'])) {
        $item['id'] = $item['ISRS'];
    }
    if (isset($item['Lat'])) {
        $item['latitude'] = $item['Lat'];
    }
    if (isset($item['Lon'])) {
        $item['longitude'] = $item['Lon'];
    }
    if (isset($item['Naam'])) {
        $item['naam'] = $item['Naam'];
    }

    // Vereiste velden (id, latitude, longitude, naam)
    $ok = true;
    $missing = [];

    if (!isset($item['id']) || $item['id'] === '') {
        $ok = false;
        $missing[] = 'id';
    }
    if (!isset($item['latitude']) || $item['latitude'] === '' || !is_numeric($item['latitude'])) {
        $ok = false;
        $missing[] = 'latitude';
    }
    if (!isset($item['longitude']) || $item['longitude'] === '' || !is_numeric($item['longitude'])) {
        $ok = false;
        $missing[] = 'longitude';
    }
    if (!isset($item['naam']) || $item['naam'] === '') {
        $ok = false;
        $missing[] = 'naam';
    }

    if (!$ok) {
        $logLine = date('c') . " - Ongeldige brug op index $index. Missende/invalid keys: " . implode(',', $missing) . "\n";
        $logLine .= print_r($item, true) . "\n---\n";
        file_put_contents($logBadFile, $logLine, FILE_APPEND);
        continue; // skip deze brug
    }

    // Alles OK: voeg toe aan genormaliseerde lijst
    $bruggen[] = $item;
}

if (count($bruggen) === 0) {
    die("Geen geldige bruggen gevonden na validatie. Kijk in $logBadFile voor details.\n");
}

// ---------- Database init ----------
$historyTable = sanitize_table_name($dbConfig['table']);
$pdo = null;

try {
    $pdo = init_db($dbConfig, $historyTable);
} catch (Throwable $e) {
    // Geen database beschikbaar: log dit en ga verder zodat de hoofdscript niet crasht
    file_put_contents(
        $logBadFile,
        date('c') . " - Database niet beschikbaar: " . $e->getMessage() . "\n",
        FILE_APPEND
    );
}

// ---------- NDW XML ophalen en parsen ----------
$xml_gz_content = @file_get_contents($ndwUrl);
if ($xml_gz_content === false) {
    file_put_contents($logBadFile, date('c') . " - Fout bij ophalen NDW URL: $ndwUrl\n", FILE_APPEND);
    die("Kon NDW XML niet ophalen.\n");
}

$xml_content = @gzdecode($xml_gz_content);
if ($xml_content === false) {
    file_put_contents($logBadFile, date('c') . " - Fout bij gzdecode van NDW content\n", FILE_APPEND);
    die("Fout bij gzdecode.\n");
}

$rcXML = @simplexml_load_string($xml_content);
if ($rcXML === false) {
    file_put_contents($logBadFile, date('c') . " - Fout bij simplexml_load_string op NDW content\n", FILE_APPEND);
    die("Fout bij parsen NDW XML.\n");
}

// Navigeer naar situations (defensie tegen ontbrekende nodes)
$envelope = $rcXML->children('http://schemas.xmlsoap.org/soap/envelope/');
$body     = $envelope->Body ?? null;
$datex    = $body ? $body->children('http://datex2.eu/schema/2/2_0') : null;

if (!$datex || !isset($datex->d2LogicalModel->payloadPublication->situation)) {
    file_put_contents($logBadFile, date('c') . " - NDW XML heeft geen situation nodes\n", FILE_APPEND);
    // We gaan door met lege situaties
    $arraySituation = [];
} else {
    $arraySituation = $datex->d2LogicalModel->payloadPublication->situation;
}

// ---------- Indexeer situaties op afgeronde coördinaten (lat_ lon) ----------
// Hiermee voorkomen we n x m loops. We bewaren enkel situaties waarvan overallStartTime +2h > now
$situationMap = [];
$now = new DateTime();

foreach ($arraySituation as $situation) {

    // Beveiliging: check of nodes bestaan
    $loc = $situation->situationRecord->groupOfLocations->locationForDisplay ?? null;
    $validityNode = $situation->situationRecord->validity->validityTimeSpecification ?? null;

    if (!$loc || !$validityNode) continue;

    $latRaw = safe_get_string($loc->latitude);
    $lonRaw = safe_get_string($loc->longitude);
    $startRaw = safe_get_string($validityNode->overallStartTime);

    if ($latRaw === '' || $lonRaw === '' || $startRaw === '') continue;

    if (!is_numeric((string)$latRaw) || !is_numeric((string)$lonRaw)) continue;

    $lat = round((float)$latRaw, 5);
    $lon = round((float)$lonRaw, 5);
    $key = $lat . '_' . $lon;

    // Parse start time veilig
    try {
        $startDt = new DateTime($startRaw);
    } catch (Exception $e) {
        continue;
    }

    $toekomst = (clone $startDt)->modify("+2 hours");
    if ($toekomst > $now) {
        // bewaar situatie
        if (!isset($situationMap[$key])) $situationMap[$key] = [];
        $situationMap[$key][] = $situation;
    }
}

// ---------- Verwerk elke brug: 1 lookup op key, kies dichtsbijzijnde starttime ----------
$dataArray = [];

foreach ($bruggen as $brug) {

    $lat = round((float)$brug['latitude'], 5);
    $lon = round((float)$brug['longitude'], 5);
    $key = $lat . '_' . $lon;

    $found = null;

    if (isset($situationMap[$key])) {
        // kies situatie waarvan overallStartTime het dichtst bij nu ligt
        foreach ($situationMap[$key] as $situation) {

            $startRaw = safe_get_string($situation->situationRecord->validity->validityTimeSpecification->overallStartTime);
            if ($startRaw === '') continue;

            try {
                $dt = new DateTime($startRaw);
            } catch (Exception $e) {
                continue;
            }

            if ($found === null) {
                $found = $situation;
                $foundTs = $dt->getTimestamp();
            } else {
                $foundStartRaw = safe_get_string($found->situationRecord->validity->validityTimeSpecification->overallStartTime);
                try {
                    $foundDt = new DateTime($foundStartRaw);
                } catch (Exception $e) {
                    // behoud huidige found als fallback
                    continue;
                }

                $diff1 = abs($now->getTimestamp() - $dt->getTimestamp());
                $diff2 = abs($now->getTimestamp() - $foundDt->getTimestamp());

                if ($diff1 < $diff2) {
                    $found = $situation;
                }
            }
        }
    }

    // Vul het output-object (zelfde velden als jouw oude script, maar robuust)
    if ($found) {
        $SituationCurrent   = (string)($found->situationRecord->operatorActionStatus ?? '');
        $SituationVoorspeld = (string)($found->situationRecord->probabilityOfOccurrence ?? '');
        // attributes() kan ontbrekend zijn of korter zijn; bescherm tegen notices
        $attributes = $found->situationRecord->attributes();
        $ndwVersion = isset($attributes[1]) ? (string)$attributes[1] : "0";
        $GetDatumStart = (string)($found->situationRecord->validity->validityTimeSpecification->overallStartTime ?? '');

        if ($SituationVoorspeld === "certain") {
            $status = "open";
        } elseif ($SituationVoorspeld === "probable") {
            $status = "voorspeld";
        } else {
            $status = "dicht";
        }
    } else {
        $SituationCurrent   = "certain";
        $SituationVoorspeld = "beingTerminated";
        $ndwVersion         = "0";
        $status             = "dicht";
        $GetDatumStart      = (new DateTime())->format('Y-m-d\TH:i:s.v\Z');
    }

    $statusMoment = $GetDatumStart ?: (new DateTime())->format('Y-m-d\TH:i:s.v\Z');
    log_status($pdo, $brug['id'], $status, $statusMoment, $historyTable);

    // Bouw output voor deze brug
    $dataArray[] = [
        'Id' => $brug['id'],
        'Data' => [
            'latitude' => (float)$brug['latitude'],
            'longitude' => (float)$brug['longitude'],
            'SituationCurrent' => $SituationCurrent,
            'SituationVoorspeld' => $SituationVoorspeld,
            'ndwVersion' => $ndwVersion,
            'GetDatumStart' => $GetDatumStart,
            'Naam' => $brug['naam'],
            'Provincie' => $brug['provincie'],
            'Stad' => $brug['stad'],
            'status' => $status,
            'open' => ($status === "open") ? true : false
        ]
    ];
}

// ---------- Sla output op ----------
file_put_contents($jsonOutputFile, json_encode($dataArray, JSON_PRETTY_PRINT));

// ---------- Einde ----------
echo "Verwerking klaar. Output: $jsonOutputFile\n";
echo "Foute items (indien aanwezig) in: $logBadFile\n";
?>